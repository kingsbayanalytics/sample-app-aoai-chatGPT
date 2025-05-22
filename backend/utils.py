import os
import json
import logging
import requests
import dataclasses

from typing import List

DEBUG = os.environ.get("DEBUG", "false")
if DEBUG.lower() == "true":
    logging.basicConfig(level=logging.DEBUG)

AZURE_SEARCH_PERMITTED_GROUPS_COLUMN = os.environ.get(
    "AZURE_SEARCH_PERMITTED_GROUPS_COLUMN"
)


class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        return super().default(o)


async def format_as_ndjson(r):
    try:
        async for event in r:
            yield json.dumps(event, cls=JSONEncoder) + "\n"
    except Exception as error:
        logging.exception("Exception while generating response stream: %s", error)
        yield json.dumps({"error": str(error)})


def parse_multi_columns(columns: str) -> list:
    if "|" in columns:
        return columns.split("|")
    else:
        return columns.split(",")


def fetchUserGroups(userToken, nextLink=None):
    # Recursively fetch group membership
    if nextLink:
        endpoint = nextLink
    else:
        endpoint = "https://graph.microsoft.com/v1.0/me/transitiveMemberOf?$select=id"

    headers = {"Authorization": "bearer " + userToken}
    try:
        r = requests.get(endpoint, headers=headers)
        if r.status_code != 200:
            logging.error(f"Error fetching user groups: {r.status_code} {r.text}")
            return []

        r = r.json()
        if "@odata.nextLink" in r:
            nextLinkData = fetchUserGroups(userToken, r["@odata.nextLink"])
            r["value"].extend(nextLinkData)

        return r["value"]
    except Exception as e:
        logging.error(f"Exception in fetchUserGroups: {e}")
        return []


def generateFilterString(userToken):
    # Get list of groups user is a member of
    userGroups = fetchUserGroups(userToken)

    # Construct filter string
    if not userGroups:
        logging.debug("No user groups found")

    group_ids = ", ".join([obj["id"] for obj in userGroups])
    return f"{AZURE_SEARCH_PERMITTED_GROUPS_COLUMN}/any(g:search.in(g, '{group_ids}'))"


def format_non_streaming_response(chatCompletion, history_metadata, apim_request_id):
    response_obj = {
        "id": chatCompletion.id,
        "model": chatCompletion.model,
        "created": chatCompletion.created,
        "object": chatCompletion.object,
        "choices": [{"messages": []}],
        "history_metadata": history_metadata,
        "apim-request-id": apim_request_id,
    }

    if len(chatCompletion.choices) > 0:
        message = chatCompletion.choices[0].message
        if message:
            if hasattr(message, "context"):
                response_obj["choices"][0]["messages"].append(
                    {
                        "role": "tool",
                        "content": json.dumps(message.context),
                    }
                )
            response_obj["choices"][0]["messages"].append(
                {
                    "role": "assistant",
                    "content": message.content,
                }
            )
            return response_obj

    return {}

def format_stream_response(chatCompletionChunk, history_metadata, apim_request_id):
    response_obj = {
        "id": chatCompletionChunk.id,
        "model": chatCompletionChunk.model,
        "created": chatCompletionChunk.created,
        "object": chatCompletionChunk.object,
        "choices": [{"messages": []}],
        "history_metadata": history_metadata,
        "apim-request-id": apim_request_id,
    }

    if len(chatCompletionChunk.choices) > 0:
        delta = chatCompletionChunk.choices[0].delta
        if delta:
            if hasattr(delta, "context"):
                messageObj = {"role": "tool", "content": json.dumps(delta.context)}
                response_obj["choices"][0]["messages"].append(messageObj)
                return response_obj
            if delta.role == "assistant" and hasattr(delta, "context"):
                messageObj = {
                    "role": "assistant",
                    "context": delta.context,
                }
                response_obj["choices"][0]["messages"].append(messageObj)
                return response_obj
            if delta.tool_calls:
                messageObj = {
                    "role": "tool",
                    "tool_calls": {
                        "id": delta.tool_calls[0].id,
                        "function": {
                            "name" : delta.tool_calls[0].function.name,
                            "arguments": delta.tool_calls[0].function.arguments
                        },
                        "type": delta.tool_calls[0].type
                    }
                }
                if hasattr(delta, "context"):
                    messageObj["context"] = json.dumps(delta.context)
                response_obj["choices"][0]["messages"].append(messageObj)
                return response_obj
            else:
                if delta.content:
                    messageObj = {
                        "role": "assistant",
                        "content": delta.content,
                    }
                    response_obj["choices"][0]["messages"].append(messageObj)
                    return response_obj

    return {}


def format_pf_non_streaming_response(
    chatCompletion, history_metadata, response_field_name, citations_field_name, message_uuid=None
):
    if chatCompletion is None:
        logging.error(
            "chatCompletion object is None - Increase PROMPTFLOW_RESPONSE_TIMEOUT parameter"
        )
        return {
            "error": "No response received from promptflow endpoint increase PROMPTFLOW_RESPONSE_TIMEOUT parameter or check the promptflow endpoint."
        }
    if "error" in chatCompletion:
        logging.error(f"Error in promptflow response api: {chatCompletion['error']}")
        return {"error": chatCompletion["error"]}

    # First, log the raw structure to help with debugging
    logging.debug(f"Raw chatCompletion structure to format: {json.dumps(chatCompletion, indent=2)}")
    
    # Standardize field names for easier reference
    standard_resp_field = "output"
    standard_citations_field = "citations"
    
    # Extract answer content and citations
    answer_content = ""
    citations = []
    
    # Case 1: Nested output structure (AI Foundry pattern)
    if "output" in chatCompletion and isinstance(chatCompletion["output"], dict):
        out = chatCompletion["output"]
        logging.debug(f"Processing nested output structure with keys: {list(out.keys())}")
        
        # Try multiple possible locations for the answer content
        if standard_resp_field in out:
            answer_content = out[standard_resp_field]
            logging.debug(f"Found answer in output.{standard_resp_field}")
        elif response_field_name in out:
            answer_content = out[response_field_name]
            logging.debug(f"Found answer in output.{response_field_name}")
        
        # Try multiple possible locations for citations
        if standard_citations_field in out:
            citations = out[standard_citations_field]
            logging.debug(f"Found {len(citations)} citations in output.{standard_citations_field}")
        elif citations_field_name in out:
            citations = out[citations_field_name]
            logging.debug(f"Found {len(citations)} citations in output.{citations_field_name}")
    
    # Case 2: Flat structure with direct fields
    else:
        # Try multiple possible field names for answer content
        if response_field_name in chatCompletion:
            answer_content = chatCompletion[response_field_name]
            logging.debug(f"Found answer directly in {response_field_name}")
        elif standard_resp_field in chatCompletion:
            answer_content = chatCompletion[standard_resp_field]
            logging.debug(f"Found answer directly in {standard_resp_field}")
            
        # Try multiple possible field names for citations
        if citations_field_name in chatCompletion:
            citations = chatCompletion[citations_field_name]
            logging.debug(f"Found {len(citations)} citations directly in {citations_field_name}")
        elif standard_citations_field in chatCompletion:
            citations = chatCompletion[standard_citations_field]
            logging.debug(f"Found {len(citations)} citations directly in {standard_citations_field}")
    
    # Log what we extracted
    logging.debug(f"Extracted answer content (first 100 chars): {answer_content[:100]}...")
    logging.debug(f"Extracted {len(citations)} citations")
    
    # Create messages from extracted content
    messages = []
    if answer_content:
        messages.append({
            "role": "assistant",
            "content": answer_content
        })
    if citations:
        citation_content = {"citations": citations}
        messages.append({ 
            "role": "tool",
            "content": json.dumps(citation_content)
        })
    
    # Check if we have any messages to return
    if not messages:
        logging.warning("No content extracted from response - unable to create any messages")
        if "output" in chatCompletion and not isinstance(chatCompletion["output"], dict):
            # Fallback: try using the output field directly if it's a string
            direct_output = chatCompletion["output"]
            if isinstance(direct_output, str):
                logging.debug("Using direct string output as fallback")
                messages.append({
                    "role": "assistant",
                    "content": direct_output
                })

    # Construct and return the response object
    response_obj = {
        "id": chatCompletion.get("id", ""),
        "model": "",
        "created": "",
        "object": "",
        "history_metadata": history_metadata,
        "choices": [
            {
                "messages": messages,
            }
        ]
    }
    
    logging.debug(f"Final formatted response: {json.dumps(response_obj, indent=2)}")
    return response_obj


def convert_to_pf_format(input_json, request_field_name, response_field_name):
    output_json = []
    logging.debug(f"Input json: {input_json}")
    # align the input json to the format expected by promptflow chat flow
    for message in input_json["messages"]:
        if message:
            if message["role"] == "user":
                new_obj = {
                    "inputs": {request_field_name: message["content"]},
                    "outputs": {response_field_name: ""},
                }
                output_json.append(new_obj)
            elif message["role"] == "assistant" and len(output_json) > 0:
                output_json[-1]["outputs"][response_field_name] = message["content"]
    logging.debug(f"PF formatted response: {output_json}")
    return output_json


def comma_separated_string_to_list(s: str) -> List[str]:
    '''
    Split comma-separated values into a list.
    '''
    return s.strip().replace(' ', '').split(',')

