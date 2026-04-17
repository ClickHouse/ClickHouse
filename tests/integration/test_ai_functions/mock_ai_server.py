"""
Mock OpenAI-compatible HTTP server for AI function integration tests.

Endpoints:
  GET  /health                  — readiness probe, returns "OK"
  POST /v1/chat/completions     — returns response based on request content:
      - If response_format with json_schema is present, returns JSON matching the schema
        with values derived from the user message.
      - Otherwise echoes the user message as plain text.
      Fixed tokens: 10 input, 5 output.
  POST /v1/error                — always returns HTTP 500
"""

import http.server
import json
from urllib.parse import urlparse

MOCK_PORT = 9123


def extract_user_message(body):
    data = json.loads(body)
    messages = data.get("messages", [])
    for msg in reversed(messages):
        if msg.get("role") == "user":
            return msg.get("content", "")
    return ""


def extract_response_format(body):
    """Extract the json_schema from response_format if present."""
    data = json.loads(body)
    rf = data.get("response_format")
    if not rf or rf.get("type") != "json_schema":
        return None
    return rf.get("json_schema", {})


def build_structured_response(json_schema, user_message):
    """Build a JSON response matching the schema, using the user message as values."""
    schema = json_schema.get("schema", {})
    properties = schema.get("properties", {})

    result = {}
    for key, prop in properties.items():
        prop_type = prop.get("type", "string")
        if "enum" in prop:
            # For classification: return the first enum value
            result[key] = prop["enum"][0]
        elif isinstance(prop_type, list):
            # e.g. ["string", "null"] — return the user message
            result[key] = user_message
        else:
            result[key] = user_message

    return json.dumps(result)


def make_success_response(content, prompt_tokens=10, completion_tokens=5):
    return {
        "choices": [
            {
                "message": {"content": content},
                "finish_reason": "stop",
            }
        ],
        "usage": {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
        },
    }


def make_error_response(message, error_type="server_error"):
    return {"error": {"message": message, "type": error_type}}


class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)

        if parsed.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK")
            return

        self.send_response(404)
        self.end_headers()

    def do_POST(self):
        parsed = urlparse(self.path)
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length).decode("utf-8") if content_length else ""

        if parsed.path == "/v1/chat/completions":
            user_msg = extract_user_message(body)
            json_schema = extract_response_format(body)

            if json_schema:
                content = build_structured_response(json_schema, user_msg)
            else:
                content = user_msg

            self._send_json(200, make_success_response(content))
            return

        if parsed.path == "/v1/error":
            self._send_json(500, make_error_response("permanent failure"))
            return

        self.send_response(404)
        self.end_headers()

    def _send_json(self, status, obj):
        body = json.dumps(obj).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass  # suppress request logs


if __name__ == "__main__":
    server = http.server.HTTPServer(("0.0.0.0", MOCK_PORT), Handler)
    try:
        server.serve_forever()
    finally:
        server.server_close()
