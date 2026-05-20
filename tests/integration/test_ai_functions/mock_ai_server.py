"""
Mock OpenAI-compatible HTTP server for AI function integration tests.

Endpoints:
  GET  /health                       — readiness probe, returns "OK"
  GET  /last-request                 — returns JSON `{"path": ..., "body": ...}` of the most
      recent POST received, so tests can assert on request contents (e.g. that
      `aiTranslate`'s `instructions` argument is forwarded in the prompt).
  POST /v1/chat/completions          — returns response based on request content:
      - If response_format with json_schema is present, returns JSON matching the schema
        with values derived from the user message.
      - Otherwise echoes the user message as plain text.
      Fixed tokens: 10 input, 5 output.
  POST /v1/embeddings                — returns one deterministic embedding per input.
      Honors `dimensions` if provided, otherwise returns DEFAULT_EMBED_DIM floats.
      `prompt_tokens` = sum of input character lengths.
  POST /v1/embeddings_dup_index      — like `/v1/embeddings` but reuses `index` 0 for every
      element, exercising the duplicate-index rejection path.
  POST /v1/embeddings_wrong_count    — returns one fewer entry than requested, exercising the
      cardinality mismatch path.
  POST /v1/error                     — always returns HTTP 500 (used for chat completion errors)
  POST /v1/embeddings_error          — always returns HTTP 500 (used for embedding errors)
"""

import http.server
import json
from urllib.parse import urlparse

MOCK_PORT = 18123
DEFAULT_EMBED_DIM = 4

# Single-threaded `HTTPServer` handles one request at a time, so a plain dict is safe.
LAST_REQUEST = {"path": None, "body": None}


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
        if "enum" in prop:
            # For classification: return the first enum value
            result[key] = prop["enum"][0]
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


def make_embedding_vector(text, dim):
    """Return a deterministic float vector for `text` of length `dim`.

    Values depend on text content so different inputs produce different vectors.
    An empty `text` is supported (the function caller filters those out, but the
    server should not crash if one slips through).
    """
    if not text:
        return [0.0] * dim
    return [round(((ord(text[i % len(text)]) * (i + 1)) % 1000) / 1000.0, 3) for i in range(dim)]


def make_embeddings_response(body, *, duplicate_index=False, drop_last=False):
    data = json.loads(body)
    inputs = data.get("input", [])
    if isinstance(inputs, str):
        inputs = [inputs]
    dim = int(data.get("dimensions") or 0) or DEFAULT_EMBED_DIM

    items = []
    for i, text in enumerate(inputs):
        items.append({
            "object": "embedding",
            "index": 0 if duplicate_index else i,
            "embedding": make_embedding_vector(text, dim),
        })
    if drop_last and items:
        items.pop()

    return {
        "object": "list",
        "data": items,
        "model": data.get("model", "test-embed-model"),
        "usage": {
            "prompt_tokens": sum(len(t) for t in inputs),
            "total_tokens": sum(len(t) for t in inputs),
        },
    }


class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)

        if parsed.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK")
            return

        if parsed.path == "/last-request":
            self._send_json(200, LAST_REQUEST)
            return

        self.send_response(404)
        self.end_headers()

    def do_POST(self):
        parsed = urlparse(self.path)
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length).decode("utf-8") if content_length else ""

        LAST_REQUEST["path"] = parsed.path
        LAST_REQUEST["body"] = body

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

        if parsed.path == "/v1/embeddings":
            self._send_json(200, make_embeddings_response(body))
            return

        if parsed.path == "/v1/embeddings_dup_index":
            self._send_json(200, make_embeddings_response(body, duplicate_index=True))
            return

        if parsed.path == "/v1/embeddings_wrong_count":
            self._send_json(200, make_embeddings_response(body, drop_last=True))
            return

        if parsed.path == "/v1/embeddings_error":
            self._send_json(500, make_error_response("embedding failure"))
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
