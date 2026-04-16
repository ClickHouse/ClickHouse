"""
Mock HTTP server for AI function integration tests.

OpenAI-compatible endpoints:
  GET  /health                  — readiness probe, returns "OK"
  POST /v1/chat/completions     — echoes user message, fixed tokens (10 in, 5 out)
  POST /v1/error                — always returns HTTP 500
  POST /v1/flaky                — fails first 2 requests with HTTP 503, then succeeds
  GET  /v1/flaky/reset          — resets the flaky counter
  POST /v1/custom_tokens?prompt_tokens=N&completion_tokens=M
                                — echoes user message with configurable token counts

Anthropic-compatible endpoints:
  POST /v1/messages             — echoes user message in Anthropic response format
"""

import http.server
import json
from urllib.parse import urlparse, parse_qs

FLAKY_COUNTER = 0
FLAKY_FAIL_COUNT = 2

MOCK_PORT = 9123


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


def make_anthropic_success_response(content, input_tokens=10, output_tokens=5):
    return {
        "content": [{"type": "text", "text": content}],
        "stop_reason": "end_turn",
        "usage": {
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
        },
    }


def make_error_response(message, error_type="server_error"):
    return {"error": {"message": message, "type": error_type}}


def extract_user_message(body):
    data = json.loads(body)
    messages = data.get("messages", [])
    for msg in reversed(messages):
        if msg.get("role") == "user":
            return msg.get("content", "")
    return ""


class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)

        if parsed.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"OK")
            return

        if parsed.path == "/v1/flaky/reset":
            global FLAKY_COUNTER
            FLAKY_COUNTER = 0
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"reset")
            return

        self.send_response(404)
        self.end_headers()

    def do_POST(self):
        parsed = urlparse(self.path)
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length).decode("utf-8") if content_length else ""

        if parsed.path == "/v1/chat/completions":
            user_msg = extract_user_message(body)
            self._send_json(200, make_success_response(user_msg))
            return

        if parsed.path == "/v1/messages":
            user_msg = extract_user_message(body)
            self._send_json(200, make_anthropic_success_response(user_msg))
            return

        if parsed.path == "/v1/error":
            self._send_json(500, make_error_response("permanent failure"))
            return

        if parsed.path == "/v1/flaky":
            global FLAKY_COUNTER
            FLAKY_COUNTER += 1
            if FLAKY_COUNTER <= FLAKY_FAIL_COUNT:
                self._send_json(503, make_error_response("service temporarily unavailable", "transient_error"))
            else:
                user_msg = extract_user_message(body)
                self._send_json(200, make_success_response(user_msg))
            return

        if parsed.path == "/v1/custom_tokens":
            params = parse_qs(parsed.query)
            prompt_tokens = int(params.get("prompt_tokens", [10])[0])
            completion_tokens = int(params.get("completion_tokens", [5])[0])
            user_msg = extract_user_message(body)
            self._send_json(200, make_success_response(user_msg, prompt_tokens, completion_tokens))
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
