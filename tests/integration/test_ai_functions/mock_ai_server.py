"""
Mock OpenAI-compatible HTTP server for AI function integration tests.

Endpoints:
  GET  /health                       — readiness probe, returns "OK"
  GET  /last-request                 — returns JSON `{"path": ..., "body": ..., "headers": ...}`
      of the most recent POST received, so tests can assert on request contents (e.g. that
      `aiTranslate`'s `instructions` argument is forwarded in the prompt, or that the
      `Authorization` header is omitted when the named collection has no `api_key`).
      Header names are lower-cased for case-insensitive lookup.
  GET  /set-flaky?count=N            — arm the flaky endpoints below to fail their next N requests
      with a simulated transient network error (used to exercise retries). `count=0` disarms.
  POST /v1/chat/flaky                — like `/v1/chat/completions`, but drops the connection without
      a response for the first N requests after `/set-flaky?count=N`, then succeeds.
  POST /v1/embeddings_flaky          — like `/v1/embeddings`, but flaky in the same way as above.
  POST /v1/chat/completions          — returns response based on request content:
      - If response_format with json_schema is present, returns JSON matching the schema
        with values derived from the user message.
      - If the system prompt looks like an `aiFilter` boolean filter, returns plain
        `true` or `false` based on the user message.
      - Otherwise echoes the user message as plain text.
      Fixed tokens: 10 input, 5 output.
  POST /v1/embeddings                — returns one deterministic embedding per input.
      Honors `dimensions` if provided, otherwise returns DEFAULT_EMBED_DIM floats.
      `prompt_tokens` = sum of input character lengths.
  POST /v1/embeddings_dup_index      — like `/v1/embeddings` but reuses `index` 0 for every
      element, exercising the duplicate-index rejection path.
  POST /v1/embeddings_wrong_count    — returns one fewer entry than requested, exercising the
      cardinality mismatch path.
  POST /v1/chat/truncated            — returns HTTP 200 with a valid body but `finish_reason="length"`
      (model hit max_tokens). Exercises the truncated-response rejection path.
  POST /v1/chat/content_filter       — HTTP 200 with `finish_reason="content_filter"` (content
      withheld). Exercises the incomplete-response rejection path.
  POST /v1/chat/unknown_reason       — HTTP 200 with an unrecognized `finish_reason`; must be
      accepted as complete, not misclassified as truncation.
  POST /v1/chat/tool_calls           — HTTP 200 with `finish_reason="tool_calls"`: the model wants
      the caller to run a tool, so this is not a final answer and must be rejected.
  POST /v1/chat/refusal              — HTTP 200 structured-output safety refusal: `message.refusal`
      is populated, `content` is null and `finish_reason` stays "stop". Exercises the refusal
      rejection path, which a `finish_reason`-only check would accept as a complete empty answer.
  POST /v1/anthropic/stop_sequence   — Anthropic-shaped HTTP 200 with `stop_reason="stop_sequence"`,
      a complete answer that must NOT be rejected as truncated.
  POST /v1/anthropic/max_tokens      — Anthropic-shaped HTTP 200 with `stop_reason="max_tokens"`,
      a truncated answer that must be rejected.
  POST /v1/anthropic/pause_turn      — Anthropic-shaped HTTP 200 with `stop_reason="pause_turn"`: a
      paused multi-turn generation, also not a final answer and must be rejected.
  POST /v1/anthropic/context_window  — Anthropic-shaped HTTP 200 with
      `stop_reason="model_context_window_exceeded"`, also a truncated answer, but one whose remedy is
      the opposite of the `max_tokens` case.
  POST /v1/anthropic/tool_use        — Anthropic-shaped HTTP 200 with `stop_reason="tool_use"`, a
      successful structured-output (forced tool call) response that must NOT be rejected.
  POST /v1/error                     — always returns HTTP 500, a transient/server-side error that
      the url table function (and so the AI functions) retries.
  POST /v1/bad_request               — always returns HTTP 400, a deterministic client error that
      the url table function never retries, used to assert AI functions do not retry it either.
  POST /v1/embeddings_error          — always returns HTTP 500 (used for embedding errors)
"""

import http.server
import json
import threading
from urllib.parse import urlparse, parse_qs

MOCK_PORT = 18123
DEFAULT_EMBED_DIM = 4

# The server is threaded (see `ThreadingHTTPServer` below) so it can serve the concurrent AI calls a
# multi-threaded query issues. `_LOCK` guards the shared mutable state against those concurrent handlers.
_LOCK = threading.Lock()
LAST_REQUEST = {"path": None, "body": None, "headers": {}}

# Number of upcoming requests to the flaky endpoints (`/v1/chat/flaky`, `/v1/embeddings_flaky`)
# that should fail with a simulated transient network error before they start succeeding.
# Set via `GET /set-flaky?count=N`. Used to exercise the network-error retry path.
FLAKY = {"fails_remaining": 0}


def extract_user_message(body):
    data = json.loads(body)
    messages = data.get("messages", [])
    for msg in reversed(messages):
        if msg.get("role") == "user":
            return msg.get("content", "")
    return ""


def extract_system_prompt(body):
    data = json.loads(body)
    messages = data.get("messages", [])
    for msg in messages:
        if msg.get("role") == "system":
            return msg.get("content", "")
    return ""


def is_filter_request(body):
    """Detect `aiFilter` requests from the fixed boolean-filter system prompt."""
    return "boolean text filter" in extract_system_prompt(body).lower()


def filter_match_response(user_message):
    """Return plain true/false for `aiFilter`. False when the user message signals an obvious negative."""
    lowered = user_message.lower()
    if any(token in lowered for token in ("false", "no match", "does not match")):
        return "false"
    return "true"


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


def make_success_response(content, prompt_tokens=10, completion_tokens=5, finish_reason="stop"):
    return {
        "choices": [
            {
                "message": {"content": content},
                "finish_reason": finish_reason,
            }
        ],
        "usage": {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
        },
    }


def make_anthropic_response(content, stop_reason="end_turn", input_tokens=10, output_tokens=5):
    """Anthropic-shaped success body. Used to test the Anthropic `stop_reason` normalization,
    notably that `stop_sequence` is a complete answer, not a truncation."""
    return {
        "content": [{"type": "text", "text": content}],
        "stop_reason": stop_reason,
        "usage": {"input_tokens": input_tokens, "output_tokens": output_tokens},
    }


def make_anthropic_tool_use_response(body, input_tokens=10, output_tokens=5):
    """Anthropic-shaped structured-output success: a forced `tool_use` block with
    `stop_reason="tool_use"`. This is a completed response (`AnthropicProvider` parses the tool
    input into the result), not a truncation — used to guard against rejecting it as incomplete.

    The tool input is derived from the request's `tools[0].input_schema`, mirroring
    `build_structured_response` so `aiClassify`/`aiExtract` post-processing produces a stable value.
    """
    data = json.loads(body)
    tools = data.get("tools", [])
    input_schema = tools[0].get("input_schema", {}) if tools else {}
    user_msg = extract_user_message(body)
    tool_input = json.loads(build_structured_response({"schema": input_schema}, user_msg))
    return {
        "content": [{"type": "tool_use", "name": "structured_output", "input": tool_input}],
        "stop_reason": "tool_use",
        "usage": {"input_tokens": input_tokens, "output_tokens": output_tokens},
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
            with _LOCK:
                snapshot = dict(LAST_REQUEST)
            self._send_json(200, snapshot)
            return

        if parsed.path == "/set-flaky":
            qs = parse_qs(parsed.query)
            with _LOCK:
                FLAKY["fails_remaining"] = int(qs.get("count", ["0"])[0])
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

        with _LOCK:
            LAST_REQUEST["path"] = parsed.path
            LAST_REQUEST["body"] = body
            LAST_REQUEST["headers"] = {k.lower(): v for k, v in self.headers.items()}

        if parsed.path in ("/v1/chat/flaky", "/v1/embeddings_flaky"):
            with _LOCK:
                should_fail = FLAKY["fails_remaining"] > 0
                if should_fail:
                    FLAKY["fails_remaining"] -= 1
            if should_fail:
                # Simulate a transient network failure: close the connection without sending any
                # response, so the client sees EOF — a Poco network exception — rather than an HTTP
                # error status. This exercises the network-error retry path, distinct from the HTTP
                # 500 path (`/v1/error`).
                self.close_connection = True
                return
            if parsed.path == "/v1/chat/flaky":
                self._send_json(200, make_success_response(extract_user_message(body)))
            else:
                self._send_json(200, make_embeddings_response(body))
            return

        if parsed.path == "/v1/chat/completions":
            user_msg = extract_user_message(body)
            json_schema = extract_response_format(body)

            if json_schema:
                content = build_structured_response(json_schema, user_msg)
            elif is_filter_request(body):
                content = filter_match_response(user_msg)
            else:
                content = user_msg

            self._send_json(200, make_success_response(content))
            return

        if parsed.path == "/v1/chat/no_choices":
            # A `200` the provider bills for, whose body then fails validation.
            self._send_json(200, {
                "id": "chatcmpl-no-choices",
                "object": "chat.completion",
                "choices": [],
                "usage": {"prompt_tokens": 7, "completion_tokens": 0, "total_tokens": 7},
            })
            return

        if parsed.path == "/v1/anthropic/no_content":
            # A `200` the provider bills for, whose body then fails validation. Anthropic reports usage
            # under different keys than OpenAI.
            self._send_json(200, {
                "id": "msg-no-content",
                "type": "message",
                "stop_reason": "end_turn",
                "usage": {"input_tokens": 9, "output_tokens": 0},
            })
            return

        if parsed.path == "/v1/chat/truncated":
            # A well-formed HTTP 200 response whose body is valid but reports that the model hit the
            # max_tokens limit (`finish_reason="length"`). The returned text is therefore truncated
            # and the AI functions must reject it rather than silently return the partial content.
            user_msg = extract_user_message(body)
            self._send_json(200, make_success_response(user_msg, finish_reason="length"))
            return

        if parsed.path == "/v1/chat/content_filter":
            # HTTP 200 with `finish_reason="content_filter"`: the provider withheld content, so the
            # answer is incomplete and must be rejected.
            user_msg = extract_user_message(body)
            self._send_json(200, make_success_response(user_msg, finish_reason="content_filter"))
            return

        if parsed.path == "/v1/chat/tool_calls":
            # HTTP 200 with `finish_reason="tool_calls"`: the model is asking the caller to run a
            # tool, so there is no final answer here. Must be rejected rather than returned empty.
            user_msg = extract_user_message(body)
            self._send_json(200, make_success_response(user_msg, finish_reason="tool_calls"))
            return

        if parsed.path == "/v1/chat/refusal":
            # Structured-output safety refusal: OpenAI returns the explanation in `message.refusal`
            # with a null `content`, and leaves `finish_reason` as "stop" because the generation
            # itself ended normally. Must be rejected rather than returned as an empty answer.
            response = make_success_response(None, finish_reason="stop")
            response["choices"][0]["message"]["refusal"] = "I cannot help with that request."
            self._send_json(200, response)
            return

        if parsed.path == "/v1/chat/unknown_reason":
            # HTTP 200 with an unrecognized `finish_reason`. Must be accepted (treated as complete)
            # rather than misclassified as truncation.
            user_msg = extract_user_message(body)
            self._send_json(200, make_success_response(user_msg, finish_reason="some_future_reason"))
            return

        if parsed.path == "/v1/anthropic/stop_sequence":
            # Anthropic-shaped 200 with `stop_reason="stop_sequence"`: a complete answer produced by
            # hitting a caller stop sequence. Must NOT be rejected as truncated.
            user_msg = extract_user_message(body)
            self._send_json(200, make_anthropic_response(user_msg, stop_reason="stop_sequence"))
            return

        if parsed.path == "/v1/anthropic/max_tokens":
            # Anthropic-shaped 200 with `stop_reason="max_tokens"`: a truncated answer that must be
            # rejected.
            user_msg = extract_user_message(body)
            self._send_json(200, make_anthropic_response(user_msg, stop_reason="max_tokens"))
            return

        if parsed.path == "/v1/anthropic/pause_turn":
            # Anthropic-shaped 200 with `stop_reason="pause_turn"`: the generation is paused mid
            # multi-turn exchange, so it is not a completed answer and must be rejected.
            user_msg = extract_user_message(body)
            self._send_json(200, make_anthropic_response(user_msg, stop_reason="pause_turn"))
            return

        if parsed.path == "/v1/anthropic/context_window":
            # Anthropic-shaped 200 with `stop_reason="model_context_window_exceeded"`: also truncated,
            # but raising max_tokens would make it worse, so the hint must differ from the max_tokens
            # case.
            user_msg = extract_user_message(body)
            self._send_json(
                200,
                make_anthropic_response(user_msg, stop_reason="model_context_window_exceeded"),
            )
            return

        if parsed.path == "/v1/anthropic/tool_use":
            # Anthropic-shaped 200 with `stop_reason="tool_use"`: a successful structured-output
            # response (forced tool call). Must NOT be rejected as incomplete.
            self._send_json(200, make_anthropic_tool_use_response(body))
            return

        if parsed.path == "/v1/error":
            self._send_json(500, make_error_response("permanent failure"))
            return

        if parsed.path == "/v1/bad_request":
            # A deterministic client error (e.g. malformed request / bad API key). The url table
            # function never retries 400, so neither should the AI functions.
            self._send_json(400, make_error_response("invalid request", error_type="invalid_request_error"))
            return

        if parsed.path == "/v1/error_control_chars":
            # An error whose `message` and `type` carry control characters (newline, tab, carriage
            # return, BEL). The server must not copy these verbatim into the logged exception, so the
            # AI functions sanitize them; this endpoint lets the test assert that.
            self._send_json(
                400,
                make_error_response("start\nmid\tend\rBEL\x07done", error_type="err\ttype"),
            )
            return

        if parsed.path == "/v1/error_nonjson":
            # A non-JSON error body with control characters. It cannot be parsed as a structured
            # error, so the AI functions fall back to the truncated raw body -- which must also be
            # sanitized before it reaches the logs.
            self._send_raw(500, b"Internal Error:\nstack\ttrace\x07here")
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

    def _send_raw(self, status, body_bytes, content_type="text/plain"):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body_bytes)))
        self.end_headers()
        self.wfile.write(body_bytes)

    def log_message(self, format, *args):
        pass  # suppress request logs


class MockServer(http.server.ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True
    # Absorb a burst of simultaneous connections from a multi-threaded query. The default backlog of
    # 5 overflows when several pipeline threads each open a connection at once, dropping SYNs and
    # making the client's connect time out.
    request_queue_size = 128


if __name__ == "__main__":
    server = MockServer(("0.0.0.0", MOCK_PORT), Handler)
    try:
        server.serve_forever()
    finally:
        server.server_close()
