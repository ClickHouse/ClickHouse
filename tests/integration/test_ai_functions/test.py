"""
Integration tests for AI function execution paths.

Tests the row-processing loop against a mock OpenAI-compatible HTTP server
for aiGenerate, aiClassify, aiFilter, aiExtract, and aiTranslate.
"""

import json
import os
import typing
import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import wait_condition

MOCK_PORT = 18123
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance("node")


def run_mock_server():
    instance.copy_file_to_container(
        os.path.join(SCRIPT_DIR, "mock_ai_server.py"),
        "/mock_ai_server.py",
    )
    instance.exec_in_container(
        [
            "bash",
            "-c",
            "python3 /mock_ai_server.py > /var/log/clickhouse-server/mock_ai_server.log 2>&1",
        ],
        detach=True,
        user="root",
    )
    try:
        wait_condition(
            lambda: instance.exec_in_container(
                ["curl", "-s", f"http://localhost:{MOCK_PORT}/health"],
                nothrow=True,
            ),
            lambda r: r == "OK",
            max_attempts=40,
            delay=0.5,
        )
    except Exception as e:
        log = instance.exec_in_container(
            ["cat", "/var/log/clickhouse-server/mock_ai_server.log"],
            nothrow=True,
        )
        raise RuntimeError(
            f"Mock AI server failed to become ready. Server log:\n{log}"
        ) from e


def unique_query_id(prefix):
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def get_profile_events(query_id, query_type="QueryFinish"):
    """AI counters from `system.query_log`. A query that threw logs `ExceptionWhileProcessing`
    rather than `QueryFinish`, so the throwing paths pass that type explicitly."""
    instance.query("SYSTEM FLUSH LOGS")
    result = instance.query(
        f"""
        SELECT
            ProfileEvents['AIAPICalls'] AS api_calls,
            ProfileEvents['AIInputTokens'] AS input_tokens,
            ProfileEvents['AIOutputTokens'] AS output_tokens,
            ProfileEvents['AIRowsProcessed'] AS rows_processed,
            ProfileEvents['AIRowsSkipped'] AS rows_skipped,
            peak_threads_usage AS peak_threads
        FROM system.query_log
        WHERE query_id = '{query_id}' AND type = '{query_type}'
        LIMIT 1
        FORMAT JSONEachRow
        """
    ).strip()
    assert (
        result
    ), f"no system.query_log row found for query_id={query_id} type={query_type}"
    return json.loads(result)


@pytest.fixture(scope="module")
def started_cluster() -> typing.Generator[ClickHouseCluster, None, None]:
    try:
        cluster.start()
        run_mock_server()

        instance.query(
            f"CREATE NAMED COLLECTION ai_mock AS "
            f"provider = 'openai', "
            f"endpoint = 'http://localhost:{MOCK_PORT}/v1/chat/completions', "
            f"model = 'test-model', "
            f"api_key = 'test-key'"
        )
        instance.query(
            f"CREATE NAMED COLLECTION ai_error AS "
            f"provider = 'openai', "
            f"endpoint = 'http://localhost:{MOCK_PORT}/v1/error', "
            f"model = 'test-model', "
            f"api_key = 'test-key'"
        )
        # Endpoint returning a billed `200` whose body has no usable `choices`.
        instance.query(
            f"CREATE NAMED COLLECTION ai_no_choices AS "
            f"provider = 'openai', "
            f"endpoint = 'http://localhost:{MOCK_PORT}/v1/chat/no_choices', "
            f"model = 'test-model', "
            f"api_key = 'test-key'"
        )
        # Anthropic endpoint returning a billed `200` with no `content` array.
        instance.query(
            f"CREATE NAMED COLLECTION ai_anthropic_no_content AS "
            f"provider = 'anthropic', "
            f"endpoint = 'http://localhost:{MOCK_PORT}/v1/anthropic/no_content', "
            f"model = 'test-model', "
            f"api_key = 'test-key'"
        )
        # Endpoint returning a deterministic HTTP 400, which the url table function never retries.
        instance.query(
            f"CREATE NAMED COLLECTION ai_bad_request AS "
            f"provider = 'openai', "
            f"endpoint = 'http://localhost:{MOCK_PORT}/v1/bad_request', "
            f"model = 'test-model', "
            f"api_key = 'test-key'"
        )
        # Endpoint returning an error whose message/type contain control characters, used to assert
        # they are sanitized before landing in the logged exception.
        instance.query(
            f"CREATE NAMED COLLECTION ai_error_control_chars AS "
            f"provider = 'openai', "
            f"endpoint = 'http://localhost:{MOCK_PORT}/v1/error_control_chars', "
            f"model = 'test-model', "
            f"api_key = 'test-key'"
        )
        # Endpoint returning a non-JSON error body with control characters, exercising the
        # raw-body fallback of the error formatter.
        instance.query(
            f"CREATE NAMED COLLECTION ai_error_nonjson AS "
            f"provider = 'openai', "
            f"endpoint = 'http://localhost:{MOCK_PORT}/v1/error_nonjson', "
            f"model = 'test-model', "
            f"api_key = 'test-key'"
        )
        # Endpoints returning a valid HTTP 200 body but a non-completion `finish_reason`, used to test
        # that incomplete generations are rejected (and benign non-"stop" reasons are not).
        instance.query(
            f"CREATE NAMED COLLECTION ai_truncated AS "
            f"provider = 'openai', "
            f"endpoint = 'http://localhost:{MOCK_PORT}/v1/chat/truncated', "
            f"model = 'test-model', "
            f"api_key = 'test-key'"
        )
        instance.query(
            f"CREATE NAMED COLLECTION ai_content_filter AS "
            f"provider = 'openai', "
            f"endpoint = 'http://localhost:{MOCK_PORT}/v1/chat/content_filter', "
            f"model = 'test-model', "
            f"api_key = 'test-key'"
        )
        instance.query(
            f"CREATE NAMED COLLECTION ai_unknown_reason AS "
            f"provider = 'openai', "
            f"endpoint = 'http://localhost:{MOCK_PORT}/v1/chat/unknown_reason', "
            f"model = 'test-model', "
            f"api_key = 'test-key'"
        )
        instance.query(
            f"CREATE NAMED COLLECTION ai_tool_calls AS "
            f"provider = 'openai', "
            f"endpoint = 'http://localhost:{MOCK_PORT}/v1/chat/tool_calls', "
            f"model = 'test-model', "
            f"api_key = 'test-key'"
        )
        instance.query(
            f"CREATE NAMED COLLECTION ai_anthropic_pause_turn AS "
            f"provider = 'anthropic', "
            f"endpoint = 'http://localhost:{MOCK_PORT}/v1/anthropic/pause_turn', "
            f"model = 'test-model', "
            f"api_key = 'test-key'"
        )
        instance.query(
            f"CREATE NAMED COLLECTION ai_refusal AS "
            f"provider = 'openai', "
            f"endpoint = 'http://localhost:{MOCK_PORT}/v1/chat/refusal', "
            f"model = 'test-model', "
            f"api_key = 'test-key'"
        )
        # Anthropic-provider collections, used to test the Anthropic `stop_reason` normalization.
        instance.query(
            f"CREATE NAMED COLLECTION ai_anthropic_stop_sequence AS "
            f"provider = 'anthropic', "
            f"endpoint = 'http://localhost:{MOCK_PORT}/v1/anthropic/stop_sequence', "
            f"model = 'test-model', "
            f"api_key = 'test-key'"
        )
        instance.query(
            f"CREATE NAMED COLLECTION ai_anthropic_max_tokens AS "
            f"provider = 'anthropic', "
            f"endpoint = 'http://localhost:{MOCK_PORT}/v1/anthropic/max_tokens', "
            f"model = 'test-model', "
            f"api_key = 'test-key'"
        )
        instance.query(
            f"CREATE NAMED COLLECTION ai_anthropic_context_window AS "
            f"provider = 'anthropic', "
            f"endpoint = 'http://localhost:{MOCK_PORT}/v1/anthropic/context_window', "
            f"model = 'test-model', "
            f"api_key = 'test-key'"
        )
        instance.query(
            f"CREATE NAMED COLLECTION ai_anthropic_tool_use AS "
            f"provider = 'anthropic', "
            f"endpoint = 'http://localhost:{MOCK_PORT}/v1/anthropic/tool_use', "
            f"model = 'test-model', "
            f"api_key = 'test-key'"
        )
        # `api_key` is optional (some providers, e.g. a local Ollama, need no auth).
        # This collection omits it so we can assert no `Authorization` header is sent.
        instance.query(
            f"CREATE NAMED COLLECTION ai_no_key AS "
            f"provider = 'openai', "
            f"endpoint = 'http://localhost:{MOCK_PORT}/v1/chat/completions', "
            f"model = 'test-model'"
        )
        instance.query(
            f"CREATE NAMED COLLECTION ai_embed AS "
            f"provider = 'openai', "
            f"endpoint = 'http://localhost:{MOCK_PORT}/v1/embeddings', "
            f"api_key = 'test-key'"
        )
        instance.query(
            f"CREATE NAMED COLLECTION ai_embed_error AS "
            f"provider = 'openai', "
            f"endpoint = 'http://localhost:{MOCK_PORT}/v1/embeddings_error', "
            f"api_key = 'test-key'"
        )
        instance.query(
            f"CREATE NAMED COLLECTION ai_embed_dup_index AS "
            f"provider = 'openai', "
            f"endpoint = 'http://localhost:{MOCK_PORT}/v1/embeddings_dup_index', "
            f"api_key = 'test-key'"
        )
        instance.query(
            f"CREATE NAMED COLLECTION ai_embed_wrong_count AS "
            f"provider = 'openai', "
            f"endpoint = 'http://localhost:{MOCK_PORT}/v1/embeddings_wrong_count', "
            f"api_key = 'test-key'"
        )
        # Endpoints that drop the connection for the first N requests (armed via /set-flaky),
        # used to test that transient network failures are retried like the url table function.
        instance.query(
            f"CREATE NAMED COLLECTION ai_flaky AS "
            f"provider = 'openai', "
            f"endpoint = 'http://localhost:{MOCK_PORT}/v1/chat/flaky', "
            f"model = 'test-model', "
            f"api_key = 'test-key'"
        )
        instance.query(
            f"CREATE NAMED COLLECTION ai_embed_flaky AS "
            f"provider = 'openai', "
            f"endpoint = 'http://localhost:{MOCK_PORT}/v1/embeddings_flaky', "
            f"api_key = 'test-key'"
        )

        instance.query("CREATE TABLE test_input (x String) ENGINE = Memory")
        instance.query(
            "CREATE TABLE test_input_nullable (x Nullable(String)) ENGINE = Memory"
        )
        # MergeTree is required for a real PREWHERE (other engines rewrite it to WHERE).
        instance.query(
            "CREATE TABLE test_filter_mt (x String) ENGINE = MergeTree ORDER BY x"
        )
        instance.query(
            "CREATE TABLE test_filter_join_left (id UInt32, x String) ENGINE = Memory"
        )
        instance.query(
            "CREATE TABLE test_filter_join_right (id UInt32, tag String) ENGINE = Memory"
        )
        instance.query(
            "CREATE TABLE test_input_pairs (id UInt8, a Nullable(String), b Nullable(String)) ENGINE = Memory"
        )

        yield cluster
    finally:
        cluster.shutdown()


# ---------------------------------------------------------------------------
# aiGenerate
# ---------------------------------------------------------------------------


def test_generate_content_basic(started_cluster):
    result = instance.query(
        "SELECT aiGenerate('hello world', map('credentials', 'ai_mock'))",
    )
    assert result.strip() == "hello world"


def test_generate_content_multiple_rows(started_cluster):
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('row1'), ('row2'), ('row3')")
    result = instance.query(
        "SELECT aiGenerate(x, map('credentials', 'ai_mock')) FROM test_input ORDER BY x",
    )
    assert result.strip().split("\n") == ["row1", "row2", "row3"]


def test_generate_uses_text_default_credentials(started_cluster):
    """End-to-end default-credentials path: with no `credentials` in the call, a real (non-empty)
    request must actually use `ai_function_text_default_credentials`, not just resolve it for the
    zero-row fast path. The mock echoes the input back, so a wiring bug would show up here."""
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('row1'), ('row2')")
    result = instance.query(
        "SELECT aiGenerate(x) FROM test_input ORDER BY x",
        settings={"ai_function_text_default_credentials": "ai_mock"},
    )
    assert result.strip().split("\n") == ["row1", "row2"]


def test_generate_content_profile_events(started_cluster):
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('a'), ('b'), ('c')")
    qid = unique_query_id("gen_content_events")
    instance.query(
        "SELECT aiGenerate(x, map('credentials', 'ai_mock')) FROM test_input",
        query_id=qid,
    )
    events = get_profile_events(qid)
    assert int(events["api_calls"]) == 3
    assert int(events["input_tokens"]) == 30  # 3 * 10
    assert int(events["output_tokens"]) == 15  # 3 * 5
    assert int(events["rows_processed"]) == 3
    assert int(events["rows_skipped"]) == 0


def test_generate_content_null_input(started_cluster):
    instance.query("TRUNCATE TABLE test_input_nullable")
    instance.query(
        "INSERT INTO test_input_nullable VALUES (NULL), ('hello'), (NULL)"
    )
    result = instance.query(
        "SELECT aiGenerate(x, map('credentials', 'ai_mock')) FROM test_input_nullable",
    )
    lines = result.strip().split("\n")
    assert lines.count("\\N") == 2
    assert lines.count("hello") == 1


def test_generate_content_error_throw(started_cluster):
    error = instance.query_and_get_error(
        "SELECT aiGenerate('hello', map('credentials', 'ai_error'))",
    )
    assert "RECEIVED_ERROR_FROM_REMOTE_IO_SERVER" in error


def test_generate_content_error_sanitizes_control_chars(started_cluster):
    error = instance.query_and_get_error(
        "SELECT aiGenerate('hello', map('credentials', 'ai_error_control_chars'))",
    )
    assert "RECEIVED_ERROR_FROM_REMOTE_IO_SERVER" in error
    # Control characters in the provider's error message/type are replaced with spaces, so the whole
    # message stays on one line (no forged log lines) while the text is still readable.
    assert "[err type]: start mid end BEL done" in error
    # The raw control sequences must not survive.
    assert "start\nmid" not in error
    assert "mid\tend" not in error


def test_generate_content_error_nonjson_sanitized(started_cluster):
    error = instance.query_and_get_error(
        "SELECT aiGenerate('hello', map('credentials', 'ai_error_nonjson'))",
    )
    assert "RECEIVED_ERROR_FROM_REMOTE_IO_SERVER" in error
    # A non-JSON body falls back to the truncated raw body, sanitized to a single readable line.
    assert "Internal Error: stack trace here" in error
    assert "Error:\nstack" not in error


def test_generate_content_error_graceful(started_cluster):
    result = instance.query(
        "SELECT aiGenerate('hello', map('credentials', 'ai_error'))",
        settings={"ai_function_throw_on_error": 0},
    )
    assert result.strip() == ""


def test_generate_truncated_response_throw(started_cluster):
    """A well-formed response with `finish_reason="length"` (model hit max_tokens) must be
    rejected as truncated rather than silently returning the partial content."""
    error = instance.query_and_get_error(
        "SELECT aiGenerate('hello', map('credentials', 'ai_truncated'))",
    )
    assert "AI_PROVIDER_RESPONSE_TRUNCATED" in error


def test_generate_truncated_response_graceful(started_cluster):
    """With `ai_function_throw_on_error = 0`, a truncated response yields the column default ("")."""
    result = instance.query(
        "SELECT aiGenerate('hello', map('credentials', 'ai_truncated'))",
        settings={"ai_function_throw_on_error": 0},
    )
    assert result.strip() == ""


def test_generate_truncated_response_counts_tokens(started_cluster):
    """A truncated response still consumed provider tokens, so it must be recorded before the
    rejection: otherwise a query full of truncated rows sees a zero token count and keeps
    dispatching requests past `ai_function_max_output_tokens_per_query`."""
    instance.query("TRUNCATE TABLE test_input")
    instance.query(
        "INSERT INTO test_input SELECT 'row_' || toString(number) FROM numbers(3)"
    )
    qid = unique_query_id("gen_truncated_quota")
    # The mock reports 10 input / 5 output tokens per call. The first row is rejected as
    # truncated but exhausts the 5-token output cap, so the remaining two rows are skipped
    # without an API call.
    result = instance.query(
        "SELECT aiGenerate(x, map('credentials', 'ai_truncated')) FROM test_input",
        settings={
            "ai_function_throw_on_error": 0,
            "ai_function_max_output_tokens_per_query": 5,
            "ai_function_throw_on_quota_exceeded": 0,
        },
        query_id=qid,
    )
    assert result.strip() == ""
    events = get_profile_events(qid)
    assert int(events["api_calls"]) == 1
    assert int(events["input_tokens"]) == 10
    assert int(events["output_tokens"]) == 5
    assert int(events["rows_processed"]) == 0
    assert int(events["rows_skipped"]) == 3


def test_generate_truncated_response_records_tokens_when_throwing(started_cluster):
    """Rejecting an incomplete response throws out of `executeImpl`, but the provider already
    charged for the call, so the AI counters must still reach `system.query_log`. Without the RAII
    flush the throwing path reported zero for every counter."""
    qid = unique_query_id("gen_truncated_throw_events")
    error = instance.query_and_get_error(
        "SELECT aiGenerate('hello', map('credentials', 'ai_truncated'))",
        query_id=qid,
    )
    assert "AI_PROVIDER_RESPONSE_TRUNCATED" in error
    # The query threw, so its log row is an exception row rather than QueryFinish.
    events = get_profile_events(qid, query_type="ExceptionWhileProcessing")
    assert int(events["api_calls"]) == 1
    assert int(events["input_tokens"]) == 10
    assert int(events["output_tokens"]) == 5


def test_generate_content_filter_response_throw(started_cluster):
    """`finish_reason="content_filter"` means the answer was withheld/filtered; reject as incomplete."""
    error = instance.query_and_get_error(
        "SELECT aiGenerate('hello', map('credentials', 'ai_content_filter'))",
    )
    assert "AI_PROVIDER_RESPONSE_INCOMPLETE" in error


def test_generate_content_filter_response_graceful(started_cluster):
    result = instance.query(
        "SELECT aiGenerate('hello', map('credentials', 'ai_content_filter'))",
        settings={"ai_function_throw_on_error": 0},
    )
    assert result.strip() == ""


def test_generate_tool_calls_response_throw(started_cluster):
    """OpenAI's `finish_reason="tool_calls"` means the model wants the caller to run a tool, so the
    HTTP 200 carries no final answer. It must be rejected, not returned as empty output."""
    error = instance.query_and_get_error(
        "SELECT aiGenerate('hello', map('credentials', 'ai_tool_calls'))",
    )
    assert "AI_PROVIDER_RESPONSE_INCOMPLETE" in error
    assert "tool_calls" in error
    # ContentFilter raises the same error code with the same reason string, so pin the arm's wording.
    assert "further caller action" in error


def test_generate_tool_calls_response_graceful(started_cluster):
    result = instance.query(
        "SELECT aiGenerate('hello', map('credentials', 'ai_tool_calls'))",
        settings={"ai_function_throw_on_error": 0},
    )
    assert result.strip() == ""


def test_generate_anthropic_pause_turn_throw(started_cluster):
    """Anthropic's `stop_reason="pause_turn"` is a paused multi-turn generation, the same
    "HTTP 200 but not a final answer" case as OpenAI's `tool_calls`."""
    error = instance.query_and_get_error(
        "SELECT aiGenerate('hello', map('credentials', 'ai_anthropic_pause_turn'))",
    )
    assert "AI_PROVIDER_RESPONSE_INCOMPLETE" in error
    assert "pause_turn" in error
    assert "further caller action" in error


def test_generate_refusal_response_throw(started_cluster):
    """A structured-output safety refusal keeps `finish_reason="stop"` and carries the explanation in
    `message.refusal` with a null `content`. Checking `finish_reason` alone would accept it as a
    complete empty answer, so the refusal field must be rejected on its own."""
    error = instance.query_and_get_error(
        "SELECT aiGenerate('hello', map('credentials', 'ai_refusal'))",
    )
    assert "AI_PROVIDER_RESPONSE_INCOMPLETE" in error


def test_generate_refusal_response_graceful(started_cluster):
    result = instance.query(
        "SELECT aiGenerate('hello', map('credentials', 'ai_refusal'))",
        settings={"ai_function_throw_on_error": 0},
    )
    assert result.strip() == ""


def test_generate_unknown_finish_reason_accepted(started_cluster):
    """An unrecognized `finish_reason` must be accepted as a complete answer, not misclassified as
    truncation (regression guard against rejecting any non-"stop" value)."""
    result = instance.query(
        "SELECT aiGenerate('hello unknown', map('credentials', 'ai_unknown_reason'))",
    )
    assert result.strip() == "hello unknown"


def test_generate_anthropic_stop_sequence_accepted(started_cluster):
    """Anthropic's `stop_reason="stop_sequence"` is a complete answer and must NOT be rejected as
    truncated (this is the exact case the string-comparison catch-all got wrong)."""
    result = instance.query(
        "SELECT aiGenerate('hello anthropic', map('credentials', 'ai_anthropic_stop_sequence'))",
    )
    assert result.strip() == "hello anthropic"


def test_generate_anthropic_max_tokens_throw(started_cluster):
    """Anthropic's `stop_reason="max_tokens"` is truncation and must be rejected."""
    error = instance.query_and_get_error(
        "SELECT aiGenerate('hello', map('credentials', 'ai_anthropic_max_tokens'))",
    )
    assert "AI_PROVIDER_RESPONSE_TRUNCATED" in error


def test_generate_anthropic_context_window_hint(started_cluster):
    """`model_context_window_exceeded` is truncation too, but raising max_tokens reserves more output
    space and makes it worse, so the hint must point at reducing the input instead."""
    error = instance.query_and_get_error(
        "SELECT aiGenerate('hello', map('credentials', 'ai_anthropic_context_window'))",
    )
    assert "AI_PROVIDER_RESPONSE_TRUNCATED" in error
    assert "model_context_window_exceeded" in error
    assert "ran out of context window" in error
    assert "larger context window" in error
    # The diagnosis must not name the output token limit either, since that is the wrong limit here.
    assert "Increase max_tokens" not in error
    assert "output token limit" not in error


def test_generate_anthropic_max_tokens_hint(started_cluster):
    """The output-cap case keeps the max_tokens advice, which is correct only for that case."""
    error = instance.query_and_get_error(
        "SELECT aiGenerate('hello', map('credentials', 'ai_anthropic_max_tokens'))",
    )
    assert "output token limit" in error
    assert "Increase max_tokens" in error
    assert "context window" not in error


def test_classify_anthropic_structured_output(started_cluster):
    """Anthropic structured output is a forced tool call, returned with `stop_reason="tool_use"`.
    That is a completed response and must NOT be rejected as incomplete (regression guard: rejecting
    `tool_use` broke every Anthropic `aiClassify`/`aiExtract` call)."""
    result = instance.query(
        "SELECT aiClassify('I love it', ['positive', 'negative', 'neutral'], "
        "map('credentials', 'ai_anthropic_tool_use'))",
    )
    assert result.strip() == "positive"


def test_generate_anthropic_tool_use_rejected(started_cluster):
    """A plain `aiGenerate` request sends no tools, so an Anthropic-compatible endpoint returning
    `stop_reason="tool_use"` is signalling a tool-call turn, not a final answer, and must be rejected.
    `tool_use` is only a completed answer for the forced structured-output path
    (test_classify_anthropic_structured_output)."""
    error = instance.query_and_get_error(
        "SELECT aiGenerate('hello', map('credentials', 'ai_anthropic_tool_use'))",
    )
    assert "AI_PROVIDER_RESPONSE_INCOMPLETE" in error


def last_request():
    return json.loads(
        instance.exec_in_container(
            ["curl", "-s", f"http://localhost:{MOCK_PORT}/last-request"]
        )
    )


def test_generate_without_api_key(started_cluster):
    """A named collection that omits `api_key` resolves and runs end-to-end, and the
    provider sends no `Authorization` header (rather than an empty/dummy token)."""
    result = instance.query(
        "SELECT aiGenerate('no key here', map('credentials', 'ai_no_key'))",
    )
    assert result.strip() == "no key here"
    assert "authorization" not in last_request()["headers"]


def test_generate_with_api_key_sends_auth_header(started_cluster):
    """A keyed collection forwards the key as a `Bearer` `Authorization` header."""
    result = instance.query(
        "SELECT aiGenerate('with key', map('credentials', 'ai_mock'))",
    )
    assert result.strip() == "with key"
    assert last_request()["headers"].get("authorization") == "Bearer test-key"


def test_generate_model_override_with_default_credentials(started_cluster):
    """`map('model', ...)` overrides the collection's model on the actual request, even when the
    collection itself is selected via `ai_function_text_default_credentials` rather than the map."""
    instance.query(
        "SELECT aiGenerate('hi', map('model', 'override-model'))",
        settings={"ai_function_text_default_credentials": "ai_mock"},
    )
    assert json.loads(last_request()["body"])["model"] == "override-model"


def test_embed_model_override_with_default_credentials(started_cluster):
    """Same for aiEmbed: the required positional `model` argument sets the embedding model on the
    request, with the collection selected via `ai_function_embedding_default_credentials`."""
    instance.query(
        "SELECT aiEmbed('hi', 'override-embed-model')",
        settings={"ai_function_embedding_default_credentials": "ai_embed"},
    )
    assert json.loads(last_request()["body"])["model"] == "override-embed-model"


def test_generate_empty_model_override_with_default_credentials(started_cluster):
    """An explicitly empty `model` in the params map overrides the collection's model: the resolver
    honors presence, not content, so `map('model', '')` sends an empty model (letting an endpoint
    pick one), even though the collection selected via the default setting defines `test-model`."""
    instance.query(
        "SELECT aiGenerate('hi', map('model', ''))",
        settings={"ai_function_text_default_credentials": "ai_mock"},
    )
    assert json.loads(last_request()["body"])["model"] == ""


# Setting every credential/config key in the params map at once. `ai_mock` carries an api_key,
# `ai_no_key` does not, so the auth header proves which collection was actually contacted.
_ALL_PARAMS_QUERY = (
    "SELECT aiGenerate('hi', map("
    "'credentials', 'ai_mock', 'model', 'map-model', 'max_tokens', '7', "
    "'temperature', '0.9', 'system_prompt', 'be terse'))"
)


def _assert_all_params_applied():
    req = last_request()
    body = json.loads(req["body"])
    # `credentials` picked `ai_mock` (keyed) — proves the map won over any default setting.
    assert req["headers"].get("authorization") == "Bearer test-key"
    assert body["model"] == "map-model"
    assert body["max_tokens"] == 7
    assert abs(body["temperature"] - 0.9) < 1e-4
    assert body["messages"][0]["role"] == "system"
    assert body["messages"][0]["content"] == "be terse"


def test_generate_all_map_params_override_setting(started_cluster):
    """Every param passed in the map overrides a default-credentials setting that points at a
    different collection: `credentials` (proven via the auth header) plus `model` / `max_tokens` /
    `temperature` / `system_prompt` all take effect."""
    instance.query(
        _ALL_PARAMS_QUERY,
        settings={"ai_function_text_default_credentials": "ai_no_key"},
    )
    _assert_all_params_applied()


def test_generate_all_map_params_without_setting(started_cluster):
    """The same map, with no default-credentials setting at all: the map supplies everything,
    including `credentials`, and all keys take effect."""
    instance.query(_ALL_PARAMS_QUERY)
    _assert_all_params_applied()


# ---------------------------------------------------------------------------
# aiClassify
# ---------------------------------------------------------------------------


def test_classify_basic(started_cluster):
    """aiClassify sends a response_format with enum constraint.
    The mock returns the first enum value."""
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('I love this product!')")
    result = instance.query(
        "SELECT aiClassify(x, ['positive', 'negative', 'neutral'], map('credentials', 'ai_mock')) FROM test_input",
    )
    # Mock returns first enum value; postProcessResponse extracts "category" from JSON
    assert result.strip() == "positive"


def test_classify_multiple_rows(started_cluster):
    instance.query("TRUNCATE TABLE test_input")
    instance.query(
        "INSERT INTO test_input VALUES ('great'), ('terrible'), ('okay')"
    )
    result = instance.query(
        "SELECT aiClassify(x, ['positive', 'negative', 'neutral'], map('credentials', 'ai_mock')) FROM test_input",
    )
    lines = result.strip().split("\n")
    # All rows get the first enum value from the mock
    assert all(l == "positive" for l in lines)
    assert len(lines) == 3


def test_classify_profile_events(started_cluster):
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('a'), ('b')")
    qid = unique_query_id("classify_events")
    instance.query(
        "SELECT aiClassify(x, ['cat_a', 'cat_b'], map('credentials', 'ai_mock')) FROM test_input",
        query_id=qid,
    )
    events = get_profile_events(qid)
    assert int(events["api_calls"]) == 2
    assert int(events["rows_processed"]) == 2


def test_classify_null_input(started_cluster):
    instance.query("TRUNCATE TABLE test_input_nullable")
    instance.query("INSERT INTO test_input_nullable VALUES (NULL), ('text')")
    result = instance.query(
        "SELECT aiClassify(x, ['a', 'b'], map('credentials', 'ai_mock')) FROM test_input_nullable",
    )
    lines = result.strip().split("\n")
    assert len(lines) == 2
    assert "\\N" in lines
    assert "a" in lines


# ---------------------------------------------------------------------------
# aiFilter
# ---------------------------------------------------------------------------


def test_filter_basic(started_cluster):
    """aiFilter asks the model for a bare true/false response.
    The mock returns true for ordinary messages."""
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('The package never arrived')")
    result = instance.query(
        "SELECT aiFilter(x, 'the customer is complaining about shipping', map('credentials', 'ai_mock')) FROM test_input",
    )
    assert result.strip() == "1"


def test_filter_negative(started_cluster):
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('does not match anything')")
    result = instance.query(
        "SELECT aiFilter(x, 'angry about shipping', map('credentials', 'ai_mock')) FROM test_input",
    )
    assert result.strip() == "0"


def test_filter_where(started_cluster):
    instance.query("TRUNCATE TABLE test_input")
    instance.query(
        "INSERT INTO test_input VALUES ('great product'), ('does not match'), ('also good')"
    )
    result = instance.query(
        "SELECT x FROM test_input WHERE aiFilter(x, 'positive feedback', map('credentials', 'ai_mock')) ORDER BY x",
    )
    lines = result.strip().split("\n")
    assert lines == ["also good", "great product"]


def test_filter_truncated_response_throw(started_cluster):
    """aiFilter shares the FunctionBaseAI rejection path. A provider-signalled incomplete reply
    (here `finish_reason="length"`) is an error under the default `ai_function_throw_on_error=1`,
    aborting the query rather than silently dropping the row on a non-answer."""
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('some text')")
    error = instance.query_and_get_error(
        "SELECT aiFilter(x, 'is positive', map('credentials', 'ai_truncated')) FROM test_input",
    )
    assert "AI_PROVIDER_RESPONSE_TRUNCATED" in error


def test_filter_truncated_response_graceful(started_cluster):
    """With `ai_function_throw_on_error=0`, a truncated reply maps to `0` and the row is filtered
    out, preserving aiFilter's fail-closed contract."""
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('some text')")
    result = instance.query(
        "SELECT x FROM test_input WHERE aiFilter(x, 'is positive', map('credentials', 'ai_truncated'))",
        settings={"ai_function_throw_on_error": 0},
    )
    assert result.strip() == ""


def test_filter_no_response_format(started_cluster):
    """aiFilter does not send a JSON-schema response_format; it asks for bare true/false."""
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('hello')")
    instance.query(
        "SELECT aiFilter(x, 'is a greeting', map('credentials', 'ai_mock')) FROM test_input",
    )
    last = json.loads(
        instance.exec_in_container(
            ["curl", "-s", f"http://localhost:{MOCK_PORT}/last-request"]
        )
    )
    body = json.loads(last["body"])
    assert "response_format" not in body
    system = next(m["content"] for m in body["messages"] if m["role"] == "system")
    assert "lowercase text true or false" in system.lower()


def test_filter_null_input(started_cluster):
    instance.query("TRUNCATE TABLE test_input_nullable")
    instance.query("INSERT INTO test_input_nullable VALUES (NULL), ('text')")
    result = instance.query(
        "SELECT aiFilter(x, 'mentions a bug', map('credentials', 'ai_mock')) FROM test_input_nullable",
    )
    lines = result.strip().split("\n")
    assert len(lines) == 2
    assert "\\N" in lines
    assert "1" in lines


def test_filter_profile_events(started_cluster):
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('a'), ('b')")
    qid = unique_query_id("filter_events")
    instance.query(
        "SELECT aiFilter(x, 'is alphabetic', map('credentials', 'ai_mock')) FROM test_input",
        query_id=qid,
    )
    events = get_profile_events(qid)
    assert int(events["api_calls"]) == 2
    assert int(events["rows_processed"]) == 2


def test_filter_prewhere(started_cluster):
    """aiFilter is usable in PREWHERE on MergeTree (same filtering as WHERE)."""
    instance.query("TRUNCATE TABLE test_filter_mt")
    instance.query(
        "INSERT INTO test_filter_mt VALUES ('great product'), ('does not match'), ('also good')"
    )
    qid = unique_query_id("filter_prewhere")
    result = instance.query(
        "SELECT x FROM test_filter_mt "
        "PREWHERE aiFilter(x, 'positive feedback', map('credentials', 'ai_mock')) "
        "ORDER BY x",
        query_id=qid,
    )
    assert result.strip().split("\n") == ["also good", "great product"]
    events = get_profile_events(qid)
    assert int(events["api_calls"]) == 3
    assert int(events["rows_processed"]) == 3


def test_filter_join_on(started_cluster):
    """aiFilter in JOIN ... ON with a left-only predicate: one LLM call per left row.

    When the filter does not depend on the right table, ClickHouse can evaluate it once
    per left row (not once per candidate pair), which is the cheap/correct pattern.
    """
    instance.query("TRUNCATE TABLE test_filter_join_left")
    instance.query("TRUNCATE TABLE test_filter_join_right")
    instance.query(
        "INSERT INTO test_filter_join_left VALUES "
        "(1, 'great product'), (2, 'does not match'), (3, 'also good')"
    )
    instance.query(
        "INSERT INTO test_filter_join_right VALUES "
        "(1, 'a'), (1, 'b'), (2, 'c'), (3, 'd')"
    )
    qid = unique_query_id("filter_join_on")
    result = instance.query(
        """
        SELECT l.x, r.tag
        FROM test_filter_join_left AS l
        INNER JOIN test_filter_join_right AS r
            ON aiFilter(l.x, 'positive feedback', map('credentials', 'ai_mock'))
            AND l.id = r.id
        ORDER BY l.x, r.tag
        """,
        query_id=qid,
    )
    assert result.strip().split("\n") == [
        "also good\td",
        "great product\ta",
        "great product\tb",
    ]
    events = get_profile_events(qid)
    # Three left rows, one API call each — not one call per (left,right) candidate pair.
    assert int(events["api_calls"]) == 3
    assert int(events["rows_processed"]) == 3


def test_filter_join_on_per_pair(started_cluster):
    """aiFilter in JOIN ... ON that depends on both sides is evaluated per candidate pair."""
    instance.query("TRUNCATE TABLE test_filter_join_left")
    instance.query("TRUNCATE TABLE test_filter_join_right")
    instance.query(
        "INSERT INTO test_filter_join_left VALUES (1, 'great product'), (2, 'also good')"
    )
    instance.query(
        "INSERT INTO test_filter_join_right VALUES (1, 'ok'), (1, 'does not match')"
    )
    qid = unique_query_id("filter_join_on_pair")
    result = instance.query(
        """
        SELECT l.x, r.tag
        FROM test_filter_join_left AS l
        INNER JOIN test_filter_join_right AS r
            ON l.id = r.id
            AND aiFilter(
                concat(l.x, ' ', r.tag),
                'positive feedback',
                map('credentials', 'ai_mock')
            )
        ORDER BY l.x, r.tag
        """,
        query_id=qid,
    )
    # Mock returns false when the user message contains "does not match".
    assert result.strip().split("\n") == ["great product\tok"]
    events = get_profile_events(qid)
    # Two right matches for id=1 → two candidate pairs (and one LLM call each).
    assert int(events["api_calls"]) == 2
    assert int(events["rows_processed"]) == 2


# ---------------------------------------------------------------------------
# aiExtract — simple instruction mode
# ---------------------------------------------------------------------------


def test_extract_simple_instruction(started_cluster):
    """With a plain text instruction, aiExtract uses a response_format with a
    single 'result' field. postProcessResponse extracts the value."""
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('The price is $42.99')")
    result = instance.query(
        "SELECT aiExtract(x, 'the price', map('credentials', 'ai_mock')) FROM test_input",
    )
    # Mock returns {"result": "<user_message>"}, postProcess extracts the value
    assert result.strip() == "The price is $42.99"


def test_extract_json_schema(started_cluster):
    """With a JSON schema instruction, aiExtract builds a multi-field response_format.
    The mock populates each field with the user message."""
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('John is 30 years old')")
    result = instance.query(
        """SELECT aiExtract(x, '{"name": "person name", "age": "person age"}', map('credentials', 'ai_mock')) FROM test_input""",
    )
    # Mock returns {"name": "<user_msg>", "age": "<user_msg>"}
    # postProcessResponse returns raw JSON since there's no single "result" field
    parsed = json.loads(result.strip())
    assert "name" in parsed
    assert "age" in parsed


def test_extract_multiple_rows(started_cluster):
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('text1'), ('text2'), ('text3')")
    qid = unique_query_id("extract_events")
    instance.query(
        "SELECT aiExtract(x, 'main topic', map('credentials', 'ai_mock')) FROM test_input",
        query_id=qid,
    )
    events = get_profile_events(qid)
    assert int(events["api_calls"]) == 3
    assert int(events["rows_processed"]) == 3


def test_extract_null_input(started_cluster):
    instance.query("TRUNCATE TABLE test_input_nullable")
    instance.query("INSERT INTO test_input_nullable VALUES (NULL), ('some text')")
    result = instance.query(
        "SELECT aiExtract(x, 'key info', map('credentials', 'ai_mock')) FROM test_input_nullable",
    )
    lines = result.strip().split("\n")
    assert "\\N" in lines
    assert len(lines) == 2


# ---------------------------------------------------------------------------
# aiTranslate
# ---------------------------------------------------------------------------


def test_translate_basic(started_cluster):
    """aiTranslate has no response_format — plain text echo from mock."""
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('Hello world')")
    result = instance.query(
        "SELECT aiTranslate(x, 'French', map('credentials', 'ai_mock')) FROM test_input",
    )
    assert result.strip() == "Hello world"


def test_translate_multiple_rows(started_cluster):
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('one'), ('two'), ('three')")
    result = instance.query(
        "SELECT aiTranslate(x, 'Spanish', map('credentials', 'ai_mock')) FROM test_input ORDER BY x",
    )
    assert result.strip().split("\n") == ["one", "three", "two"]


def test_translate_with_instructions(started_cluster):
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('Hello')")
    result = instance.query(
        "SELECT aiTranslate(x, 'German', map('credentials', 'ai_mock', 'instructions', 'Use formal tone')) FROM test_input",
    )
    assert result.strip() == "Hello"

    last = json.loads(
        instance.exec_in_container(
            ["curl", "-s", f"http://localhost:{MOCK_PORT}/last-request"]
        )
    )
    assert last["path"] == "/v1/chat/completions"
    sent = last["body"]
    assert "German" in sent
    assert "Use formal tone" in sent


def test_translate_profile_events(started_cluster):
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('a'), ('b')")
    qid = unique_query_id("translate_events")
    instance.query(
        "SELECT aiTranslate(x, 'Japanese', map('credentials', 'ai_mock')) FROM test_input",
        query_id=qid,
    )
    events = get_profile_events(qid)
    assert int(events["api_calls"]) == 2
    assert int(events["rows_processed"]) == 2


def test_translate_null_input(started_cluster):
    instance.query("TRUNCATE TABLE test_input_nullable")
    instance.query("INSERT INTO test_input_nullable VALUES (NULL), ('hello')")
    result = instance.query(
        "SELECT aiTranslate(x, 'French', map('credentials', 'ai_mock')) FROM test_input_nullable",
    )
    lines = result.strip().split("\n")
    assert "\\N" in lines
    assert "hello" in lines


# ---------------------------------------------------------------------------
# aiRedact
# ---------------------------------------------------------------------------


def test_redact_basic(started_cluster):
    """aiRedact returns the model's text directly. The mock echoes the user message back."""
    instance.query("TRUNCATE TABLE test_input")
    instance.query(
        "INSERT INTO test_input VALUES ('customer John Doe, john@doe.org')"
    )
    result = instance.query(
        "SELECT aiRedact(x, ['email', 'name'], map('credentials', 'ai_mock')) FROM test_input",
    )
    assert result.strip() == "customer John Doe, john@doe.org"
    # The category list is forwarded to the provider in the system prompt.
    sent = last_request()["body"]
    assert "email" in sent and "name" in sent


def test_redact_default_categories_empty_array(started_cluster):
    """An empty categories array is accepted and falls back to the default set of PII categories."""
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('some text with pii')")
    result = instance.query(
        "SELECT aiRedact(x, [], map('credentials', 'ai_mock')) FROM test_input",
    )
    assert result.strip() == "some text with pii"
    # The mock just echoes the input, so check the documented default category set is what
    # actually reaches the provider.
    system_prompt = json.loads(last_request()["body"])["messages"][0]["content"]
    for category in ("NAME", "EMAIL", "PHONE_NUMBER", "ADDRESS", "CREDIT_CARD", "IP_ADDRESS"):
        assert category in system_prompt, f"default category {category} missing from prompt"


def test_redact_replacement_forwarded(started_cluster):
    """The `replacement` token is embedded in the system prompt sent to the provider."""
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('redact me')")
    instance.query(
        "SELECT aiRedact(x, ['email'], map('credentials', 'ai_mock', 'replacement', '<<HIDDEN>>')) FROM test_input",
    )
    body = json.loads(last_request()["body"])
    assert body["messages"][0]["role"] == "system"
    assert "<<HIDDEN>>" in body["messages"][0]["content"]


def test_redact_multiple_rows(started_cluster):
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('a'), ('b'), ('c')")
    qid = unique_query_id("redact_events")
    instance.query(
        "SELECT aiRedact(x, ['email'], map('credentials', 'ai_mock')) FROM test_input",
        query_id=qid,
    )
    events = get_profile_events(qid)
    assert int(events["api_calls"]) == 3
    assert int(events["rows_processed"]) == 3


def test_redact_null_input(started_cluster):
    instance.query("TRUNCATE TABLE test_input_nullable")
    instance.query("INSERT INTO test_input_nullable VALUES (NULL), ('text')")
    result = instance.query(
        "SELECT aiRedact(x, ['email'], map('credentials', 'ai_mock')) FROM test_input_nullable",
    )
    lines = result.strip().split("\n")
    assert len(lines) == 2
    assert "\\N" in lines
    assert "text" in lines


def test_redact_error_graceful(started_cluster):
    """With ai_function_throw_on_error = 0, a provider error yields an empty string."""
    result = instance.query(
        "SELECT aiRedact('customer John Doe, john@doe.org', ['email', 'name'], map('credentials', 'ai_error'))",
        settings={"ai_function_throw_on_error": 0},
    )
    assert result.strip() == ""


def test_redact_error_throw(started_cluster):
    """By default (`ai_function_throw_on_error = 1`) a provider error propagates."""
    error = instance.query_and_get_error(
        "SELECT aiRedact('secret', ['email'], map('credentials', 'ai_error'))",
    )
    assert "RECEIVED_ERROR_FROM_REMOTE_IO_SERVER" in error


def test_redact_truncated_response_throw(started_cluster):
    """A truncated redaction reply (`finish_reason="length"`) must be rejected, not returned as
    partially redacted text: for a PII-redaction function, silently returning a truncated answer
    would leak unredacted content."""
    error = instance.query_and_get_error(
        "SELECT aiRedact('customer John Doe, john@doe.org', ['email', 'name'], map('credentials', 'ai_truncated'))",
    )
    assert "AI_PROVIDER_RESPONSE_TRUNCATED" in error


def test_redact_truncated_response_graceful(started_cluster):
    """With `ai_function_throw_on_error = 0`, a truncated redaction reply yields the column default
    ("") instead of partially redacted text."""
    result = instance.query(
        "SELECT aiRedact('customer John Doe, john@doe.org', ['email', 'name'], map('credentials', 'ai_truncated'))",
        settings={"ai_function_throw_on_error": 0},
    )
    assert result.strip() == ""


# ---------------------------------------------------------------------------
# aiEmbed
# ---------------------------------------------------------------------------


def parse_embedding(s):
    """Parse a TabSeparated `Array(Float32)` cell like '[0.1,0.2,0.3]' into a list."""
    s = s.strip()
    if not s or s == "[]":
        return []
    return [float(v) for v in s.strip("[]").split(",")]


def test_embed_basic(started_cluster):
    """Single-row aiEmbed returns an `Array(Float32)` of the model's native size."""
    result = instance.query(
        "SELECT aiEmbed('hello', 'test-embed-model', map('credentials', 'ai_embed'))",
    )
    vec = parse_embedding(result)
    assert len(vec) == 4  # DEFAULT_EMBED_DIM in mock server
    assert any(v != 0.0 for v in vec)


def test_embed_rejects_model_in_named_collection(started_cluster):
    """aiEmbed takes `model` as a positional argument and never reads it from the named collection.
    A collection that defines `model` (e.g. the text collection `ai_mock`) is rejected rather than
    silently ignored."""
    error = instance.query_and_get_error(
        "SELECT aiEmbed('hello', 'test-embed-model', map('credentials', 'ai_mock'))",
    )
    assert "BAD_ARGUMENTS" in error
    assert "defines 'model'" in error


def test_embed_uses_embedding_default_credentials(started_cluster):
    """End-to-end default-credentials path for embeddings: with no `credentials` in the call, a real
    (non-empty) request must actually use `ai_function_embedding_default_credentials`. Confirms the
    embedding default is applied on the request path, not only for the zero-row fast path."""
    result = instance.query(
        "SELECT aiEmbed('hello', 'test-embed-model')",
        settings={"ai_function_embedding_default_credentials": "ai_embed"},
    )
    vec = parse_embedding(result)
    assert len(vec) == 4  # DEFAULT_EMBED_DIM in mock server
    assert any(v != 0.0 for v in vec)


def test_embed_multiple_rows(started_cluster):
    """Multiple rows go through one batched request; each row gets its own vector."""
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('alpha'), ('beta'), ('gamma')")
    result = instance.query(
        "SELECT aiEmbed(x, 'test-embed-model', map('credentials', 'ai_embed')) FROM test_input ORDER BY x",
    )
    rows = [parse_embedding(line) for line in result.strip().split("\n")]
    assert len(rows) == 3
    assert all(len(v) == 4 for v in rows)
    # Different inputs should yield different vectors (mock uses input bytes).
    assert len({tuple(v) for v in rows}) == 3


def test_embed_with_dimensions(started_cluster):
    """The `dimensions` argument is forwarded to the provider and honored in the response."""
    result = instance.query(
        "SELECT aiEmbed('hello world', 'test-embed-model', map('credentials', 'ai_embed', 'dimensions', '16'))",
    )
    vec = parse_embedding(result)
    assert len(vec) == 16


def test_embed_null_and_empty_input(started_cluster):
    """`NULL` and empty-string inputs map to `[]` without making an API call."""
    instance.query("TRUNCATE TABLE test_input_nullable")
    instance.query(
        "INSERT INTO test_input_nullable VALUES (NULL), (''), ('hi')"
    )
    qid = unique_query_id("embed_null_empty")
    result = instance.query(
        "SELECT aiEmbed(x, 'test-embed-model', map('credentials', 'ai_embed')) FROM test_input_nullable ORDER BY x NULLS FIRST",
        query_id=qid,
    )
    rows = [parse_embedding(line) for line in result.strip().split("\n")]
    assert len(rows) == 3
    empties = sum(1 for v in rows if v == [])
    non_empties = sum(1 for v in rows if v)
    assert empties == 2
    assert non_empties == 1

    events = get_profile_events(qid)
    # Only the single non-empty row triggers an API call; NULL/'' are pre-filtered
    # and contribute to neither `rows_processed` nor `rows_skipped` (the latter is
    # reserved for rows that received a default value due to quota or error).
    assert int(events["api_calls"]) == 1
    assert int(events["rows_processed"]) == 1
    assert int(events["rows_skipped"]) == 0


def test_embed_profile_events_token_accounting(started_cluster):
    """`AIInputTokens` accumulates across rows. Mock reports `prompt_tokens = sum(len(inputs))`."""
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('abc'), ('de'), ('fghi')")
    qid = unique_query_id("embed_tokens")
    instance.query(
        "SELECT aiEmbed(x, 'test-embed-model', map('credentials', 'ai_embed')) FROM test_input",
        query_id=qid,
    )
    events = get_profile_events(qid)
    # All three rows fit in one batched call (default batch size is 100).
    assert int(events["api_calls"]) == 1
    assert int(events["input_tokens"]) == 3 + 2 + 4
    assert int(events["rows_processed"]) == 3
    assert int(events["rows_skipped"]) == 0


def test_embed_batching(started_cluster):
    """`ai_function_embedding_max_batch_size` splits inputs across HTTP calls."""
    instance.query("TRUNCATE TABLE test_input")
    instance.query(
        "INSERT INTO test_input SELECT 'row_' || toString(number) FROM numbers(5)"
    )
    qid = unique_query_id("embed_batch")
    instance.query(
        "SELECT aiEmbed(x, 'test-embed-model', map('credentials', 'ai_embed')) FROM test_input",
        settings={"ai_function_embedding_max_batch_size": 2},
        query_id=qid,
    )
    events = get_profile_events(qid)
    # 5 rows / batch of 2 -> ceil(5/2) = 3 HTTP calls.
    assert int(events["api_calls"]) == 3
    assert int(events["rows_processed"]) == 5
    assert int(events["rows_skipped"]) == 0


def test_embed_error_throw(started_cluster):
    """By default, provider errors propagate as `RECEIVED_ERROR_FROM_REMOTE_IO_SERVER`."""
    error = instance.query_and_get_error(
        "SELECT aiEmbed('hello', 'test-embed-model', map('credentials', 'ai_embed_error'))",
    )
    assert "RECEIVED_ERROR_FROM_REMOTE_IO_SERVER" in error


def test_embed_quota_throw_records_input_tokens(started_cluster):
    """With `ai_function_throw_on_quota_exceeded = 1` the second batch throws from `checkQuotas`, so
    the tokens the first batch really consumed must still be reported. Pins `AIInputTokens`, which the
    failed-request tests cannot: there the very first call fails, leaving nothing to count."""
    instance.query("TRUNCATE TABLE test_input")
    instance.query(
        "INSERT INTO test_input SELECT 'row_' || toString(number) FROM numbers(4)"
    )
    qid = unique_query_id("embed_quota_throw")
    # Batch size 1 and rows of length 5 ("row_0".."row_3"): the first batch consumes the whole
    # 5-token cap, so the second trips the quota and raises instead of skipping.
    error = instance.query_and_get_error(
        "SELECT aiEmbed(x, 'test-embed-model', map('credentials', 'ai_embed')) FROM test_input",
        settings={
            "ai_function_embedding_max_batch_size": 1,
            "ai_function_max_input_tokens_per_query": 5,
            "ai_function_throw_on_quota_exceeded": 1,
        },
        query_id=qid,
    )
    assert "LIMIT_EXCEEDED" in error
    events = get_profile_events(qid, query_type="ExceptionWhileProcessing")
    assert int(events["api_calls"]) == 1
    assert int(events["input_tokens"]) == 5  # "row_0"


def test_embed_quota_throw_records_rows_processed(started_cluster):
    """Same throw, seen through the row counters: `aiEmbed` embeds one text per row, so the rows the
    first batch did embed must survive `embedTexts` throwing on the second."""
    instance.query("TRUNCATE TABLE test_input")
    instance.query(
        "INSERT INTO test_input SELECT 'row_' || toString(number) FROM numbers(4)"
    )
    qid = unique_query_id("embed_quota_throw_rows")
    error = instance.query_and_get_error(
        "SELECT aiEmbed(x, 'test-embed-model', map('credentials', 'ai_embed')) FROM test_input",
        settings={
            "ai_function_embedding_max_batch_size": 1,
            "ai_function_max_input_tokens_per_query": 5,
            "ai_function_throw_on_quota_exceeded": 1,
        },
        query_id=qid,
    )
    assert "LIMIT_EXCEEDED" in error
    events = get_profile_events(qid, query_type="ExceptionWhileProcessing")
    assert int(events["rows_processed"]) == 1  # "row_0" was embedded before the quota tripped
    assert int(events["rows_skipped"]) == 0  # the quota raised instead of skipping


def test_embed_malformed_response_records_input_tokens(started_cluster):
    """A `200` body the provider billed for but that fails validation still consumed tokens, so they must
    reach `system.query_log` and `AIQuotaTracker` rather than being lost with the rejected payload."""
    qid = unique_query_id("embed_malformed_tokens")
    error = instance.query_and_get_error(
        "SELECT aiEmbed(x, 'test-embed-model', map('credentials', 'ai_embed_dup_index')) FROM (SELECT arrayJoin(['a', 'b']) AS x)",
        settings={"ai_function_max_retries": 0},
        query_id=qid,
    )
    assert "MALFORMED_AI_PROVIDER_RESPONSE" in error
    events = get_profile_events(qid, query_type="ExceptionWhileProcessing")
    assert int(events["api_calls"]) == 1
    assert int(events["input_tokens"]) == 2  # the mock bills one token per input character: "a", "b"


def test_generate_malformed_response_records_input_tokens(started_cluster):
    """Same guarantee on the text path: a chat `200` the provider billed for still reports its tokens when
    the body fails validation. Uses `ai_function_throw_on_error = 0` so the query reaches `QueryFinish`."""
    qid = unique_query_id("generate_malformed_tokens")
    result = instance.query(
        "SELECT aiGenerate('hi', map('credentials', 'ai_no_choices'))",
        settings={
            "ai_function_throw_on_error": 0,
            "ai_function_max_retries": 0,
        },
        query_id=qid,
    )
    assert result.strip() == ""  # the rejected response yields no output
    events = get_profile_events(qid)
    assert int(events["api_calls"]) == 1
    assert int(events["input_tokens"]) == 7  # `usage.prompt_tokens` of the rejected body


def test_generate_malformed_response_counts_tokens_against_quota(started_cluster):
    """The tokens of a billed-but-rejected response must reach `AIQuotaTracker`, not only `system.query_log`:
    the first row spends the whole input-token cap, so the second row is never dispatched."""
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('a'), ('b')")
    qid = unique_query_id("generate_malformed_quota")
    instance.query(
        "SELECT aiGenerate(x, map('credentials', 'ai_no_choices')) FROM test_input",
        settings={
            "ai_function_throw_on_error": 0,
            "ai_function_throw_on_quota_exceeded": 0,
            "ai_function_max_retries": 0,
            "ai_function_max_input_tokens_per_query": 7,
        },
        query_id=qid,
    )
    events = get_profile_events(qid)
    # One request only: the first response's 7 rejected-but-billed tokens met the cap.
    assert int(events["api_calls"]) == 1
    assert int(events["input_tokens"]) == 7
    assert int(events["rows_skipped"]) == 2  # one rejected response, one row never dispatched


def test_anthropic_malformed_response_records_input_tokens(started_cluster):
    """Same guarantee through `AnthropicProvider`, whose body shape and usage keys differ from OpenAI's."""
    qid = unique_query_id("anthropic_malformed_tokens")
    result = instance.query(
        "SELECT aiGenerate('hi', map('credentials', 'ai_anthropic_no_content'))",
        settings={
            "ai_function_throw_on_error": 0,
            "ai_function_max_retries": 0,
        },
        query_id=qid,
    )
    assert result.strip() == ""  # the rejected response yields no output
    assert int(get_profile_events(qid)["input_tokens"]) == 9  # `usage.input_tokens` of the rejected body


def test_similarity_row_counters_stay_zero_on_throw(started_cluster):
    """`aiSimilarity` scores rows only once every batch is embedded, so a throw mid-embedding leaves no
    scored row to report even though the first row's pair was embedded and billed. Pins that split: the
    embedding counters are reported, the row counters are zero because no row was scored."""
    qid = unique_query_id("sim_throw_rows")
    # Batch size 2 over rows ('a','b') and ('c','d'): the first batch embeds row 0's pair and consumes the
    # 2-token cap, so the second batch raises instead of embedding row 1.
    error = instance.query_and_get_error(
        "SELECT aiSimilarity(p.1, p.2, 'test-embed-model', map('credentials', 'ai_embed')) "
        "FROM (SELECT arrayJoin([('a', 'b'), ('c', 'd')]) AS p)",
        settings={
            "ai_function_embedding_max_batch_size": 2,
            "ai_function_max_input_tokens_per_query": 2,
            "ai_function_throw_on_quota_exceeded": 1,
        },
        query_id=qid,
    )
    assert "LIMIT_EXCEEDED" in error
    events = get_profile_events(qid, query_type="ExceptionWhileProcessing")
    assert int(events["api_calls"]) == 1
    assert int(events["input_tokens"]) == 2  # "a" and "b" were embedded and billed
    assert int(events["rows_processed"]) == 0
    assert int(events["rows_skipped"]) == 0


def test_embed_error_throw_records_api_calls(started_cluster):
    """The provider was called and charged for it, so `embedTexts` must report the usage counters even
    though it rethrows. They used to be lost with the `EmbeddingResult` that never reached the caller."""
    qid = unique_query_id("embed_error_throw_events")
    error = instance.query_and_get_error(
        "SELECT aiEmbed('hello', 'test-embed-model', map('credentials', 'ai_embed_error'))",
        settings={"ai_function_max_retries": 0},
        query_id=qid,
    )
    assert "RECEIVED_ERROR_FROM_REMOTE_IO_SERVER" in error
    # The query threw, so its log row is an exception row rather than QueryFinish.
    events = get_profile_events(qid, query_type="ExceptionWhileProcessing")
    assert int(events["api_calls"]) == 1  # one attempt, retries disabled


def test_similarity_error_throw_records_api_calls(started_cluster):
    """Same guarantee through the other `embedTexts` caller, which counts rows differently."""
    qid = unique_query_id("similarity_error_throw_events")
    instance.query_and_get_error(
        "SELECT aiSimilarity('a', 'b', 'test-embed-model', map('credentials', 'ai_embed_error'))",
        settings={"ai_function_max_retries": 0},
        query_id=qid,
    )
    events = get_profile_events(qid, query_type="ExceptionWhileProcessing")
    assert int(events["api_calls"]) == 1


def test_embed_error_graceful(started_cluster):
    """With `ai_function_throw_on_error = 0` the failed batch's rows become `[]`."""
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('a'), ('b')")
    result = instance.query(
        "SELECT aiEmbed(x, 'test-embed-model', map('credentials', 'ai_embed_error')) FROM test_input",
        settings={
            "ai_function_throw_on_error": 0,
            "ai_function_max_retries": 0,
        },
    )
    rows = [parse_embedding(line) for line in result.strip().split("\n")]
    assert rows == [[], []]


def test_embed_duplicate_index_rejected(started_cluster):
    """`OpenAIProvider::embed` rejects responses with duplicate `index` values."""
    error = instance.query_and_get_error(
        "SELECT aiEmbed(x, 'test-embed-model', map('credentials', 'ai_embed_dup_index')) FROM (SELECT arrayJoin(['a', 'b']) AS x)",
        settings={"ai_function_max_retries": 0},
    )
    assert "MALFORMED_AI_PROVIDER_RESPONSE" in error
    assert "duplicates" in error or "duplicate" in error.lower()


def test_embed_wrong_count_rejected(started_cluster):
    """`OpenAIProvider::embed` rejects responses whose `data` size != number of inputs."""
    error = instance.query_and_get_error(
        "SELECT aiEmbed(x, 'test-embed-model', map('credentials', 'ai_embed_wrong_count')) FROM (SELECT arrayJoin(['a', 'b']) AS x)",
        settings={"ai_function_max_retries": 0},
    )
    assert "MALFORMED_AI_PROVIDER_RESPONSE" in error


def test_embed_empty_input_table(started_cluster):
    """Zero-row input must not make any API calls."""
    instance.query("TRUNCATE TABLE test_input")
    qid = unique_query_id("embed_zero_rows")
    result = instance.query(
        "SELECT aiEmbed(x, 'test-embed-model', map('credentials', 'ai_embed')) FROM test_input",
        query_id=qid,
    )
    assert result.strip() == ""
    events = get_profile_events(qid)
    assert int(events["api_calls"]) == 0
    assert int(events["rows_processed"]) == 0


def test_embed_quota_input_tokens_exceeded(started_cluster):
    """When the input-token quota is exceeded, remaining batches are skipped."""
    instance.query("TRUNCATE TABLE test_input")
    instance.query(
        "INSERT INTO test_input SELECT 'row_' || toString(number) FROM numbers(4)"
    )
    qid = unique_query_id("embed_quota")
    # Each batch costs `sum(len(text))` input tokens. With batch_size=1 and rows
    # of length 5 ("row_0".."row_3"), the second batch pushes us over a 5-token cap.
    result = instance.query(
        "SELECT aiEmbed(x, 'test-embed-model', map('credentials', 'ai_embed')) FROM test_input",
        settings={
            "ai_function_embedding_max_batch_size": 1,
            "ai_function_max_input_tokens_per_query": 5,
            "ai_function_throw_on_quota_exceeded": 0,
        },
        query_id=qid,
    )
    rows = [parse_embedding(line) for line in result.strip().split("\n")]
    # First batch succeeds, remaining batches are aborted and produce [].
    assert sum(1 for r in rows if r) == 1
    assert sum(1 for r in rows if not r) == 3
    events = get_profile_events(qid)
    assert int(events["api_calls"]) == 1
    # Quota-aborted live rows count as `rows_skipped` — they received a default
    # value due to a quota cut, matching the documented `AIRowsSkipped` semantics.
    assert int(events["rows_processed"]) == 1
    assert int(events["rows_skipped"]) == 3


# ---------------------------------------------------------------------------
# Retry on transient network errors (like the url table function)
# ---------------------------------------------------------------------------


def set_flaky(count):
    """Arm the mock's flaky endpoints to fail their next `count` requests with a dropped
    connection (a transient network error). `count=0` disarms them."""
    instance.exec_in_container(
        ["curl", "-s", f"http://localhost:{MOCK_PORT}/set-flaky?count={count}"]
    )


def test_generate_retries_on_network_error(started_cluster):
    """A transient network failure (connection dropped without a response) is retried, matching
    the url table function. With enough retries the call recovers and ultimately succeeds."""
    set_flaky(2)
    qid = unique_query_id("gen_retry_net")
    result = instance.query(
        "SELECT aiGenerate('recover me', map('credentials', 'ai_flaky'))",
        settings={
            "ai_function_max_retries": 5,
        },
        query_id=qid,
    )
    assert result.strip() == "recover me"
    events = get_profile_events(qid)
    # 2 failed attempts + 1 successful attempt for the single row.
    assert int(events["api_calls"]) == 3
    assert int(events["rows_processed"]) == 1


def test_generate_network_error_not_retried_when_disabled(started_cluster):
    """With `ai_function_max_retries = 0`, a network failure is surfaced rather than retried."""
    set_flaky(10)
    try:
        error = instance.query_and_get_error(
            "SELECT aiGenerate('no retry', map('credentials', 'ai_flaky'))",
            settings={
                "ai_function_max_retries": 0,
            },
        )
        assert error  # a network/IO error is raised instead of a result
    finally:
        set_flaky(0)


def test_embed_retries_on_network_error(started_cluster):
    """The embedding path retries transient network failures too."""
    set_flaky(2)
    qid = unique_query_id("embed_retry_net")
    result = instance.query(
        "SELECT aiEmbed('hello', 'test-embed-model', map('credentials', 'ai_embed_flaky'))",
        settings={
            "ai_function_max_retries": 5,
        },
        query_id=qid,
    )
    vec = parse_embedding(result)
    assert len(vec) == 4  # DEFAULT_EMBED_DIM in mock server
    events = get_profile_events(qid)
    assert int(events["api_calls"]) == 3
    assert int(events["rows_processed"]) == 1


# ---------------------------------------------------------------------------
# Provider HTTP-status retry policy (matches the url table function):
# deterministic client errors (400/401/403/404/405/501) are surfaced immediately,
# transient/server-side errors (5xx, …) are retried.
# ---------------------------------------------------------------------------


def test_generate_deterministic_http_error_not_retried(started_cluster):
    """A deterministic provider HTTP status (400 Bad Request) is surfaced immediately and is NOT
    retried, even with `ai_function_max_retries` enabled — exactly like the url table function,
    which never retries 400/401/403/404/405/501. Only a single API call is made."""
    qid = unique_query_id("gen_400_no_retry")
    result = instance.query(
        "SELECT aiGenerate('bad request', map('credentials', 'ai_bad_request'))",
        settings={
            "ai_function_max_retries": 5,
            "ai_function_throw_on_error": 0,
        },
        query_id=qid,
    )
    # Non-retriable error with throw_on_error = 0: the row is skipped, producing an empty result.
    assert result.strip() == ""
    events = get_profile_events(qid)
    assert int(events["api_calls"]) == 1  # exactly one call: the 400 was not retried
    assert int(events["rows_processed"]) == 0
    assert int(events["rows_skipped"]) == 1


def test_generate_deterministic_http_error_throws(started_cluster):
    """With the default `ai_function_throw_on_error = 1`, the deterministic 400 surfaces as
    `RECEIVED_ERROR_FROM_REMOTE_IO_SERVER` rather than being retried away."""
    error = instance.query_and_get_error(
        "SELECT aiGenerate('bad request', map('credentials', 'ai_bad_request'))",
        settings={
            "ai_function_max_retries": 5,
        },
    )
    assert "RECEIVED_ERROR_FROM_REMOTE_IO_SERVER" in error


def test_generate_server_error_is_retried(started_cluster):
    """Counterpart to the 400 case: an HTTP 500 is a transient/server-side error, so it IS retried
    (1 initial attempt + `ai_function_max_retries` retries), matching the url table function."""
    qid = unique_query_id("gen_500_retried")
    result = instance.query(
        "SELECT aiGenerate('server error', map('credentials', 'ai_error'))",
        settings={
            "ai_function_max_retries": 2,
            "ai_function_retry_initial_delay_ms": 1,  # keep the test fast
            "ai_function_throw_on_error": 0,
        },
        query_id=qid,
    )
    assert result.strip() == ""
    events = get_profile_events(qid)
    assert int(events["api_calls"]) == 3  # 1 + 2 retries
    assert int(events["rows_skipped"]) == 1


# ---------------------------------------------------------------------------
# The API-call quota bounds retries: `ai_function_max_api_calls_per_query` caps the
# total number of HTTP requests per query, including retried requests, so a flaky
# endpoint cannot dispatch `1 + ai_function_max_retries` requests for a single row/batch.
# ---------------------------------------------------------------------------


def test_generate_retry_respects_api_call_quota(started_cluster):
    """An HTTP 500 is retriable, but the API-call quota is enforced before every attempt — including
    retries. With `ai_function_max_api_calls_per_query = 1` and `ai_function_max_retries = 5`, only a
    single request is dispatched (the quota stops the retries), not `1 + 5`."""
    qid = unique_query_id("gen_quota_caps_retries")
    result = instance.query(
        "SELECT aiGenerate('server error', map('credentials', 'ai_error'))",
        settings={
            "ai_function_max_retries": 5,
            "ai_function_retry_initial_delay_ms": 1,  # keep the test fast
            "ai_function_max_api_calls_per_query": 1,
            "ai_function_throw_on_error": 0,
            "ai_function_throw_on_quota_exceeded": 0,
        },
        query_id=qid,
    )
    assert result.strip() == ""
    events = get_profile_events(qid)
    # Without the per-attempt quota check this would be 6 (1 initial + 5 retries).
    assert int(events["api_calls"]) == 1
    assert int(events["rows_processed"]) == 0
    assert int(events["rows_skipped"]) == 1


def test_function_name_header(started_cluster):
    """The OpenAI provider tags every request with an `X-ClickHouse-AI-Function` header carrying the
    SQL name of the calling function, so the upstream endpoint can tell which function made the call.
    Covers the chat path (aiGenerate/aiClassify/aiExtract/aiTranslate) and the embedding path
    (aiEmbed, aiSimilarity)."""
    cases = [
        ("aiGenerate", "SELECT aiGenerate('hi', map('credentials', 'ai_mock'))"),
        (
            "aiClassify",
            "SELECT aiClassify('hi', ['a', 'b'], map('credentials', 'ai_mock'))",
        ),
        ("aiExtract", "SELECT aiExtract('hi', 'the price', map('credentials', 'ai_mock'))"),
        ("aiTranslate", "SELECT aiTranslate('hi', 'French', map('credentials', 'ai_mock'))"),
        (
            "aiEmbed",
            "SELECT aiEmbed('hi', 'test-embed-model', map('credentials', 'ai_embed'))",
        ),
        (
            "aiSimilarity",
            "SELECT aiSimilarity('cat', 'kitten', 'test-embed-model', map('credentials', 'ai_embed'))",
        ),
    ]
    for name, query in cases:
        instance.query(query)
        assert last_request()["headers"].get("x-clickhouse-ai-function") == name


def test_embed_retry_respects_api_call_quota(started_cluster):
    """The embedding path enforces the same per-attempt API-call quota: a retriable HTTP 500 is not
    retried past `ai_function_max_api_calls_per_query`."""
    qid = unique_query_id("embed_quota_caps_retries")
    result = instance.query(
        "SELECT aiEmbed('server error', 'test-embed-model', map('credentials', 'ai_embed_error'))",
        settings={
            "ai_function_max_retries": 5,
            "ai_function_retry_initial_delay_ms": 1,  # keep the test fast
            "ai_function_max_api_calls_per_query": 1,
            "ai_function_throw_on_error": 0,
            "ai_function_throw_on_quota_exceeded": 0,
        },
        query_id=qid,
    )
    # The single live row is skipped (empty array) because its batch never succeeded.
    assert parse_embedding(result) == []
    events = get_profile_events(qid)
    # Without the per-attempt quota check this would be 6 (1 initial + 5 retries).
    assert int(events["api_calls"]) == 1
    assert int(events["rows_processed"]) == 0
    assert int(events["rows_skipped"]) == 1


def test_embed_const_nullable_operand(started_cluster):
    """`aiEmbed` reads its text via the same `isNullAt`/`getDataAt` path, so a `ColumnConst(ColumnNullable)`
    input works: a const NULL yields an empty array with no request, a const non-null value is embedded."""
    qid = unique_query_id("embed_const_null")
    null_result = instance.query(
        "SELECT aiEmbed(CAST(NULL AS Nullable(String)), 'test-embed-model', map('credentials', 'ai_embed'))",
        query_id=qid,
    )
    assert parse_embedding(null_result) == []
    # The const NULL yields `[]` locally, so no embedding request is made.
    assert int(get_profile_events(qid)["api_calls"]) == 0
    value_result = instance.query(
        "SELECT aiEmbed(CAST('cat' AS Nullable(String)), 'test-embed-model', map('credentials', 'ai_embed'))",
    )
    assert len(parse_embedding(value_result)) > 0


# ---------------------------------------------------------------------------
# aiSimilarity
# ---------------------------------------------------------------------------


def parse_nullable_float(s):
    """Parse a TabSeparated `Nullable(Float32)` cell; `\\N` -> None."""
    s = s.strip()
    return None if s == "\\N" else float(s)


def mock_embedding(text, dim):
    """Mirror of `make_embedding_vector` in mock_ai_server.py, to compute expected scores."""
    if not text:
        return [0.0] * dim
    return [round(((ord(text[i % len(text)]) * (i + 1)) % 1000) / 1000.0, 3) for i in range(dim)]


def expected_similarity(t1, t2, dim=4):
    """Cosine similarity of the two mock vectors, in [-1, 1] (matches the C++ function)."""
    import math

    a = mock_embedding(t1, dim)
    b = mock_embedding(t2, dim)
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(y * y for y in b))
    if na == 0 or nb == 0:
        return None
    return max(-1.0, min(1.0, dot / (na * nb)))


def test_similarity_identical_is_one(started_cluster):
    """Identical texts score exactly 1.0 (cosine of a vector with itself)."""
    qid = unique_query_id("sim_identical")
    result = instance.query(
        "SELECT aiSimilarity('hello', 'hello', 'test-embed-model', map('credentials', 'ai_embed'))",
        query_id=qid,
    )
    assert parse_nullable_float(result) == pytest.approx(1.0, abs=1e-6)
    events = get_profile_events(qid)
    # Both operands are embedded in a single batch (one HTTP request).
    assert int(events["api_calls"]) == 1
    assert int(events["rows_processed"]) == 1


def test_similarity_matches_cosine_formula(started_cluster):
    """Two different texts score the exact cosine similarity value."""
    qid = unique_query_id("sim_formula")
    result = instance.query(
        "SELECT aiSimilarity('cat', 'kitten', 'test-embed-model', map('credentials', 'ai_embed'))",
        query_id=qid,
    )
    score = parse_nullable_float(result)
    assert score == pytest.approx(expected_similarity("cat", "kitten"), abs=1e-4)
    assert -1.0 <= score <= 1.0
    events = get_profile_events(qid)
    # `rows_processed` is row-level: one row produced one score, even though two texts were embedded.
    assert int(events["rows_processed"]) == 1


def test_similarity_uses_embedding_default_credentials(started_cluster):
    """With no `credentials` in the call, the embedding default-credentials setting is used end-to-end."""
    result = instance.query(
        "SELECT aiSimilarity('hello', 'hello', 'test-embed-model')",
        settings={"ai_function_embedding_default_credentials": "ai_embed"},
    )
    assert parse_nullable_float(result) == pytest.approx(1.0, abs=1e-6)


def test_similarity_rows_processed_is_row_level(started_cluster):
    """`rows_processed` counts rows that received a score, not embeddings — a row embeds up to two operands,
    so the two counts are not equal."""
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('a'), ('a'), ('a')")
    qid = unique_query_id("sim_rows_processed")
    result = instance.query(
        "SELECT aiSimilarity(x, 'q', 'test-embed-model', map('credentials', 'ai_embed')) FROM test_input ORDER BY x",
        query_id=qid,
    )
    scores = [parse_nullable_float(line) for line in result.strip().split("\n")]
    assert all(s == pytest.approx(expected_similarity("a", "q"), abs=1e-4) for s in scores)
    events = get_profile_events(qid)
    # The 3 rows embed 6 operands (x and 'q' per row) in a single batch, but all 3 rows receive a score,
    # so rows_processed counts rows (3), not embeddings (6).
    assert int(events["api_calls"]) == 1
    assert int(events["rows_processed"]) == 3


def test_similarity_null_and_empty_operands(started_cluster):
    """A NULL or empty operand yields a NULL score; the non-empty operands are embedded."""
    instance.query("TRUNCATE TABLE test_input_nullable")
    instance.query("INSERT INTO test_input_nullable VALUES (NULL), (''), ('hi')")
    qid = unique_query_id("sim_null_empty")
    result = instance.query(
        "SELECT aiSimilarity(x, 'hi', 'test-embed-model', map('credentials', 'ai_embed')) FROM test_input_nullable ORDER BY x NULLS FIRST",
        query_id=qid,
    )
    scores = [parse_nullable_float(line) for line in result.strip().split("\n")]
    # NULL and '' -> NULL; 'hi' vs 'hi' -> 1.0.
    assert scores[0] is None
    assert scores[1] is None
    assert scores[2] == pytest.approx(1.0, abs=1e-6)
    events = get_profile_events(qid)
    # The non-empty operands are embedded in a single batch (one HTTP request).
    assert int(events["api_calls"]) == 1
    assert int(events["rows_processed"]) == 1


def test_similarity_with_dimensions(started_cluster):
    """`dimensions` is forwarded to the provider; the score is still computed over the larger vectors."""
    result = instance.query(
        "SELECT aiSimilarity('cat', 'kitten', 'test-embed-model', map('credentials', 'ai_embed', 'dimensions', '16'))",
    )
    score = parse_nullable_float(result)
    assert score == pytest.approx(expected_similarity("cat", "kitten", dim=16), abs=1e-4)


def test_similarity_error_graceful(started_cluster):
    """With `ai_function_throw_on_error = 0`, a failed embedding makes the score NULL, and the row is
    counted as skipped (row-level, not per embedding)."""
    qid = unique_query_id("sim_error_graceful")
    result = instance.query(
        "SELECT aiSimilarity('a', 'b', 'test-embed-model', map('credentials', 'ai_embed_error'))",
        settings={
            "ai_function_throw_on_error": 0,
            "ai_function_max_retries": 0,
        },
        query_id=qid,
    )
    assert parse_nullable_float(result) is None
    events = get_profile_events(qid)
    assert int(events["rows_processed"]) == 0
    assert int(events["rows_skipped"]) == 1


def test_similarity_error_throw(started_cluster):
    """By default, a provider embedding error propagates."""
    error = instance.query_and_get_error(
        "SELECT aiSimilarity('a', 'b', 'test-embed-model', map('credentials', 'ai_embed_error'))",
    )
    assert "RECEIVED_ERROR_FROM_REMOTE_IO_SERVER" in error


def test_similarity_null_operand_skips_embedding(started_cluster):
    """A NULL operand forces the row to NULL locally, so the other operand is never embedded. Even with
    the default `ai_function_throw_on_error = 1` against a failing provider the query must not abort,
    because the would-be-failing embedding request is never issued for that row."""
    instance.query("TRUNCATE TABLE test_input_nullable")
    instance.query("INSERT INTO test_input_nullable VALUES (NULL)")
    qid = unique_query_id("sim_null_skip")
    result = instance.query(
        "SELECT aiSimilarity(x, 'server error', 'test-embed-model', map('credentials', 'ai_embed_error')) FROM test_input_nullable",
        query_id=qid,
    )
    assert parse_nullable_float(result) is None
    events = get_profile_events(qid)
    assert int(events["api_calls"]) == 0
    assert int(events["rows_processed"]) == 0
    assert int(events["rows_skipped"]) == 0


def test_similarity_empty_input_table(started_cluster):
    """Zero-row input makes no API calls."""
    instance.query("TRUNCATE TABLE test_input")
    qid = unique_query_id("sim_zero_rows")
    result = instance.query(
        "SELECT aiSimilarity(x, x, 'test-embed-model', map('credentials', 'ai_embed')) FROM test_input",
        query_id=qid,
    )
    assert result.strip() == ""
    events = get_profile_events(qid)
    assert int(events["api_calls"]) == 0
    assert int(events["rows_processed"]) == 0


def test_similarity_pairs_never_cross_rows(started_cluster):
    """Each row is scored against its own two operands, never against a neighbouring row's text.

    The embedding list is gapped with respect to rows: a row whose operand is NULL or empty
    contributes no entry at all. A positional mapping (`inputs[2 * row]`) would therefore shift
    every row after the first skipped one onto a neighbour's text, and the batch boundary would
    shift it again. The texts are chosen so that the mock's vectors are far apart: a correct pair
    scores exactly 1.0, while every possible cross-row pairing scores at most 0.95.
    """
    instance.query("TRUNCATE TABLE test_input_pairs")
    instance.query(
        "INSERT INTO test_input_pairs VALUES "
        "(1, 'aa', 'aa'), (2, NULL, 'z0'), (3, 'z0', '0z'), (4, '0zh', ''), (5, '0zh', '0zh')"
    )
    qid = unique_query_id("sim_pairs")
    result = instance.query(
        "SELECT aiSimilarity(a, b, 'test-embed-model', map('credentials', 'ai_embed')) "
        "FROM test_input_pairs ORDER BY id",
        # 3 rows embed 2 operands each; a batch of 3 splits row 3's operands across two requests.
        settings={"ai_function_embedding_max_batch_size": 3},
        query_id=qid,
    )
    scores = [parse_nullable_float(line) for line in result.strip().split("\n")]
    expected = [
        expected_similarity("aa", "aa"),
        None,
        expected_similarity("z0", "0z"),
        None,
        expected_similarity("0zh", "0zh"),
    ]
    assert len(scores) == 5
    for score, want in zip(scores, expected):
        if want is None:
            assert score is None
        else:
            assert score == pytest.approx(want, abs=1e-4)
    # Rows 1 and 5 pair a text with itself, so anything but an exact 1.0 means a mispairing.
    assert scores[0] == pytest.approx(1.0, abs=1e-6)
    assert scores[4] == pytest.approx(1.0, abs=1e-6)
    events = get_profile_events(qid)
    assert int(events["api_calls"]) == 2
    assert int(events["rows_processed"]) == 3


def test_similarity_const_nullable_operand(started_cluster):
    """`ColumnConst(ColumnNullable)` operands (a NULL or a value cast to Nullable(String) as a literal)
    are read via the same `isNullAt`/`getDataAt` path: a const NULL yields NULL, and const non-null
    values embed and score normally."""
    qid = unique_query_id("sim_const_null")
    null_result = instance.query(
        "SELECT aiSimilarity(CAST(NULL AS Nullable(String)), 'hi', 'test-embed-model', map('credentials', 'ai_embed'))",
        query_id=qid,
    )
    assert parse_nullable_float(null_result) is None
    # The const NULL operand short-circuits the row, so no embedding request is made.
    assert int(get_profile_events(qid)["api_calls"]) == 0
    value_result = instance.query(
        "SELECT aiSimilarity(CAST('cat' AS Nullable(String)), CAST('kitten' AS Nullable(String)), 'test-embed-model', map('credentials', 'ai_embed'))",
    )
    assert parse_nullable_float(value_result) == pytest.approx(expected_similarity("cat", "kitten"), abs=1e-4)


# ---------------------------------------------------------------------------
# How many API calls a query shape issues
#
# These assert `AIAPICalls`, not output: the count is a property of the planner and the
# row loop, and it is what an implementation that evaluated AI functions lazily would
# change. Exact integers, so they hold on any host given the pinned settings.
# ---------------------------------------------------------------------------

LAZY_ROWS = 64
LAZY_BLOCK = 16
LAZY_DISTINCT = 4
CHAT_CALL = "aiClassify(x, ['positive','negative','neutral'], map('credentials', 'ai_mock'))"
EMBED_CALL = "aiEmbed(x, 'test-embed-model', map('credentials', 'ai_embed'))"


@pytest.fixture(scope="module")
def call_count_tables(started_cluster):
    """One-part tables for the call-count scenarios, plus a duplicate-heavy one."""
    instance.query("DROP TABLE IF EXISTS lazy_rows SYNC")
    instance.query(
        "CREATE TABLE lazy_rows (id UInt32, x String) ENGINE = MergeTree ORDER BY id"
    )
    instance.query(
        f"INSERT INTO lazy_rows SELECT number, concat('row ', toString(number)) "
        f"FROM numbers({LAZY_ROWS})"
    )
    instance.query("OPTIMIZE TABLE lazy_rows FINAL")

    instance.query("DROP TABLE IF EXISTS lazy_dup SYNC")
    instance.query(
        "CREATE TABLE lazy_dup (id UInt32, x String) ENGINE = MergeTree ORDER BY id"
    )
    instance.query(
        f"INSERT INTO lazy_dup SELECT number, concat('dup ', toString(number % "
        f"{LAZY_DISTINCT})) FROM numbers({LAZY_ROWS})"
    )
    instance.query("OPTIMIZE TABLE lazy_dup FINAL")
    yield
    instance.query("DROP TABLE IF EXISTS lazy_rows SYNC")
    instance.query("DROP TABLE IF EXISTS lazy_dup SYNC")


def run_and_count_calls(sql, prefix, extra_settings=None):
    """Run `sql` and return the number of provider requests it issued."""
    settings = {"max_block_size": LAZY_BLOCK, "max_threads": 1}
    # `preferred_block_size_bytes` can split a block below `max_block_size` on its own.
    settings["preferred_block_size_bytes"] = 0
    if extra_settings:
        settings.update(extra_settings)
    qid = unique_query_id(prefix)
    instance.query(sql, settings=settings, query_id=qid)
    return int(get_profile_events(qid)["api_calls"])


# `expected` is what the implementation does today; `ideal` is what a maximally lazy
# implementation would do. They differ only for dedup, which does not exist: identical
# inputs are embedded once per row (`aiEmbed.cpp`, the live-row collection loop).
@pytest.mark.parametrize(
    "case, sql, expected, ideal, settings",
    [
        (
            "filter",
            f"SELECT {CHAT_CALL} FROM lazy_rows WHERE id % 8 = 0 FORMAT Null",
            LAZY_ROWS // 8,
            LAZY_ROWS // 8,
            {},
        ),
        (
            "limit",
            f"SELECT {CHAT_CALL} FROM lazy_rows LIMIT 5 FORMAT Null",
            5,
            5,
            {},
        ),
        (
            "order_by_limit",
            f"SELECT {CHAT_CALL} FROM lazy_rows ORDER BY id LIMIT 5 FORMAT Null",
            5,
            5,
            {},
        ),
        (
            "ai_predicate_last",
            f"SELECT count() FROM lazy_rows WHERE id % 8 = 0 AND {CHAT_CALL} = 'positive' "
            f"FORMAT Null",
            LAZY_ROWS // 8,
            LAZY_ROWS // 8,
            {},
        ),
        (
            "short_circuit_if",
            f"SELECT if(id % 8 = 0, {CHAT_CALL}, '') FROM lazy_rows FORMAT Null",
            LAZY_ROWS // 8,
            LAZY_ROWS // 8,
            {"short_circuit_function_evaluation": "force_enable"},
        ),
        (
            "prewhere",
            f"SELECT {CHAT_CALL} FROM lazy_rows PREWHERE id % 8 = 0 FORMAT Null",
            LAZY_ROWS // 8,
            LAZY_ROWS // 8,
            {},
        ),
        (
            # Batch size 1 makes one request per input, so the count can show dedup.
            # It does not: every row is embedded even though there are four distinct values.
            "no_dedup_of_identical_inputs",
            f"SELECT {EMBED_CALL} FROM lazy_dup FORMAT Null",
            LAZY_ROWS,
            LAZY_DISTINCT,
            {"ai_function_embedding_max_batch_size": 1},
        ),
        (
            # The control for the case above: deduplicating in SQL costs four requests.
            "distinct_subquery_control",
            f"SELECT {EMBED_CALL} FROM (SELECT DISTINCT x FROM lazy_dup) FORMAT Null",
            LAZY_DISTINCT,
            LAZY_DISTINCT,
            {"ai_function_embedding_max_batch_size": 1},
        ),
        (
            # Common subexpression elimination: `aiEmbed` is deterministic, so evaluating
            # it in both the filter and the projection must not double the requests.
            "cse_filter_and_projection",
            f"SELECT {EMBED_CALL} FROM lazy_rows WHERE length({EMBED_CALL}) > 0 FORMAT Null",
            LAZY_ROWS,
            LAZY_ROWS,
            {"ai_function_embedding_max_batch_size": 1},
        ),
    ],
)
def test_api_call_count_per_query_shape(call_count_tables, case, sql, expected, ideal, settings):
    calls = run_and_count_calls(sql, f"calls_{case}", settings)
    assert calls == expected, (
        f"{case}: {calls} API calls, expected {expected} (a maximally lazy implementation "
        f"would issue {ideal})"
    )


def _create_quota_parts(name, parts=8, rows_per_part=8, index_granularity=None):
    """Create a MergeTree table of `parts` unmerged parts (merges stopped) so a scan over it
    produces several blocks - the shape needed to tell a per-query quota from a per-block one.
    A single-part table cannot: one block is one allowance. `SYSTEM STOP MERGES` keeps a
    background merge from collapsing the parts before the scan and masking the difference.

    `index_granularity` pins a small, fixed granule size (adaptive granularity disabled) so the
    number of marks is deterministic - needed when a test relies on the read pool splitting the
    scan across threads, which is driven by mark count."""
    instance.query(f"DROP TABLE IF EXISTS {name} SYNC")
    create = f"CREATE TABLE {name} (id UInt32, x String) ENGINE = MergeTree ORDER BY id"
    if index_granularity is not None:
        create += f" SETTINGS index_granularity = {index_granularity}, index_granularity_bytes = 0"
    instance.query(create)
    instance.query(f"SYSTEM STOP MERGES {name}")
    for part in range(parts):
        base = part * rows_per_part
        instance.query(
            f"INSERT INTO {name} SELECT number + {base}, "
            f"concat('row ', toString(number + {base})) FROM numbers({rows_per_part})"
        )


# `max_block_size` = 8 with 8-row parts gives one block per part, so a per-block tracker
# reaches at most 8 (< the caps below) and never fires, while a per-query tracker accumulates
# across all 64 rows.
_QUOTA_SCOPE_SETTINGS = {
    "max_block_size": 8,
    "max_threads": 1,
    "preferred_block_size_bytes": 0,
}


def test_api_call_quota_is_per_query(started_cluster):
    """`ai_function_max_api_calls_per_query` must bound the query, not each block of it.

    The tracker is shared per query (owned by the query `Context`), so every block and every
    pipeline stream draws on one allowance. It used to be a stack local in `executeImpl` with
    no shared state, so each block started with a fresh allowance and the effective ceiling
    grew with the data.
    """
    limit = 10
    _create_quota_parts("quota_parts")
    try:
        qid = unique_query_id("quota_scope")
        instance.query(
            f"SELECT {CHAT_CALL} FROM quota_parts FORMAT Null",
            settings={
                **_QUOTA_SCOPE_SETTINGS,
                "ai_function_max_api_calls_per_query": limit,
                "ai_function_throw_on_quota_exceeded": 0,
            },
            query_id=qid,
        )
        calls = int(get_profile_events(qid)["api_calls"])
    finally:
        instance.query("DROP TABLE IF EXISTS quota_parts SYNC")

    assert calls <= limit, (
        f"{calls} API calls with ai_function_max_api_calls_per_query = {limit}: the quota "
        "is tracked per executeImpl call, so the query spent a multiple of its own cap"
    )


def test_api_call_quota_throws_per_query(started_cluster):
    """With `ai_function_throw_on_quota_exceeded = 1` (the default) the query must raise once
    the per-query call quota is reached. No single 8-row block reaches the cap of 10, so a
    per-block tracker never throws and the query completes; the per-query tracker throws."""
    _create_quota_parts("quota_throw")
    try:
        error = instance.query_and_get_error(
            f"SELECT {CHAT_CALL} FROM quota_throw FORMAT Null",
            settings={
                **_QUOTA_SCOPE_SETTINGS,
                "ai_function_max_api_calls_per_query": 10,
                "ai_function_throw_on_quota_exceeded": 1,
            },
        )
    finally:
        instance.query("DROP TABLE IF EXISTS quota_throw SYNC")

    assert "AI API call limit reached" in error, error


def test_input_token_quota_is_per_query(started_cluster):
    """`ai_function_max_input_tokens_per_query` must bound the query too. The mock reports
    `prompt_tokens = 10` per chat call, so a per-block tracker tops out at 80 tokens per 8-row
    block (< the 100-token cap) and never fires, while the per-query tracker stops the scan."""
    limit = 100
    _create_quota_parts("quota_tokens")
    try:
        qid = unique_query_id("quota_tokens")
        instance.query(
            f"SELECT {CHAT_CALL} FROM quota_tokens FORMAT Null",
            settings={
                **_QUOTA_SCOPE_SETTINGS,
                "ai_function_max_input_tokens_per_query": limit,
                "ai_function_throw_on_quota_exceeded": 0,
            },
            query_id=qid,
        )
        input_tokens = int(get_profile_events(qid)["input_tokens"])
    finally:
        instance.query("DROP TABLE IF EXISTS quota_tokens SYNC")

    assert input_tokens <= limit, (
        f"{input_tokens} input tokens with ai_function_max_input_tokens_per_query = {limit}: "
        "the quota is tracked per executeImpl call, so the query spent a multiple of its cap"
    )


def test_api_call_quota_holds_under_concurrency(started_cluster):
    """The API-call cap must hold when several pipeline threads reserve slots against the shared
    tracker at once. The slot is claimed with an atomic bounded increment (`tryReserveApiCall`), so
    two threads cannot both pass a stale check and overshoot.

    The table has enough marks (small pinned granularity over 16 parts) that the read pool hands the
    scan - and thus the AI function - to several threads under `max_threads = 8`. `peak_threads_usage`
    from `system.query_log` is the count of threads that ran simultaneously; asserting it is > 1
    means a green result proves the concurrent reservation path was exercised, not that the scan
    happened to collapse to one stream. The query wants 2048 calls but must make at most `limit`."""
    limit = 10
    _create_quota_parts("quota_concurrent", parts=16, rows_per_part=128, index_granularity=8)
    try:
        qid = unique_query_id("quota_concurrent")
        instance.query(
            f"SELECT {CHAT_CALL} FROM quota_concurrent FORMAT Null",
            settings={
                "max_block_size": 8,
                "max_threads": 8,
                "ai_function_max_api_calls_per_query": limit,
                "ai_function_throw_on_quota_exceeded": 0,
            },
            query_id=qid,
        )
        events = get_profile_events(qid)
        calls = int(events["api_calls"])
        peak_threads = int(events["peak_threads"])
    finally:
        instance.query("DROP TABLE IF EXISTS quota_concurrent SYNC")

    assert peak_threads > 1, (
        f"query peaked at {peak_threads} simultaneous thread(s); the concurrent reservation path was "
        "not exercised, so this test would not catch a check-then-act overshoot"
    )
    assert calls <= limit, (
        f"{calls} API calls with ai_function_max_api_calls_per_query = {limit} and {peak_threads} peak "
        "threads: concurrent streams overshot the per-query cap"
    )


def test_api_call_quota_ignores_subquery_settings(started_cluster):
    """`ai_function_max_*_per_query` is read from the top-level query context, so a nested
    subquery's own `SETTINGS` override of it does not apply: the whole query shares one budget
    seeded from the outer settings. A subquery runs in a copied child context carrying its own
    settings, but the quota tracker lives on the query context, so those overrides are ignored."""
    _create_quota_parts("quota_levels")  # 8 parts x 8 rows = 64 rows, one API call per row
    try:
        # The subquery caps at 5, the outer query at 20. A result of 20 shows the outer
        # (query-context) value governs; the subquery override (which would give 5, as would a
        # min-of-both rule) is ignored.
        qid = unique_query_id("quota_levels_outer_wins")
        instance.query(
            f"SELECT c FROM (SELECT {CHAT_CALL} AS c FROM quota_levels "
            "SETTINGS ai_function_max_api_calls_per_query = 5) FORMAT Null",
            settings={
                **_QUOTA_SCOPE_SETTINGS,
                "ai_function_max_api_calls_per_query": 20,
                "ai_function_throw_on_quota_exceeded": 0,
            },
            query_id=qid,
        )
        outer_wins = int(get_profile_events(qid)["api_calls"])

        # The quota is set only in the subquery; the outer query leaves it at the default (far
        # above 64). The subquery cap is ignored, so all 64 rows run rather than stopping at 5 -
        # a quota set only in a subquery has no effect.
        qid = unique_query_id("quota_levels_subquery_only")
        instance.query(
            f"SELECT c FROM (SELECT {CHAT_CALL} AS c FROM quota_levels "
            "SETTINGS ai_function_max_api_calls_per_query = 5) FORMAT Null",
            settings={
                **_QUOTA_SCOPE_SETTINGS,
                "ai_function_throw_on_quota_exceeded": 0,
            },
            query_id=qid,
        )
        subquery_only = int(get_profile_events(qid)["api_calls"])
    finally:
        instance.query("DROP TABLE IF EXISTS quota_levels SYNC")

    assert outer_wins == 20, (
        f"expected the top-level cap (20) to govern, got {outer_wins}: a subquery-scoped SETTINGS "
        "override of ai_function_max_api_calls_per_query must not change the query budget"
    )
    assert subquery_only == 64, (
        f"expected all 64 rows to run (a quota set only in the subquery is ignored), got {subquery_only}"
    )
