"""
Integration tests for AI function execution paths.

Tests the row-processing loop against a mock OpenAI-compatible HTTP server
for aiGenerate, aiClassify, aiExtract, and aiTranslate.
"""

import json
import os
import typing
import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import wait_condition

MOCK_PORT = 18123
AI_SETTINGS = {"allow_experimental_ai_functions": 1}
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


def get_profile_events(query_id):
    instance.query("SYSTEM FLUSH LOGS")
    result = instance.query(
        f"""
        SELECT
            ProfileEvents['AIAPICalls'] AS api_calls,
            ProfileEvents['AIInputTokens'] AS input_tokens,
            ProfileEvents['AIOutputTokens'] AS output_tokens,
            ProfileEvents['AIRowsProcessed'] AS rows_processed,
            ProfileEvents['AIRowsSkipped'] AS rows_skipped
        FROM system.query_log
        WHERE query_id = '{query_id}' AND type = 'QueryFinish'
        LIMIT 1
        FORMAT JSONEachRow
        """
    ).strip()
    assert result, f"no system.query_log row found for query_id={query_id}"
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
        # Endpoint returning a deterministic HTTP 400, which the url table function never retries.
        instance.query(
            f"CREATE NAMED COLLECTION ai_bad_request AS "
            f"provider = 'openai', "
            f"endpoint = 'http://localhost:{MOCK_PORT}/v1/bad_request', "
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
            f"model = 'test-embed-model', "
            f"api_key = 'test-key'"
        )
        instance.query(
            f"CREATE NAMED COLLECTION ai_embed_error AS "
            f"provider = 'openai', "
            f"endpoint = 'http://localhost:{MOCK_PORT}/v1/embeddings_error', "
            f"model = 'test-embed-model', "
            f"api_key = 'test-key'"
        )
        instance.query(
            f"CREATE NAMED COLLECTION ai_embed_dup_index AS "
            f"provider = 'openai', "
            f"endpoint = 'http://localhost:{MOCK_PORT}/v1/embeddings_dup_index', "
            f"model = 'test-embed-model', "
            f"api_key = 'test-key'"
        )
        instance.query(
            f"CREATE NAMED COLLECTION ai_embed_wrong_count AS "
            f"provider = 'openai', "
            f"endpoint = 'http://localhost:{MOCK_PORT}/v1/embeddings_wrong_count', "
            f"model = 'test-embed-model', "
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
            f"model = 'test-embed-model', "
            f"api_key = 'test-key'"
        )

        instance.query("CREATE TABLE test_input (x String) ENGINE = Memory")
        instance.query(
            "CREATE TABLE test_input_nullable (x Nullable(String)) ENGINE = Memory"
        )

        yield cluster
    finally:
        cluster.shutdown()


# ---------------------------------------------------------------------------
# aiGenerate
# ---------------------------------------------------------------------------


def test_generate_content_basic(started_cluster):
    result = instance.query(
        "SELECT aiGenerate('ai_mock', 'hello world')",
        settings=AI_SETTINGS,
    )
    assert result.strip() == "hello world"


def test_generate_content_multiple_rows(started_cluster):
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('row1'), ('row2'), ('row3')")
    result = instance.query(
        "SELECT aiGenerate('ai_mock', x) FROM test_input ORDER BY x",
        settings=AI_SETTINGS,
    )
    assert result.strip().split("\n") == ["row1", "row2", "row3"]


def test_generate_content_profile_events(started_cluster):
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('a'), ('b'), ('c')")
    qid = unique_query_id("gen_content_events")
    instance.query(
        "SELECT aiGenerate('ai_mock', x) FROM test_input",
        settings=AI_SETTINGS,
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
        "SELECT aiGenerate('ai_mock', x) FROM test_input_nullable",
        settings=AI_SETTINGS,
    )
    lines = result.strip().split("\n")
    assert lines.count("\\N") == 2
    assert lines.count("hello") == 1


def test_generate_content_error_throw(started_cluster):
    error = instance.query_and_get_error(
        "SELECT aiGenerate('ai_error', 'hello')",
        settings=AI_SETTINGS,
    )
    assert "RECEIVED_ERROR_FROM_REMOTE_IO_SERVER" in error


def test_generate_content_error_graceful(started_cluster):
    result = instance.query(
        "SELECT aiGenerate('ai_error', 'hello')",
        settings={**AI_SETTINGS, "ai_function_throw_on_error": 0},
    )
    assert result.strip() == ""


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
        "SELECT aiGenerate('ai_no_key', 'no key here')",
        settings=AI_SETTINGS,
    )
    assert result.strip() == "no key here"
    assert "authorization" not in last_request()["headers"]


def test_generate_with_api_key_sends_auth_header(started_cluster):
    """A keyed collection forwards the key as a `Bearer` `Authorization` header."""
    result = instance.query(
        "SELECT aiGenerate('ai_mock', 'with key')",
        settings=AI_SETTINGS,
    )
    assert result.strip() == "with key"
    assert last_request()["headers"].get("authorization") == "Bearer test-key"


# ---------------------------------------------------------------------------
# aiClassify
# ---------------------------------------------------------------------------


def test_classify_basic(started_cluster):
    """aiClassify sends a response_format with enum constraint.
    The mock returns the first enum value."""
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('I love this product!')")
    result = instance.query(
        "SELECT aiClassify('ai_mock', x, ['positive', 'negative', 'neutral']) FROM test_input",
        settings=AI_SETTINGS,
    )
    # Mock returns first enum value; postProcessResponse extracts "category" from JSON
    assert result.strip() == "positive"


def test_classify_multiple_rows(started_cluster):
    instance.query("TRUNCATE TABLE test_input")
    instance.query(
        "INSERT INTO test_input VALUES ('great'), ('terrible'), ('okay')"
    )
    result = instance.query(
        "SELECT aiClassify('ai_mock', x, ['positive', 'negative', 'neutral']) FROM test_input",
        settings=AI_SETTINGS,
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
        "SELECT aiClassify('ai_mock', x, ['cat_a', 'cat_b']) FROM test_input",
        settings=AI_SETTINGS,
        query_id=qid,
    )
    events = get_profile_events(qid)
    assert int(events["api_calls"]) == 2
    assert int(events["rows_processed"]) == 2


def test_classify_null_input(started_cluster):
    instance.query("TRUNCATE TABLE test_input_nullable")
    instance.query("INSERT INTO test_input_nullable VALUES (NULL), ('text')")
    result = instance.query(
        "SELECT aiClassify('ai_mock', x, ['a', 'b']) FROM test_input_nullable",
        settings=AI_SETTINGS,
    )
    lines = result.strip().split("\n")
    assert len(lines) == 2
    assert "\\N" in lines
    assert "a" in lines


# ---------------------------------------------------------------------------
# aiExtract — simple instruction mode
# ---------------------------------------------------------------------------


def test_extract_simple_instruction(started_cluster):
    """With a plain text instruction, aiExtract uses a response_format with a
    single 'result' field. postProcessResponse extracts the value."""
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('The price is $42.99')")
    result = instance.query(
        "SELECT aiExtract('ai_mock', x, 'the price') FROM test_input",
        settings=AI_SETTINGS,
    )
    # Mock returns {"result": "<user_message>"}, postProcess extracts the value
    assert result.strip() == "The price is $42.99"


def test_extract_json_schema(started_cluster):
    """With a JSON schema instruction, aiExtract builds a multi-field response_format.
    The mock populates each field with the user message."""
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('John is 30 years old')")
    result = instance.query(
        """SELECT aiExtract('ai_mock', x, '{"name": "person name", "age": "person age"}') FROM test_input""",
        settings=AI_SETTINGS,
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
        "SELECT aiExtract('ai_mock', x, 'main topic') FROM test_input",
        settings=AI_SETTINGS,
        query_id=qid,
    )
    events = get_profile_events(qid)
    assert int(events["api_calls"]) == 3
    assert int(events["rows_processed"]) == 3


def test_extract_null_input(started_cluster):
    instance.query("TRUNCATE TABLE test_input_nullable")
    instance.query("INSERT INTO test_input_nullable VALUES (NULL), ('some text')")
    result = instance.query(
        "SELECT aiExtract('ai_mock', x, 'key info') FROM test_input_nullable",
        settings=AI_SETTINGS,
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
        "SELECT aiTranslate('ai_mock', x, 'French') FROM test_input",
        settings=AI_SETTINGS,
    )
    assert result.strip() == "Hello world"


def test_translate_multiple_rows(started_cluster):
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('one'), ('two'), ('three')")
    result = instance.query(
        "SELECT aiTranslate('ai_mock', x, 'Spanish') FROM test_input ORDER BY x",
        settings=AI_SETTINGS,
    )
    assert result.strip().split("\n") == ["one", "three", "two"]


def test_translate_with_instructions(started_cluster):
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('Hello')")
    result = instance.query(
        "SELECT aiTranslate('ai_mock', x, 'German', 'Use formal tone') FROM test_input",
        settings=AI_SETTINGS,
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
        "SELECT aiTranslate('ai_mock', x, 'Japanese') FROM test_input",
        settings=AI_SETTINGS,
        query_id=qid,
    )
    events = get_profile_events(qid)
    assert int(events["api_calls"]) == 2
    assert int(events["rows_processed"]) == 2


def test_translate_null_input(started_cluster):
    instance.query("TRUNCATE TABLE test_input_nullable")
    instance.query("INSERT INTO test_input_nullable VALUES (NULL), ('hello')")
    result = instance.query(
        "SELECT aiTranslate('ai_mock', x, 'French') FROM test_input_nullable",
        settings=AI_SETTINGS,
    )
    lines = result.strip().split("\n")
    assert "\\N" in lines
    assert "hello" in lines


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
        "SELECT aiEmbed('ai_embed', 'hello')",
        settings=AI_SETTINGS,
    )
    vec = parse_embedding(result)
    assert len(vec) == 4  # DEFAULT_EMBED_DIM in mock server
    assert any(v != 0.0 for v in vec)


def test_embed_multiple_rows(started_cluster):
    """Multiple rows go through one batched request; each row gets its own vector."""
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('alpha'), ('beta'), ('gamma')")
    result = instance.query(
        "SELECT aiEmbed('ai_embed', x) FROM test_input ORDER BY x",
        settings=AI_SETTINGS,
    )
    rows = [parse_embedding(line) for line in result.strip().split("\n")]
    assert len(rows) == 3
    assert all(len(v) == 4 for v in rows)
    # Different inputs should yield different vectors (mock uses input bytes).
    assert len({tuple(v) for v in rows}) == 3


def test_embed_with_dimensions(started_cluster):
    """The `dimensions` argument is forwarded to the provider and honored in the response."""
    result = instance.query(
        "SELECT aiEmbed('ai_embed', 'hello world', 16)",
        settings=AI_SETTINGS,
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
        "SELECT aiEmbed('ai_embed', x) FROM test_input_nullable ORDER BY x NULLS FIRST",
        settings=AI_SETTINGS,
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
        "SELECT aiEmbed('ai_embed', x) FROM test_input",
        settings=AI_SETTINGS,
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
        "SELECT aiEmbed('ai_embed', x) FROM test_input",
        settings={**AI_SETTINGS, "ai_function_embedding_max_batch_size": 2},
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
        "SELECT aiEmbed('ai_embed_error', 'hello')",
        settings=AI_SETTINGS,
    )
    assert "RECEIVED_ERROR_FROM_REMOTE_IO_SERVER" in error


def test_embed_error_graceful(started_cluster):
    """With `ai_function_throw_on_error = 0` the failed batch's rows become `[]`."""
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('a'), ('b')")
    result = instance.query(
        "SELECT aiEmbed('ai_embed_error', x) FROM test_input",
        settings={
            **AI_SETTINGS,
            "ai_function_throw_on_error": 0,
            "ai_function_max_retries": 0,
        },
    )
    rows = [parse_embedding(line) for line in result.strip().split("\n")]
    assert rows == [[], []]


def test_embed_duplicate_index_rejected(started_cluster):
    """`OpenAIProvider::embed` rejects responses with duplicate `index` values."""
    error = instance.query_and_get_error(
        "SELECT aiEmbed('ai_embed_dup_index', x) FROM (SELECT arrayJoin(['a', 'b']) AS x)",
        settings={**AI_SETTINGS, "ai_function_max_retries": 0},
    )
    assert "MALFORMED_AI_PROVIDER_RESPONSE" in error
    assert "duplicates" in error or "duplicate" in error.lower()


def test_embed_wrong_count_rejected(started_cluster):
    """`OpenAIProvider::embed` rejects responses whose `data` size != number of inputs."""
    error = instance.query_and_get_error(
        "SELECT aiEmbed('ai_embed_wrong_count', x) FROM (SELECT arrayJoin(['a', 'b']) AS x)",
        settings={**AI_SETTINGS, "ai_function_max_retries": 0},
    )
    assert "MALFORMED_AI_PROVIDER_RESPONSE" in error


def test_embed_empty_input_table(started_cluster):
    """Zero-row input must not make any API calls."""
    instance.query("TRUNCATE TABLE test_input")
    qid = unique_query_id("embed_zero_rows")
    result = instance.query(
        "SELECT aiEmbed('ai_embed', x) FROM test_input",
        settings=AI_SETTINGS,
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
        "SELECT aiEmbed('ai_embed', x) FROM test_input",
        settings={
            **AI_SETTINGS,
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
        "SELECT aiGenerate('ai_flaky', 'recover me')",
        settings={
            **AI_SETTINGS,
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
            "SELECT aiGenerate('ai_flaky', 'no retry')",
            settings={
                **AI_SETTINGS,
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
        "SELECT aiEmbed('ai_embed_flaky', 'hello')",
        settings={
            **AI_SETTINGS,
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
        "SELECT aiGenerate('ai_bad_request', 'bad request')",
        settings={
            **AI_SETTINGS,
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
        "SELECT aiGenerate('ai_bad_request', 'bad request')",
        settings={
            **AI_SETTINGS,
            "ai_function_max_retries": 5,
        },
    )
    assert "RECEIVED_ERROR_FROM_REMOTE_IO_SERVER" in error


def test_generate_server_error_is_retried(started_cluster):
    """Counterpart to the 400 case: an HTTP 500 is a transient/server-side error, so it IS retried
    (1 initial attempt + `ai_function_max_retries` retries), matching the url table function."""
    qid = unique_query_id("gen_500_retried")
    result = instance.query(
        "SELECT aiGenerate('ai_error', 'server error')",
        settings={
            **AI_SETTINGS,
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
        "SELECT aiGenerate('ai_error', 'server error')",
        settings={
            **AI_SETTINGS,
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


def test_embed_retry_respects_api_call_quota(started_cluster):
    """The embedding path enforces the same per-attempt API-call quota: a retriable HTTP 500 is not
    retried past `ai_function_max_api_calls_per_query`."""
    qid = unique_query_id("embed_quota_caps_retries")
    result = instance.query(
        "SELECT aiEmbed('ai_embed_error', 'server error')",
        settings={
            **AI_SETTINGS,
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
