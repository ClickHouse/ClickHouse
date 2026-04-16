"""
Integration tests for AI function execution path (FunctionBaseAI::executeImpl).

Tests the row-processing loop against a mock OpenAI-compatible HTTP server:
provider calls, retry/backoff, quota accounting, error handling, and ProfileEvents.
"""

import json
import os
import typing
import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import wait_condition

MOCK_PORT = 9123
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
            f"python3 /mock_ai_server.py > /var/log/clickhouse-server/mock_ai_server.log 2>&1",
        ],
        detach=True,
        user="root",
    )
    wait_condition(
        lambda: instance.exec_in_container(
            ["curl", "-s", f"http://localhost:{MOCK_PORT}/health"],
            nothrow=True,
        ),
        lambda r: r == "OK",
        max_attempts=20,
        delay=0.5,
    )


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
        ORDER BY event_time_microseconds DESC
        LIMIT 1
        FORMAT JSONEachRow
        """
    )
    return json.loads(result.strip())


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
        instance.query(
            f"CREATE NAMED COLLECTION ai_flaky AS "
            f"provider = 'openai', "
            f"endpoint = 'http://localhost:{MOCK_PORT}/v1/flaky', "
            f"model = 'test-model', "
            f"api_key = 'test-key'"
        )
        instance.query(
            f"CREATE NAMED COLLECTION ai_custom_tokens AS "
            f"provider = 'openai', "
            f"endpoint = 'http://localhost:{MOCK_PORT}/v1/custom_tokens?prompt_tokens=500&completion_tokens=300', "
            f"model = 'test-model', "
            f"api_key = 'test-key'"
        )

        instance.query(
            f"CREATE NAMED COLLECTION ai_anthropic_mock AS "
            f"provider = 'anthropic', "
            f"endpoint = 'http://localhost:{MOCK_PORT}/v1/messages', "
            f"model = 'test-model', "
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
# Basic execution path
# ---------------------------------------------------------------------------


def test_basic_single_row(started_cluster):
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('hello world')")
    result = instance.query(
        "SELECT aiGenerateContent('ai_mock', x) FROM test_input",
        settings=AI_SETTINGS,
    )
    assert result.strip() == "hello world"


def test_multiple_rows(started_cluster):
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('row1'), ('row2'), ('row3')")
    result = instance.query(
        "SELECT aiGenerateContent('ai_mock', x) FROM test_input ORDER BY x",
        settings=AI_SETTINGS,
    )
    assert result.strip().split("\n") == ["row1", "row2", "row3"]


# ---------------------------------------------------------------------------
# ProfileEvents
# ---------------------------------------------------------------------------


def test_profile_events(started_cluster):
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('a'), ('b'), ('c')")
    qid = unique_query_id("profile_events")
    instance.query(
        "SELECT aiGenerateContent('ai_mock', x) FROM test_input",
        settings=AI_SETTINGS,
        query_id=qid,
    )
    events = get_profile_events(qid)
    assert int(events["api_calls"]) == 3
    assert int(events["input_tokens"]) == 30  # 3 rows * 10
    assert int(events["output_tokens"]) == 15  # 3 rows * 5
    assert int(events["rows_processed"]) == 3
    assert int(events["rows_skipped"]) == 0


def test_null_input_produces_null_output(started_cluster):
    """useDefaultImplementationForNulls=true means the framework wraps the
    function: NULL input -> NULL output. The framework still calls executeImpl
    for all rows (passing default values for NULL positions), then masks
    results at NULL positions with NULL."""
    instance.query("TRUNCATE TABLE test_input_nullable")
    instance.query(
        "INSERT INTO test_input_nullable VALUES (NULL), ('hello'), (NULL)"
    )
    result = instance.query(
        "SELECT aiGenerateContent('ai_mock', x) FROM test_input_nullable",
        settings=AI_SETTINGS,
    )
    lines = result.strip().split("\n")
    assert lines.count("\\N") == 2
    assert lines.count("hello") == 1


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


def test_throw_on_error_true(started_cluster):
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('hello')")
    error = instance.query_and_get_error(
        "SELECT aiGenerateContent('ai_error', x) FROM test_input",
        settings=AI_SETTINGS,
    )
    assert "RECEIVED_ERROR_FROM_REMOTE_IO_SERVER" in error


def test_throw_on_error_false(started_cluster):
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('hello')")
    qid = unique_query_id("throw_on_error_false")
    result = instance.query(
        "SELECT aiGenerateContent('ai_error', x) FROM test_input",
        settings={**AI_SETTINGS, "ai_function_throw_on_error": 0},
        query_id=qid,
    )
    assert result.strip() == ""

    events = get_profile_events(qid)
    assert int(events["api_calls"]) == 0
    assert int(events["rows_processed"]) == 0
    assert int(events["rows_skipped"]) == 1


def test_error_multiple_rows_silent(started_cluster):
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('a'), ('b'), ('c')")
    qid = unique_query_id("error_multi_silent")
    result = instance.query(
        "SELECT aiGenerateContent('ai_error', x) FROM test_input",
        settings={**AI_SETTINGS, "ai_function_throw_on_error": 0},
        query_id=qid,
    )
    lines = result.strip().split("\n")
    assert all(l == "" for l in lines)

    events = get_profile_events(qid)
    assert int(events["api_calls"]) == 0
    assert int(events["rows_processed"]) == 0
    assert int(events["rows_skipped"]) == 3


# ---------------------------------------------------------------------------
# Retry with backoff
# ---------------------------------------------------------------------------


def test_retry_success(started_cluster):
    # Reset the flaky counter so it fails exactly 2 times then succeeds
    instance.exec_in_container(
        ["curl", "-s", f"http://localhost:{MOCK_PORT}/v1/flaky/reset"],
        nothrow=True,
    )
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('retry_test')")
    qid = unique_query_id("retry_success")
    result = instance.query(
        "SELECT aiGenerateContent('ai_flaky', x) FROM test_input",
        settings={
            **AI_SETTINGS,
            "ai_function_max_retries": 3,
            "ai_function_retry_initial_delay_ms": 10,
        },
        query_id=qid,
    )
    assert result.strip() == "retry_test"

    events = get_profile_events(qid)
    assert int(events["api_calls"]) == 1  # only successful call counted
    assert int(events["rows_processed"]) == 1


def test_retry_exhausted(started_cluster):
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('fail_test')")
    error = instance.query_and_get_error(
        "SELECT aiGenerateContent('ai_error', x) FROM test_input",
        settings={
            **AI_SETTINGS,
            "ai_function_max_retries": 2,
            "ai_function_retry_initial_delay_ms": 10,
        },
    )
    assert "RECEIVED_ERROR_FROM_REMOTE_IO_SERVER" in error


# ---------------------------------------------------------------------------
# Quota enforcement
# ---------------------------------------------------------------------------


def test_quota_api_calls_throw(started_cluster):
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('a'), ('b'), ('c')")
    error = instance.query_and_get_error(
        "SELECT aiGenerateContent('ai_mock', x) FROM test_input",
        settings={
            **AI_SETTINGS,
            "ai_function_max_api_calls_per_query": 2,
            "ai_function_throw_on_quota_exceeded": 1,
        },
    )
    assert "LIMIT_EXCEEDED" in error


def test_quota_api_calls_silent(started_cluster):
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('a'), ('b'), ('c')")
    qid = unique_query_id("quota_api_silent")
    instance.query(
        "SELECT aiGenerateContent('ai_mock', x) FROM test_input",
        settings={
            **AI_SETTINGS,
            "ai_function_max_api_calls_per_query": 2,
            "ai_function_throw_on_quota_exceeded": 0,
        },
        query_id=qid,
    )
    events = get_profile_events(qid)
    assert int(events["api_calls"]) == 2
    assert int(events["rows_processed"]) == 2
    assert int(events["rows_skipped"]) == 1


def test_quota_input_tokens_silent(started_cluster):
    """ai_custom_tokens returns 500 prompt_tokens per call.
    With max_input_tokens=600: row 1 passes (0 < 600), row 2 passes (500 < 600),
    row 3 blocked (1000 >= 600)."""
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('a'), ('b'), ('c')")
    qid = unique_query_id("quota_tokens")
    instance.query(
        "SELECT aiGenerateContent('ai_custom_tokens', x) FROM test_input",
        settings={
            **AI_SETTINGS,
            "ai_function_max_input_tokens_per_query": 600,
            "ai_function_throw_on_quota_exceeded": 0,
        },
        query_id=qid,
    )
    events = get_profile_events(qid)
    assert int(events["api_calls"]) == 2
    assert int(events["input_tokens"]) == 1000  # 2 * 500
    assert int(events["output_tokens"]) == 600  # 2 * 300
    assert int(events["rows_processed"]) == 2
    assert int(events["rows_skipped"]) == 1


# ---------------------------------------------------------------------------
# Anthropic provider
# ---------------------------------------------------------------------------


def test_anthropic_basic(started_cluster):
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('hello anthropic')")
    result = instance.query(
        "SELECT aiGenerateContent('ai_anthropic_mock', x) FROM test_input",
        settings=AI_SETTINGS,
    )
    assert result.strip() == "hello anthropic"


def test_anthropic_multiple_rows(started_cluster):
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('x'), ('y'), ('z')")
    result = instance.query(
        "SELECT aiGenerateContent('ai_anthropic_mock', x) FROM test_input ORDER BY x",
        settings=AI_SETTINGS,
    )
    assert result.strip().split("\n") == ["x", "y", "z"]


def test_anthropic_profile_events(started_cluster):
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('a'), ('b')")
    qid = unique_query_id("anthropic_events")
    instance.query(
        "SELECT aiGenerateContent('ai_anthropic_mock', x) FROM test_input",
        settings=AI_SETTINGS,
        query_id=qid,
    )
    events = get_profile_events(qid)
    assert int(events["api_calls"]) == 2
    assert int(events["input_tokens"]) == 20  # 2 * 10
    assert int(events["output_tokens"]) == 10  # 2 * 5
    assert int(events["rows_processed"]) == 2
    assert int(events["rows_skipped"]) == 0
