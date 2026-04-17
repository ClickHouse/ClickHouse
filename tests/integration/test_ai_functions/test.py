"""
Integration tests for AI function execution paths.

Tests the row-processing loop against a mock OpenAI-compatible HTTP server
for aiGenerateContent, aiClassify, aiExtract, and aiTranslate.
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

        instance.query("CREATE TABLE test_input (x String) ENGINE = Memory")
        instance.query(
            "CREATE TABLE test_input_nullable (x Nullable(String)) ENGINE = Memory"
        )

        yield cluster
    finally:
        cluster.shutdown()


# ---------------------------------------------------------------------------
# aiGenerateContent
# ---------------------------------------------------------------------------


def test_generate_content_basic(started_cluster):
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('hello world')")
    result = instance.query(
        "SELECT aiGenerateContent('ai_mock', 'hello world')",
        settings=AI_SETTINGS,
    )
    assert result.strip() == "hello world"


def test_generate_content_multiple_rows(started_cluster):
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('row1'), ('row2'), ('row3')")
    result = instance.query(
        "SELECT aiGenerateContent('ai_mock', x) FROM test_input ORDER BY x",
        settings=AI_SETTINGS,
    )
    assert result.strip().split("\n") == ["row1", "row2", "row3"]


def test_generate_content_profile_events(started_cluster):
    instance.query("TRUNCATE TABLE test_input")
    instance.query("INSERT INTO test_input VALUES ('a'), ('b'), ('c')")
    qid = unique_query_id("gen_content_events")
    instance.query(
        "SELECT aiGenerateContent('ai_mock', x) FROM test_input",
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
        "SELECT aiGenerateContent('ai_mock', x) FROM test_input_nullable",
        settings=AI_SETTINGS,
    )
    lines = result.strip().split("\n")
    assert lines.count("\\N") == 2
    assert lines.count("hello") == 1


def test_generate_content_error_throw(started_cluster):
    error = instance.query_and_get_error(
        "SELECT aiGenerateContent('ai_error', 'hello')",
        settings=AI_SETTINGS,
    )
    assert "RECEIVED_ERROR_FROM_REMOTE_IO_SERVER" in error


def test_generate_content_error_graceful(started_cluster):
    result = instance.query(
        "SELECT aiGenerateContent('ai_error', 'hello')",
        settings={**AI_SETTINGS, "ai_function_throw_on_error": 0},
    )
    assert result.strip() == ""


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
