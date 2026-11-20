#!/usr/bin/env python3
import json
import logging
import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from helpers.cluster import ClickHouseCluster

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

LOCALSTACK_URL = "http://localstack:4566"
ACCOUNT_ID = "000000000000"

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/sqs.xml"],
    with_localstack=True,
)


def queue_url(queue_name):
    return f"{LOCALSTACK_URL}/{ACCOUNT_ID}/{queue_name}"


def create_queue(queue_name, visibility_timeout=None):
    params = f"Action=CreateQueue&QueueName={queue_name}"
    if visibility_timeout is not None:
        params += f"&VisibilityTimeout={visibility_timeout}"
    response = instance.exec_in_container(
        [
            "bash",
            "-c",
            f"curl -s -X POST '{LOCALSTACK_URL}/?' -H 'Content-Type: application/x-www-form-urlencoded' --data-urlencode 'Action=CreateQueue' --data-urlencode 'QueueName={queue_name}'"
            + (
                f" --data-urlencode 'VisibilityTimeout={visibility_timeout}'"
                if visibility_timeout is not None
                else ""
            ),
        ],
        user="root",
    )
    logger.info(f"Created queue {queue_name}: {response}")


def send_message(queue_name, message):
    response = instance.exec_in_container(
        [
            "bash",
            "-c",
            f"curl -s -X POST '{LOCALSTACK_URL}/{ACCOUNT_ID}/{queue_name}' "
            f"-H 'Content-Type: application/x-www-form-urlencoded' "
            f"--data-urlencode 'Action=SendMessage' "
            f"--data-urlencode 'MessageBody={message}'",
        ],
        user="root",
    )
    logger.info(f"Sent message to {queue_name}: {response}")


def wait_for_localstack():
    for i in range(30):
        try:
            result = instance.exec_in_container(
                ["bash", "-c", f"curl -s {LOCALSTACK_URL}/health"],
                nothrow=True,
                user="root",
            )
            if "services" in result:
                logger.info(f"LocalStack is ready: {result}")
                return
        except Exception as e:
            logger.warning(f"LocalStack not ready yet (attempt {i + 1}): {e}")
        time.sleep(1)
    raise Exception("LocalStack did not start in time")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        wait_for_localstack()
        instance.query("CREATE DATABASE IF NOT EXISTS test")
        yield cluster
    finally:
        cluster.shutdown()


def sqs_check_result(result, reference):
    result_lines = sorted([line for line in result.strip().split("\n") if line])
    reference_lines = sorted([line for line in reference.strip().split("\n") if line])
    logger.info(f"result_lines: {result_lines}")
    logger.info(f"reference_lines: {reference_lines}")
    return result_lines == reference_lines


def test_sqs_basic(started_cluster):
    """Test basic INSERT/SELECT round-trip through SQS."""
    queue_name = "basic_test"
    create_queue(queue_name)

    instance.query("DROP TABLE IF EXISTS test.sqs")
    instance.query("DROP TABLE IF EXISTS test.sqs_data")

    instance.query(
        f"""
        CREATE TABLE test.sqs (
            key UInt64,
            value String
        ) ENGINE = SQS
        SETTINGS
            sqs_queue_url = '{queue_url(queue_name)}',
            sqs_format = 'JSONEachRow',
            sqs_max_messages_per_receive = 10,
            sqs_auto_delete = 1,
            sqs_num_consumers = 1
        """
    )

    instance.query(
        "INSERT INTO test.sqs VALUES (1, 'test_1'), (2, 'test_2'), (3, 'test_3')"
    )

    instance.query(
        """
        CREATE TABLE test.sqs_data (
            key UInt64,
            value String
        ) ENGINE = Memory
        """
    )

    time.sleep(1)

    instance.query("INSERT INTO test.sqs_data SELECT * FROM test.sqs")

    expected = "1\ttest_1\n2\ttest_2\n3\ttest_3\n"
    result = instance.query("SELECT * FROM test.sqs_data ORDER BY key")
    assert sqs_check_result(result, expected), f"Expected:\n{expected}\nGot:\n{result}"

    instance.query("DROP TABLE IF EXISTS test.sqs")
    instance.query("DROP TABLE IF EXISTS test.sqs_data")


def test_sqs_materialized_view(started_cluster):
    """Test that a materialized view backed by SQS processes messages automatically."""
    queue_name = "mv_test"
    create_queue(queue_name)

    instance.query("DROP TABLE IF EXISTS test.sqs_mv")
    instance.query("DROP TABLE IF EXISTS test.sqs")
    instance.query("DROP TABLE IF EXISTS test.sqs_data")

    instance.query(
        """
        CREATE TABLE test.sqs_data (
            key UInt64,
            value String
        ) ENGINE = MergeTree()
        ORDER BY key
        """
    )

    instance.query(
        f"""
        CREATE TABLE test.sqs (
            key UInt64,
            value String
        ) ENGINE = SQS
        SETTINGS
            sqs_queue_url = '{queue_url(queue_name)}',
            sqs_format = 'JSONEachRow',
            sqs_max_messages_per_receive = 10,
            sqs_auto_delete = 1
        """
    )

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.sqs_mv
        TO test.sqs_data
        AS SELECT * FROM test.sqs
        """
    )

    instance.query("INSERT INTO test.sqs VALUES (1, 'mv_1'), (2, 'mv_2'), (3, 'mv_3')")

    expected = "1\tmv_1\n2\tmv_2\n3\tmv_3\n"
    result = ""
    for _ in range(20):
        result = instance.query("SELECT * FROM test.sqs_data ORDER BY key")
        if sqs_check_result(result, expected):
            break
        time.sleep(1)

    assert sqs_check_result(result, expected), f"Expected:\n{expected}\nGot:\n{result}"

    instance.query("DROP TABLE IF EXISTS test.sqs_mv")
    instance.query("DROP TABLE IF EXISTS test.sqs")
    instance.query("DROP TABLE IF EXISTS test.sqs_data")


def test_sqs_external_messages(started_cluster):
    """Test consuming messages sent externally (via HTTP API) into a materialized view."""
    queue_name = "external_messages_test"
    create_queue(queue_name)

    instance.query("DROP TABLE IF EXISTS test.sqs_mv")
    instance.query("DROP TABLE IF EXISTS test.sqs_source")
    instance.query("DROP TABLE IF EXISTS test.sqs_target")

    for i in range(1, 6):
        send_message(
            queue_name, json.dumps({"id": i, "message": f"external_message_{i}"})
        )

    instance.query(
        """
        CREATE TABLE test.sqs_target (
            id UInt64,
            message String
        ) ENGINE = Memory
        """
    )

    instance.query(
        f"""
        CREATE TABLE test.sqs_source (
            id UInt64,
            message String
        ) ENGINE = SQS
        SETTINGS
            sqs_queue_url = '{queue_url(queue_name)}',
            sqs_format = 'JSONEachRow',
            sqs_auto_delete = 1
        """
    )

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.sqs_mv
        TO test.sqs_target
        AS SELECT id, message FROM test.sqs_source
        """
    )

    for attempt in range(20):
        result = instance.query("SELECT count() FROM test.sqs_target")
        count = int(result.strip())
        logger.info(f"Attempt {attempt + 1}: {count} records in target table")
        if count == 5:
            break
        time.sleep(1)

    result = instance.query("SELECT count() FROM test.sqs_target")
    assert int(result.strip()) == 5, f"Expected 5 messages, got {result.strip()}"

    result = instance.query("SELECT id, message FROM test.sqs_target ORDER BY id")
    for i in range(1, 6):
        assert f"external_message_{i}" in result, (
            f"Message 'external_message_{i}' not found in results"
        )

    instance.query("DROP TABLE IF EXISTS test.sqs_mv")
    instance.query("DROP TABLE IF EXISTS test.sqs_source")
    instance.query("DROP TABLE IF EXISTS test.sqs_target")


def test_sqs_different_formats(started_cluster):
    """Test that various ClickHouse formats work when reading from SQS."""

    class TestCase:
        def __init__(self, format, table_scheme, messages, expected):
            self.format = format
            self.queue_name = f"fmt_{format.lower()}_test"
            self.table_scheme = table_scheme
            self.messages = messages
            self.expected = expected

    test_cases = [
        TestCase(
            format="CSV",
            table_scheme="key UInt64,\nvalue String",
            messages=["1,csv_1", "2,csv_2", "3,csv_3"],
            expected="1\tcsv_1\n2\tcsv_2\n3\tcsv_3\n",
        ),
        TestCase(
            format="TSV",
            table_scheme="key UInt64,\nvalue String",
            messages=["1\ttsv_1", "2\ttsv_2", "3\ttsv_3"],
            expected="1\ttsv_1\n2\ttsv_2\n3\ttsv_3\n",
        ),
        TestCase(
            format="JSONEachRow",
            table_scheme="key UInt64,\nvalue String",
            messages=[
                '{"key": 1, "value": "json_1"}',
                '{"key": 2, "value": "json_2"}',
                '{"key": 3, "value": "json_3"}',
            ],
            expected="1\tjson_1\n2\tjson_2\n3\tjson_3\n",
        ),
        TestCase(
            format="JSONCompactEachRow",
            table_scheme="key UInt64,\nvalue String",
            messages=['[1, "compact_1"]', '[2, "compact_2"]', '[3, "compact_3"]'],
            expected="1\tcompact_1\n2\tcompact_2\n3\tcompact_3\n",
        ),
    ]

    for tc in test_cases:
        create_queue(tc.queue_name)

        instance.query("DROP TABLE IF EXISTS test.sqs")
        instance.query("DROP TABLE IF EXISTS test.sqs_data")

        instance.query(
            f"""
            CREATE TABLE test.sqs ({tc.table_scheme}) ENGINE = SQS
            SETTINGS
                sqs_queue_url = '{queue_url(tc.queue_name)}',
                sqs_format = '{tc.format}',
                sqs_max_messages_per_receive = 10,
                sqs_auto_delete = 1
            """
        )

        for msg in tc.messages:
            send_message(tc.queue_name, msg)

        instance.query(
            f"CREATE TABLE test.sqs_data ({tc.table_scheme}) ENGINE = Memory"
        )

        instance.query("INSERT INTO test.sqs_data SELECT * FROM test.sqs")

        result = instance.query("SELECT * FROM test.sqs_data ORDER BY key")
        assert sqs_check_result(result, tc.expected), (
            f"Format {tc.format}: expected:\n{tc.expected}\ngot:\n{result}"
        )

        instance.query("DROP TABLE IF EXISTS test.sqs")
        instance.query("DROP TABLE IF EXISTS test.sqs_data")


def test_sqs_virtual_columns(started_cluster):
    """Test that virtual columns (_message_id, _receive_count, _sent_timestamp) are populated."""
    queue_name = "virtual_columns_test"
    create_queue(queue_name)

    instance.query("DROP TABLE IF EXISTS test.sqs")
    instance.query("DROP TABLE IF EXISTS test.sqs_data")

    # Virtual columns must be declared in the table schema to select them
    instance.query(
        f"""
        CREATE TABLE test.sqs (
            key UInt64,
            value String
        ) ENGINE = SQS
        SETTINGS
            sqs_queue_url = '{queue_url(queue_name)}',
            sqs_format = 'JSONEachRow',
            sqs_auto_delete = 1
        """
    )

    instance.query(
        "INSERT INTO test.sqs (key, value) VALUES (1, 'test_vc_1'), (2, 'test_vc_2')"
    )

    instance.query(
        """
        CREATE TABLE test.sqs_data (
            key UInt64,
            value String,
            message_id String,
            receive_count UInt64,
            sent_timestamp UInt64
        ) ENGINE = Memory
        """
    )

    time.sleep(1)

    instance.query(
        """
        INSERT INTO test.sqs_data
        SELECT
            key,
            value,
            _message_id AS message_id,
            _receive_count AS receive_count,
            _sent_timestamp AS sent_timestamp
        FROM test.sqs
        """
    )

    result = instance.query(
        "SELECT count() FROM test.sqs_data WHERE message_id != '' AND receive_count > 0"
    )
    count = int(result.strip())
    assert count == 2, f"Expected 2 rows with non-empty virtual columns, got {count}"

    instance.query("DROP TABLE IF EXISTS test.sqs")
    instance.query("DROP TABLE IF EXISTS test.sqs_data")


def test_read_empty_queue(started_cluster):
    """Test that reading from an empty queue returns no rows."""
    queue_name = "empty_queue_test"
    create_queue(queue_name)

    instance.query("DROP TABLE IF EXISTS test.sqs")

    instance.query(
        f"""
        CREATE TABLE test.sqs (
            id UInt64,
            value String
        ) ENGINE = SQS
        SETTINGS
            sqs_queue_url = '{queue_url(queue_name)}',
            sqs_format = 'JSONEachRow',
            sqs_auto_delete = 1
        """
    )

    result = instance.query("SELECT * FROM test.sqs")
    assert result.strip() == "", f"Expected empty result, got: {result}"

    result = instance.query("SELECT count() FROM test.sqs")
    assert int(result.strip()) == 0, f"Expected count 0, got: {result}"

    instance.query("DROP TABLE IF EXISTS test.sqs")


def test_visibility_timeout(started_cluster):
    """Test that a message hidden by visibility timeout is not re-read until the timeout expires."""
    queue_name = "visibility_timeout_test"
    create_queue(queue_name, visibility_timeout=5)

    instance.query("DROP TABLE IF EXISTS test.sqs")

    instance.query(
        f"""
        CREATE TABLE test.sqs (
            id UInt64,
            value String
        ) ENGINE = SQS
        SETTINGS
            sqs_queue_url = '{queue_url(queue_name)}',
            sqs_format = 'JSONEachRow',
            sqs_auto_delete = 0,
            sqs_max_messages_per_receive = 1,
            sqs_visibility_timeout = 5,
            sqs_wait_time_seconds = 1
        """
    )

    send_message(queue_name, '{"id": 1, "value": "test"}')

    # First read — should see the message
    result = instance.query("SELECT * FROM test.sqs")
    assert "test" in result, f"Expected message in first read, got: {result}"

    # Second read — message is hidden by visibility timeout
    result2 = instance.query("SELECT * FROM test.sqs")
    assert result2.strip() == "", (
        f"Expected empty result during visibility timeout, got: {result2}"
    )

    # Wait for visibility timeout to expire
    time.sleep(6)

    # Third read — message should be visible again
    result3 = instance.query("SELECT * FROM test.sqs")
    assert "test" in result3, (
        f"Expected message to reappear after visibility timeout, got: {result3}"
    )

    instance.query("DROP TABLE IF EXISTS test.sqs")


def test_sqs_skip_broken_messages(started_cluster):
    """Test that broken messages are skipped when sqs_skip_broken_messages is set."""
    queue_name = "skip_broken_test"
    create_queue(queue_name)

    send_message(queue_name, '{"key": 1, "value": "valid1"}')
    send_message(queue_name, "this is not valid json")
    send_message(queue_name, '{"key": 2, "value": "valid2"}')

    instance.query("DROP TABLE IF EXISTS test.sqs")
    instance.query("DROP TABLE IF EXISTS test.sqs_data")

    instance.query(
        f"""
        CREATE TABLE test.sqs (
            key UInt64,
            value String
        ) ENGINE = SQS
        SETTINGS
            sqs_queue_url = '{queue_url(queue_name)}',
            sqs_format = 'JSONEachRow',
            sqs_skip_broken_messages = 1,
            sqs_auto_delete = 1
        """
    )

    instance.query(
        """
        CREATE TABLE test.sqs_data (
            key UInt64,
            value String
        ) ENGINE = Memory
        """
    )

    instance.query("INSERT INTO test.sqs_data SELECT * FROM test.sqs")

    result = instance.query("SELECT count() FROM test.sqs_data")
    count = int(result.strip())
    assert count == 2, f"Expected 2 valid messages, got {count}"

    instance.query("DROP TABLE IF EXISTS test.sqs")
    instance.query("DROP TABLE IF EXISTS test.sqs_data")
