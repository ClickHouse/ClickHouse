import pytest
import datetime
import logging
import time
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)

instance = cluster.add_instance(
    "instance",
    with_rabbitmq=True,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_last_event_time(logger_name, message):
    instance.query("SYSTEM FLUSH LOGS")
    ts = instance.query(
        f"""SELECT
                event_time_microseconds
            FROM merge(system, '^text_log')
            WHERE
                logger_name  = '{logger_name}' AND
                message = '{ message}'
            ORDER BY event_time_microseconds DESC
            LIMIT 1"""
    ).strip()
    logging.info(
        f"logger_name='{logger_name}', message='{message}', last_event_time='{ts}'"
    )
    return datetime.datetime.fromisoformat(ts)


@pytest.mark.skip(
    reason=(
        "Flaky in CI: shutdown ordering on the RabbitMQ event loop is not "
        "reliably reproducible under integration-test load. See "
        "https://github.com/ClickHouse/ClickHouse/pull/104253 for the "
        "directive to disable, and https://github.com/ClickHouse/ClickHouse/pull/104273 "
        "for context."
    )
)
def test_shutdown_rabbitmq_with_materialized_view(started_cluster):
    """
    Test that restarting server during active RabbitMQ consumption
    doesn't cause "Table is shutting down" errors.
    """

    instance.query("DROP DATABASE IF EXISTS test SYNC")
    instance.query("CREATE DATABASE test ENGINE=Atomic")

    # Create destination table
    instance.query(
        """
        CREATE TABLE test.destination (
            key UInt64,
            value String,
            _timestamp DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY key
    """
    )

    # Create RabbitMQ queue table
    logging.info("Creating RabbitMQ table...")
    instance.query(
        """
        CREATE TABLE test.rabbitmq_queue (
            key UInt64,
            value String
        ) ENGINE = RabbitMQ
        SETTINGS
            rabbitmq_host_port = 'rabbitmq1:5672',
            rabbitmq_exchange_name = 'test_shutdown_exchange',
            rabbitmq_exchange_type = 'fanout',
            rabbitmq_format = 'JSONEachRow',
            rabbitmq_username = 'root',
            rabbitmq_password = 'clickhouse',
            rabbitmq_num_consumers = 3,
            rabbitmq_flush_interval_ms = 100,
            rabbitmq_max_block_size = 100
    """
    )

    # Wait for RabbitMQ connection
    time.sleep(10)

    # Create materialized view
    instance.query(
        """
        CREATE MATERIALIZED VIEW test.test_view TO test.destination AS
        SELECT key, value FROM test.rabbitmq_queue
    """
    )

    # Publish by inserting directly into RabbitMQ table
    logging.info("Publishing messages by inserting into RabbitMQ table")
    instance.query(
        """
        INSERT INTO test.rabbitmq_queue
        SELECT number AS key, toString(number) AS value
        FROM numbers(1000)
    """
    )

    # Wait for messages to be published and consumed back
    time.sleep(10)

    # Check messages
    count_before = int(instance.query("SELECT count() FROM test.destination").strip())
    logging.info(f"Messages in destination before restart: {count_before}")

    # Publish more messages
    for batch in range(5):
        instance.query(
            f"""
            INSERT INTO test.rabbitmq_queue
            SELECT number + {1000 + batch * 100} AS key,
                   concat('batch_', toString({batch}), '_', toString(number)) AS value
            FROM numbers(100)
        """
        )
        time.sleep(0.5)

    time.sleep(3)

    instance.restart_clickhouse()

    registry_shutdown_queue_time = get_last_event_time(
        "StreamingStorageRegistry", "Will shutdown 1 queue storages"
    )

    rabbit_shutdown_time = get_last_event_time(
        "StorageRabbitMQ (test.rabbitmq_queue)", "Shutdown finished"
    )

    registry_already_shutdown_queue_time = get_last_event_time(
        "StreamingStorageRegistry", "Already shutdown 1 queue storages"
    )
    assert registry_shutdown_queue_time < rabbit_shutdown_time
    assert rabbit_shutdown_time < registry_already_shutdown_queue_time


@pytest.mark.skip(reason="There is data race in Rabbit MQ storage when shutting down")
def test_attach_detach_rabbitmq_with_materialized_view(started_cluster):
    """
    Test that restarting server during active RabbitMQ consumption
    doesn't cause "Table is shutting down" errors.
    """

    instance.query("DROP DATABASE IF EXISTS test SYNC")
    instance.query("CREATE DATABASE test ENGINE=Atomic")

    # Create RabbitMQ queue table
    logging.info("Creating RabbitMQ table...")
    instance.query(
        """
        CREATE TABLE test.rabbitmq_queue (
            key UInt64,
            value String
        ) ENGINE = RabbitMQ
        SETTINGS
            rabbitmq_host_port = 'rabbitmq1:5672',
            rabbitmq_exchange_name = 'test_shutdown_exchange',
            rabbitmq_exchange_type = 'fanout',
            rabbitmq_format = 'JSONEachRow',
            rabbitmq_username = 'root',
            rabbitmq_password = 'clickhouse',
            rabbitmq_num_consumers = 3,
            rabbitmq_flush_interval_ms = 100,
            rabbitmq_max_block_size = 100
    """
    )

    # Wait for RabbitMQ connection
    time.sleep(10)

    instance.query_with_retry(
        "SELECT count() FROM system.tables WHERE name='rabbitmq_queue' AND database='test'",
        check_callback=lambda x: x.strip() == "1",
    )

    instance.query("DETACH TABLE test.rabbitmq_queue")

    instance.query_with_retry(
        "SELECT count() FROM system.tables WHERE name='rabbitmq_queue' AND database='test'",
        check_callback=lambda x: x.strip() == "0",
    )

    rabbit_shutdown_time_after_detach = get_last_event_time(
        "StorageRabbitMQ (test.rabbitmq_queue)", "Shutdown finished"
    )
    instance.restart_clickhouse()

    registry_no_queue_to_shutdown_time1 = get_last_event_time(
        "StreamingStorageRegistry", "There are no queue storages to shutdown"
    )

    assert registry_no_queue_to_shutdown_time1 > rabbit_shutdown_time_after_detach

    instance.query_with_retry(
        "SELECT count() FROM system.tables WHERE name='rabbitmq_queue' AND database='test'",
        check_callback=lambda x: x.strip() == "1",
    )

    instance.restart_clickhouse()

    registry_shutdown_queue_time = get_last_event_time(
        "StreamingStorageRegistry", "Will shutdown 1 queue storages"
    )

    rabbit_shutdown_time_after_restart = get_last_event_time(
        "StorageRabbitMQ (test.rabbitmq_queue)", "Shutdown finished"
    )

    registry_already_shutdown_queue_time = get_last_event_time(
        "StreamingStorageRegistry", "Already shutdown 1 queue storages"
    )

    assert registry_shutdown_queue_time > registry_no_queue_to_shutdown_time1
    assert rabbit_shutdown_time_after_restart != rabbit_shutdown_time_after_detach

    assert rabbit_shutdown_time_after_restart > registry_shutdown_queue_time
    assert registry_already_shutdown_queue_time > rabbit_shutdown_time_after_restart


@pytest.mark.skip(
    reason=(
        "Flaky in CI: idle-loop shutdown timing is sensitive to libuv "
        "scheduling under integration-test load. See "
        "https://github.com/ClickHouse/ClickHouse/pull/104253 for the "
        "directive to disable, and https://github.com/ClickHouse/ClickHouse/pull/104273 "
        "for context."
    )
)
def test_idle_loop_shutdown_completes_promptly(started_cluster):
    """
    Regression test: when the RabbitMQ event loop is idle (no pending messages),
    shutdown/deactivation must complete promptly. Previously, UV_RUN_ONCE could
    block indefinitely when no events were pending and stop paths did not wake
    the libuv loop via uv_stop.
    """

    instance.query("DROP DATABASE IF EXISTS test SYNC")
    instance.query("DROP DATABASE IF EXISTS test_idle SYNC")
    instance.query("CREATE DATABASE test_idle ENGINE=Atomic")

    instance.query(
        """
        CREATE TABLE test_idle.destination (
            key UInt64,
            value String
        ) ENGINE = MergeTree()
        ORDER BY key
    """
    )

    instance.query(
        """
        CREATE TABLE test_idle.rabbitmq_queue (
            key UInt64,
            value String
        ) ENGINE = RabbitMQ
        SETTINGS
            rabbitmq_host_port = 'rabbitmq1:5672',
            rabbitmq_exchange_name = 'test_idle_shutdown_exchange',
            rabbitmq_exchange_type = 'fanout',
            rabbitmq_format = 'JSONEachRow',
            rabbitmq_username = 'root',
            rabbitmq_password = 'clickhouse',
            rabbitmq_flush_interval_ms = 100,
            rabbitmq_max_block_size = 100
    """
    )

    instance.query(
        """
        CREATE MATERIALIZED VIEW test_idle.consumer TO test_idle.destination AS
        SELECT key, value FROM test_idle.rabbitmq_queue
    """
    )

    # Wait for streaming to start — the loop is now active but idle (no messages).
    instance.wait_for_log_line("Started streaming to .* attached views")

    # Confirm the background loop is actually running before we restart.
    instance.wait_for_log_line("Background loop started")

    # Restart without publishing any messages — the loop has no pending events.
    instance.restart_clickhouse()

    # Verify the loop was deactivated during shutdown (not already stopped).
    instance.query("SYSTEM FLUSH LOGS")
    deactivate_count = int(
        instance.query(
            """SELECT count()
            FROM merge(system, '^text_log')
            WHERE
                logger_name = 'StorageRabbitMQ (test_idle.rabbitmq_queue)' AND
                message = 'Deactivating looping task'"""
        ).strip()
    )
    assert deactivate_count > 0, "Looping task was never deactivated — loop may not have been running"

    registry_shutdown_time = get_last_event_time(
        "StreamingStorageRegistry", "Will shutdown 1 queue storages"
    )

    rabbit_shutdown_time = get_last_event_time(
        "StorageRabbitMQ (test_idle.rabbitmq_queue)", "Shutdown finished"
    )

    registry_already_shutdown_time = get_last_event_time(
        "StreamingStorageRegistry", "Already shutdown 1 queue storages"
    )

    # Verify correct ordering.
    assert registry_shutdown_time < rabbit_shutdown_time
    assert rabbit_shutdown_time < registry_already_shutdown_time

    # The idle loop must not block shutdown for a long time.
    shutdown_duration = (
        rabbit_shutdown_time - registry_shutdown_time
    ).total_seconds()
    logging.info(f"Idle loop shutdown took {shutdown_duration:.2f}s")
    assert shutdown_duration < 15, (
        f"Idle loop shutdown took {shutdown_duration:.2f}s, expected < 15s. "
        "The event loop may be blocking in uv_run without being woken by uv_stop."
    )

    instance.query("DROP DATABASE test_idle SYNC")


@pytest.mark.skip(
    reason=(
        "Flaky in CI: depends on RabbitMQ message-delivery timing that is "
        "not reliable under integration-test load. See "
        "https://github.com/ClickHouse/ClickHouse/pull/104253 for the "
        "directive to disable, and https://github.com/ClickHouse/ClickHouse/pull/104273 "
        "for context."
    )
)
def test_rabbitmq_virtual_column_table(started_cluster):
    """
    Test that the `_table` virtual column returns the table name
    for the RabbitMQ engine.
    """

    instance.query("DROP DATABASE IF EXISTS test_virt SYNC")
    instance.query("CREATE DATABASE test_virt ENGINE=Atomic")

    exchange_name = "test_virtual_table_exchange"

    instance.query(
        f"""
        CREATE TABLE test_virt.rabbitmq_source (
            key UInt64,
            value String
        ) ENGINE = RabbitMQ
        SETTINGS
            rabbitmq_host_port = 'rabbitmq1:5672',
            rabbitmq_exchange_name = '{exchange_name}',
            rabbitmq_exchange_type = 'fanout',
            rabbitmq_format = 'JSONEachRow',
            rabbitmq_username = 'root',
            rabbitmq_password = 'clickhouse',
            rabbitmq_flush_interval_ms = 100,
            rabbitmq_max_block_size = 100,
            rabbitmq_commit_on_select = 1
    """
    )

    time.sleep(10)

    instance.query(
        """
        INSERT INTO test_virt.rabbitmq_source
        SELECT number AS key, toString(number) AS value
        FROM numbers(10)
        """
    )

    result = ""
    for _ in range(100):
        result += instance.query(
            """
            SELECT key, value, _exchange_name, _table
            FROM test_virt.rabbitmq_source
            SETTINGS stream_like_engine_allow_direct_select=1
            """
        )
        lines = [l for l in result.strip().split("\n") if l]
        if len(lines) == 10:
            break
        time.sleep(0.5)

    lines = [l for l in result.strip().split("\n") if l]
    assert len(lines) == 10, f"Expected 10 rows, got {len(lines)}"

    for line in lines:
        parts = line.split("\t")
        assert parts[2] == exchange_name, f"Expected exchange '{exchange_name}', got '{parts[2]}'"
        assert parts[3] == "rabbitmq_source", f"Expected table 'rabbitmq_source', got '{parts[3]}'"

    instance.query("DROP DATABASE test_virt SYNC")
