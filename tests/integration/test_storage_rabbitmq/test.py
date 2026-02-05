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
