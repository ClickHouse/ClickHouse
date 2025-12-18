import logging
import time

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from .test import check_expected_result_polling

pytestmark = pytest.mark.timeout(1200)

DEFAULT_TIMEOUT_SEC = 120
CLICKHOUSE_VIEW_TIMEOUT_SEC = 120

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=[
        "configs/rabbitmq.xml",
        "configs/macros.xml",
        "configs/named_collection.xml",
    ],
    user_configs=["configs/users.xml"],
    with_rabbitmq=True,
    stay_alive=True,
)


# Fixtures

@pytest.fixture(scope="module")
def rabbitmq_cluster():
    try:
        cluster.start()
        logging.debug("rabbitmq_id is {}".format(instance.cluster.rabbitmq_docker_id))
        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def rabbitmq_fixture():
    logging.debug("RabbitMQ is available - running test")
    instance.query("CREATE DATABASE test")
    yield
    instance.query("DROP DATABASE test SYNC")
    cluster.reset_rabbitmq()


# Tests

def common_restore_failed_connection_without_losses(rabbitmq_cluster, messages_num, table_src, table_dst):
    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        try:
            instance.query(
                f"INSERT INTO {table_src} SELECT number, number FROM numbers({messages_num})"
            )
            break
        except QueryRuntimeException as e:
            if "Local: Timed out." in str(e):
                continue
            else:
                raise
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The query could not be executed successfully."
        )

    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        number = int(instance.query(f"SELECT count() FROM {table_dst}"))
        if number != 0:
            logging.debug(f"{number}/{messages_num} before suspending RabbitMQ")
            break
        time.sleep(0.1)
    else:
        pytest.fail(f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The count is still 0.")

    with rabbitmq_cluster.pause_rabbitmq():
        number = int(instance.query(f"SELECT count() FROM {table_dst}"))
        logging.debug(f"{number}/{messages_num} after suspending RabbitMQ")
        if number == messages_num:
            pytest.fail("All RabbitMQ messages have been consumed before resuming the RabbitMQ server")

    check_expected_result_polling(messages_num, f"SELECT count(DISTINCT key) FROM {table_dst}", instance=instance, timeout=CLICKHOUSE_VIEW_TIMEOUT_SEC)


def test_rabbitmq_restore_failed_connection_without_losses_1(rabbitmq_cluster):
    """
    This test checks that after inserting through a RabbitMQ Engine, we can keep consuming from it
    automatically after suspending and resuming the RabbitMQ server. To do that, we need the
    consumption to be slow enough (hence, the rabbitmq_max_block_size = 1) so that we can check that
    something has already been consumed before suspending RabbitMQ server, but not so fast so that
    everything is consumed before suspending and resuming the RabbitMQ server.
    """
    instance.query(
        """
        DROP TABLE IF EXISTS test.consume;
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree
            ORDER BY key;
        CREATE TABLE test.consume (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                    rabbitmq_flush_interval_ms=1000,
                    rabbitmq_max_block_size = 1,
                    rabbitmq_exchange_name = 'producer_reconnect',
                    rabbitmq_format = 'JSONEachRow',
                    rabbitmq_num_consumers = 2,
                    rabbitmq_row_delimiter = '\\n';
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.consume;
        DROP TABLE IF EXISTS test.producer_reconnect;
        CREATE TABLE test.producer_reconnect (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                    rabbitmq_exchange_name = 'producer_reconnect',
                    rabbitmq_persistent = '1',
                    rabbitmq_flush_interval_ms=1000,
                    rabbitmq_max_block_size = 1,
                    rabbitmq_format = 'JSONEachRow',
                    rabbitmq_row_delimiter = '\\n';
    """
    )

    messages_num = 10000
    common_restore_failed_connection_without_losses(rabbitmq_cluster, messages_num, "test.producer_reconnect", "test.view")

    instance.query(
        """
        DROP TABLE test.consume;
        DROP TABLE test.producer_reconnect;
        DROP TABLE test.view;
        """
    )


def test_rabbitmq_restore_failed_connection_without_losses_2(rabbitmq_cluster):
    """
    This test checks that after inserting through a RabbitMQ Engine, we can keep consuming from it
    automatically after suspending and resuming the RabbitMQ server. To do that, we need the
    consumption to be slow enough (hence, the rabbitmq_max_block_size = 1) so that we can check that
    something has already been consumed before suspending RabbitMQ server, but not so fast so that
    everything is consumed before suspending and resuming the RabbitMQ server.
    """
    instance.query(
        """
        DROP TABLE IF EXISTS test.consumer_reconnect;
        CREATE TABLE test.consumer_reconnect (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                    rabbitmq_exchange_name = 'consumer_reconnect',
                    rabbitmq_num_consumers = 2,
                    rabbitmq_flush_interval_ms = 1000,
                    rabbitmq_max_block_size = 1,
                    rabbitmq_num_queues = 10,
                    rabbitmq_format = 'JSONEachRow',
                    rabbitmq_row_delimiter = '\\n';
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.consumer_reconnect;
    """
    )

    messages_num = 10000
    common_restore_failed_connection_without_losses(rabbitmq_cluster, messages_num, "test.consumer_reconnect", "test.view")

    instance.query(
        """
        DROP TABLE test.consumer;
        DROP TABLE test.consumer_reconnect;
        DROP TABLE test.view;
        """
    )
