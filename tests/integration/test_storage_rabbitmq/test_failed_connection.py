import logging
import time

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster


DEFAULT_TIMEOUT_SEC = 60

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

instance2 = cluster.add_instance(
    "instance2",
    user_configs=["configs/users.xml"],
    with_rabbitmq=True,
)

instance3 = cluster.add_instance(
    "instance3",
    user_configs=["configs/users.xml"],
    main_configs=[
        "configs/rabbitmq.xml",
        "configs/macros.xml",
        "configs/named_collection.xml",
        "configs/mergetree.xml",
    ],
    with_rabbitmq=True,
    stay_alive=True,
)

# Helpers


def suspend_rabbitmq(rabbitmq_cluster):
    rabbitmq_cluster.stop_rabbitmq_app()


def resume_rabbitmq(rabbitmq_cluster):
    rabbitmq_cluster.start_rabbitmq_app()
    rabbitmq_cluster.wait_rabbitmq_to_start()


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
def rabbitmq_setup_teardown():
    logging.debug("RabbitMQ is available - running test")
    instance.query("CREATE DATABASE test")
    instance3.query("CREATE DATABASE test")
    yield  # run test
    instance.query("DROP DATABASE test SYNC")
    instance3.query("DROP DATABASE test SYNC")
    cluster.reset_rabbitmq()


# Tests

def test_rabbitmq_restore_failed_connection_without_losses_1(rabbitmq_cluster):
    instance.query(
        """
        DROP TABLE IF EXISTS test.consume;
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree
            ORDER BY key;
        CREATE TABLE test.consume (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_flush_interval_ms=500,
                     rabbitmq_max_block_size = 100,
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
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_row_delimiter = '\\n';
    """
    )

    messages_num = 100000
    values = []
    for i in range(messages_num):
        values.append("({i}, {i})".format(i=i))
    values = ",".join(values)

    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        try:
            instance.query(
                "INSERT INTO test.producer_reconnect VALUES {}".format(values)
            )
            break
        except QueryRuntimeException as e:
            if "Local: Timed out." in str(e):
                continue
            else:
                raise
    else:
        pytest.fail(
            f"Time limit of 180 seconds reached. The query could not be executed successfully."
        )

    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        number = int(instance.query("SELECT count() FROM test.view"))
        if number != 0:
            if number == messages_num:
                pytest.fail("The RabbitMQ messages have been consumed before suspending the RabbitMQ server")
            break
        time.sleep(0.1)
    else:
        pytest.fail(f"Time limit of 180 seconds reached. The count is still 0.")

    suspend_rabbitmq(rabbitmq_cluster)
    resume_rabbitmq(rabbitmq_cluster)

    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        result = instance.query("SELECT count(DISTINCT key) FROM test.view")
        if int(result) == messages_num:
            break
        time.sleep(1)
    else:
        pytest.fail(
            f"Time limit of 180 seconds reached. The result did not match the expected value."
        )

    instance.query(
        """
        DROP TABLE test.consume;
        DROP TABLE test.producer_reconnect;
    """
    )

    assert int(result) == messages_num, "ClickHouse lost some messages: {}".format(
        result
    )


def test_rabbitmq_restore_failed_connection_without_losses_2(rabbitmq_cluster):
    logging.getLogger("pika").propagate = False
    instance.query(
        """
        DROP TABLE IF EXISTS test.consumer_reconnect;
        CREATE TABLE test.consumer_reconnect (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'consumer_reconnect',
                     rabbitmq_num_consumers = 10,
                     rabbitmq_flush_interval_ms = 100,
                     rabbitmq_max_block_size = 100,
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

    messages_num = 150000

    messages = []
    for i in range(messages_num):
        messages.append("({i}, {i})".format(i=i))
    messages = ",".join(messages)

    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        try:
            instance.query(
                "INSERT INTO test.consumer_reconnect VALUES {}".format(messages)
            )
            break
        except QueryRuntimeException as e:
            if "Local: Timed out." in str(e):
                continue
            else:
                raise
    else:
        pytest.fail(
            f"Time limit of 180 seconds reached. The query could not be executed successfully."
        )

    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        number = int(instance.query("SELECT count() FROM test.view"))
        if number != 0:
            if number == messages_num:
                pytest.fail("The RabbitMQ messages have been consumed before suspending the RabbitMQ server")
            break
        time.sleep(0.1)
    else:
        pytest.fail(f"Time limit of 180 seconds reached. The count is still 0.")

    suspend_rabbitmq(rabbitmq_cluster)
    resume_rabbitmq(rabbitmq_cluster)

    # while int(instance.query('SELECT count() FROM test.view')) == 0:
    #    time.sleep(0.1)

    # kill_rabbitmq()
    # revive_rabbitmq()

    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        result = instance.query("SELECT count(DISTINCT key) FROM test.view").strip()
        if int(result) == messages_num:
            break
        logging.debug(f"Result: {result} / {messages_num}")
        time.sleep(1)
    else:
        pytest.fail(
            f"Time limit of 180 seconds reached. The result did not match the expected value."
        )

    instance.query(
        """
        DROP TABLE test.consumer;
        DROP TABLE test.consumer_reconnect;
    """
    )

    assert int(result) == messages_num, "ClickHouse lost some messages: {}".format(
        result
    )
