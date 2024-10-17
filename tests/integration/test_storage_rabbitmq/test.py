import json
import logging
import math
import os.path as p
import random
import subprocess
import threading
import time
import uuid
from random import randrange

import pika
import pytest
from google.protobuf.internal.encoder import _VarintBytes

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster, check_rabbitmq_is_available
from helpers.test_tools import TSV

from . import rabbitmq_pb2

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


def rabbitmq_check_result(result, check=False, reference=None):
    if reference is None:
        reference = "\n".join([f"{i}\t{i}" for i in range(50)])
    if check:
        assert TSV(result) == TSV(reference)
    else:
        return TSV(result) == TSV(reference)


def wait_rabbitmq_to_start(rabbitmq_docker_id, cookie, timeout=180):
    logging.getLogger("pika").propagate = False
    start = time.time()
    while time.time() - start < timeout:
        try:
            if check_rabbitmq_is_available(rabbitmq_docker_id, cookie):
                logging.debug("RabbitMQ is available")
                return
            time.sleep(0.5)
        except Exception as ex:
            logging.debug("Can't connect to RabbitMQ " + str(ex))
            time.sleep(0.5)


def kill_rabbitmq(rabbitmq_id):
    p = subprocess.Popen(("docker", "stop", rabbitmq_id), stdout=subprocess.PIPE)
    p.wait(timeout=60)
    return p.returncode == 0


def revive_rabbitmq(rabbitmq_id, cookie):
    p = subprocess.Popen(("docker", "start", rabbitmq_id), stdout=subprocess.PIPE)
    p.wait(timeout=60)
    wait_rabbitmq_to_start(rabbitmq_id, cookie)


# Fixtures


@pytest.fixture(scope="module")
def rabbitmq_cluster():
    try:
        cluster.start()
        logging.debug("rabbitmq_id is {}".format(instance.cluster.rabbitmq_docker_id))
        instance.query("CREATE DATABASE test")
        instance3.query("CREATE DATABASE test")

        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def rabbitmq_setup_teardown():
    logging.debug("RabbitMQ is available - running test")
    yield  # run test
    instance.query("DROP DATABASE test SYNC")
    instance.query("CREATE DATABASE test")


# Tests


@pytest.mark.parametrize(
    "secure",
    [
        pytest.param(0),
        pytest.param(1),
    ],
)
def test_rabbitmq_select(rabbitmq_cluster, secure):
    if secure and instance.is_built_with_thread_sanitizer():
        pytest.skip(
            "Data races: see https://github.com/ClickHouse/ClickHouse/issues/56866"
        )

    port = cluster.rabbitmq_port
    if secure:
        port = cluster.rabbitmq_secure_port

    # MATERIALIZED and ALIAS columns are not supported in RabbitMQ engine, but we can test that it does not fail
    instance.query(
        """
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64, value2 ALIAS value + 1, value3 MATERIALIZED value + 1)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = '{}:{}',
                     rabbitmq_exchange_name = 'select',
                     rabbitmq_commit_on_select = 1,
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_row_delimiter = '\\n',
                     rabbitmq_secure = {};
        """.format(
            rabbitmq_cluster.rabbitmq_host, port, secure
        )
    )

    assert (
        "RabbitMQ table engine doesn\\'t support ALIAS, DEFAULT or MATERIALIZED columns"
        in instance.query("SELECT * FROM system.warnings")
    )

    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    messages = []
    for i in range(50):
        messages.append(json.dumps({"key": i, "value": i}))

    for message in messages:
        channel.basic_publish(exchange="select", routing_key="", body=message)

    connection.close()
    # The order of messages in select * from test.rabbitmq is not guaranteed, so sleep to collect everything in one select
    time.sleep(1)

    result = ""
    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        result += instance.query(
            "SELECT * FROM test.rabbitmq ORDER BY key", ignore_error=True
        )
        if rabbitmq_check_result(result):
            break
        time.sleep(0.05)
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The result did not match the expected value."
        )

    rabbitmq_check_result(result, True)


def test_rabbitmq_select_empty(rabbitmq_cluster):
    instance.query(
        """
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = '{}:5672',
                     rabbitmq_exchange_name = 'empty',
                     rabbitmq_commit_on_select = 1,
                     rabbitmq_format = 'TSV',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_row_delimiter = '\\n';
        """.format(
            rabbitmq_cluster.rabbitmq_host
        )
    )

    assert int(instance.query("SELECT count() FROM test.rabbitmq")) == 0


def test_rabbitmq_json_without_delimiter(rabbitmq_cluster):
    instance.query(
        """
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = '{}:5672',
                     rabbitmq_commit_on_select = 1,
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_max_block_size=100,
                     rabbitmq_exchange_name = 'json',
                     rabbitmq_format = 'JSONEachRow'
        """.format(
            rabbitmq_cluster.rabbitmq_host
        )
    )

    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    messages = ""
    for i in range(25):
        messages += json.dumps({"key": i, "value": i}) + "\n"

    all_messages = [messages]
    for message in all_messages:
        channel.basic_publish(exchange="json", routing_key="", body=message)

    messages = ""
    for i in range(25, 50):
        messages += json.dumps({"key": i, "value": i}) + "\n"
    all_messages = [messages]
    for message in all_messages:
        channel.basic_publish(exchange="json", routing_key="", body=message)

    connection.close()
    time.sleep(1)

    result = ""
    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        result += instance.query(
            "SELECT * FROM test.rabbitmq ORDER BY key", ignore_error=True
        )
        if rabbitmq_check_result(result):
            break

        time.sleep(0.05)
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The result did not match the expected value."
        )

    rabbitmq_check_result(result, True)


def test_rabbitmq_csv_with_delimiter(rabbitmq_cluster):
    instance.query(
        """
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'csv',
                     rabbitmq_commit_on_select = 1,
                     rabbitmq_format = 'CSV',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_max_block_size=100,
                     rabbitmq_row_delimiter = '\\n';
        """
    )

    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    messages = []
    for i in range(50):
        messages.append("{i}, {i}".format(i=i))

    for message in messages:
        channel.basic_publish(exchange="csv", routing_key="", body=message)

    connection.close()
    time.sleep(1)

    result = ""
    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        result += instance.query(
            "SELECT * FROM test.rabbitmq ORDER BY key", ignore_error=True
        )
        if rabbitmq_check_result(result):
            break

        time.sleep(0.05)
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The result did not match the expected value."
        )

    rabbitmq_check_result(result, True)


def test_rabbitmq_tsv_with_delimiter(rabbitmq_cluster):
    instance.query(
        """
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'tsv',
                     rabbitmq_format = 'TSV',
                     rabbitmq_commit_on_select = 1,
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_max_block_size=100,
                     rabbitmq_queue_base = 'tsv',
                     rabbitmq_row_delimiter = '\\n';
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.rabbitmq;
        """
    )

    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    messages = []
    for i in range(50):
        messages.append("{i}\t{i}".format(i=i))

    for message in messages:
        channel.basic_publish(exchange="tsv", routing_key="", body=message)
    connection.close()

    result = ""
    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        result = instance.query("SELECT * FROM test.view ORDER BY key")
        if rabbitmq_check_result(result):
            break
        time.sleep(0.05)
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The result did not match the expected value."
        )

    rabbitmq_check_result(result, True)


def test_rabbitmq_macros(rabbitmq_cluster):
    instance.query(
        """
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = '{rabbitmq_host}:{rabbitmq_port}',
                     rabbitmq_commit_on_select = 1,
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_max_block_size=100,
                     rabbitmq_exchange_name = '{rabbitmq_exchange_name}',
                     rabbitmq_format = '{rabbitmq_format}'
        """
    )

    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    message = ""
    for i in range(50):
        message += json.dumps({"key": i, "value": i}) + "\n"
    channel.basic_publish(exchange="macro", routing_key="", body=message)

    connection.close()
    time.sleep(1)

    result = ""
    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        result += instance.query(
            "SELECT * FROM test.rabbitmq ORDER BY key", ignore_error=True
        )
        if rabbitmq_check_result(result):
            break
        time.sleep(0.05)
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The result did not match the expected value."
        )

    rabbitmq_check_result(result, True)


def test_rabbitmq_materialized_view(rabbitmq_cluster):
    instance.query(
        """
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64, dt1 DateTime MATERIALIZED now(), value2 ALIAS value + 1)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'mv',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_max_block_size=100,
                     rabbitmq_row_delimiter = '\\n';
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.rabbitmq;

        CREATE TABLE test.view2 (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer2 TO test.view2 AS
            SELECT * FROM test.rabbitmq group by (key, value);
    """
    )

    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    instance.wait_for_log_line("Started streaming to 2 attached views")

    messages = []
    for i in range(50):
        message = json.dumps({"key": i, "value": i})
        channel.basic_publish(exchange="mv", routing_key="", body=message)

    time_limit_sec = 60
    deadline = time.monotonic() + time_limit_sec

    while time.monotonic() < deadline:
        result = instance.query("SELECT * FROM test.view ORDER BY key")
        if rabbitmq_check_result(result):
            break
        time.sleep(0.05)
    else:
        pytest.fail(
            f"Time limit of {time_limit_sec} seconds reached. The result did not match the expected value."
        )

    rabbitmq_check_result(result, True)

    deadline = time.monotonic() + time_limit_sec

    while time.monotonic() < deadline:
        result = instance.query("SELECT * FROM test.view2 ORDER BY key")
        logging.debug(f"Result: {result}")
        if rabbitmq_check_result(result):
            break
        time.sleep(1)
    else:
        pytest.fail(
            f"Time limit of {time_limit_sec} seconds reached. The result did not match the expected value."
        )

    rabbitmq_check_result(result, True)
    connection.close()


def test_rabbitmq_materialized_view_with_subquery(rabbitmq_cluster):
    instance.query(
        """
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'mvsq',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_max_block_size=100,
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_row_delimiter = '\\n';
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM (SELECT * FROM test.rabbitmq);
    """
    )

    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    messages = []
    for i in range(50):
        messages.append(json.dumps({"key": i, "value": i}))
    for message in messages:
        channel.basic_publish(exchange="mvsq", routing_key="", body=message)

    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC

    while time.monotonic() < deadline:
        result = instance.query("SELECT * FROM test.view ORDER BY key")
        if rabbitmq_check_result(result):
            break
        time.sleep(0.05)
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The result did not match the expected value."
        )

    connection.close()
    rabbitmq_check_result(result, True)


def test_rabbitmq_many_materialized_views(rabbitmq_cluster):
    instance.query(
        """
        DROP TABLE IF EXISTS test.view1;
        DROP TABLE IF EXISTS test.view2;
        DROP TABLE IF EXISTS test.view3;
        DROP TABLE IF EXISTS test.consumer1;
        DROP TABLE IF EXISTS test.consumer2;
        DROP TABLE IF EXISTS test.consumer3;
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64, value2 ALIAS value + 1, value3 MATERIALIZED value + 1, value4 DEFAULT 1)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'mmv',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_max_block_size=100,
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_row_delimiter = '\\n';
        CREATE TABLE test.view1 (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE TABLE test.view2 (key UInt64, value UInt64, value2 UInt64, value3 UInt64, value4 UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE TABLE test.view3 (key UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer1 TO test.view1 AS
            SELECT * FROM test.rabbitmq;
        CREATE MATERIALIZED VIEW test.consumer2 TO test.view2 AS
            SELECT * FROM test.rabbitmq;
        CREATE MATERIALIZED VIEW test.consumer3 TO test.view3 AS
            SELECT * FROM test.rabbitmq;
    """
    )

    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    instance.wait_for_log_line("Started streaming to 3 attached views")

    messages = []
    for i in range(50):
        messages.append(json.dumps({"key": i, "value": i}))
    for message in messages:
        channel.basic_publish(exchange="mmv", routing_key="", body=message)

    is_check_passed = False
    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        result1 = instance.query("SELECT * FROM test.view1 ORDER BY key")
        result2 = instance.query("SELECT * FROM test.view2 ORDER BY key")
        result3 = instance.query("SELECT * FROM test.view3 ORDER BY key")
        # Note that for view2 result is `i i 0 0 0`, but not `i i i+1 i+1 1` as expected, ALIAS/MATERIALIZED/DEFAULT columns are not supported in RabbitMQ engine
        # We onlt check that at least it do not fail
        if (
            rabbitmq_check_result(result1)
            and rabbitmq_check_result(
                result2, reference="\n".join([f"{i}\t{i}\t0\t0\t0" for i in range(50)])
            )
            and rabbitmq_check_result(
                result3, reference="\n".join([str(i) for i in range(50)])
            )
        ):
            is_check_passed = True
            break
        time.sleep(0.1)

    assert (
        is_check_passed
    ), f"References are not equal to results, result1: {result1}, result2: {result2}, result3: {result3}"

    instance.query(
        """
        DROP TABLE test.consumer1;
        DROP TABLE test.consumer2;
        DROP TABLE test.consumer3;
        DROP TABLE test.view1;
        DROP TABLE test.view2;
        DROP TABLE test.view3;
    """
    )

    connection.close()


def test_rabbitmq_big_message(rabbitmq_cluster):
    # Create batchs of messages of size ~100Kb
    rabbitmq_messages = 1000
    batch_messages = 1000
    messages = [
        json.dumps({"key": i, "value": "x" * 100}) * batch_messages
        for i in range(rabbitmq_messages)
    ]

    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    instance.query(
        """
        CREATE TABLE test.rabbitmq (key UInt64, value String)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'big',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_max_block_size=100,
                     rabbitmq_format = 'JSONEachRow';
        CREATE TABLE test.view (key UInt64, value String)
            ENGINE = MergeTree
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.rabbitmq;
    """
    )

    for message in messages:
        channel.basic_publish(exchange="big", routing_key="", body=message)

    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        result = instance.query("SELECT count() FROM test.view")
        if int(result) == batch_messages * rabbitmq_messages:
            break
        time.sleep(0.05)
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The result did not match the expected value."
        )

    connection.close()

    assert (
        int(result) == rabbitmq_messages * batch_messages
    ), "ClickHouse lost some messages: {}".format(result)


def test_rabbitmq_sharding_between_queues_publish(rabbitmq_cluster):
    NUM_CONSUMERS = 10
    NUM_QUEUES = 10
    logging.getLogger("pika").propagate = False

    instance.query(
        """
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'test_sharding',
                     rabbitmq_num_queues = 5,
                     rabbitmq_num_consumers = 10,
                     rabbitmq_max_block_size = 100,
                     rabbitmq_flush_interval_ms=500,
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_row_delimiter = '\\n';
        CREATE TABLE test.view (key UInt64, value UInt64, channel_id String)
            ENGINE = MergeTree
            ORDER BY key
            SETTINGS old_parts_lifetime=5, cleanup_delay_period=2, cleanup_delay_period_random_add=3,
            cleanup_thread_preferred_points_per_iteration=0;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT *, _channel_id AS channel_id FROM test.rabbitmq;
    """
    )

    i = [0]
    messages_num = 10000

    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )

    def produce():
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        messages = []
        for _ in range(messages_num):
            messages.append(json.dumps({"key": i[0], "value": i[0]}))
            i[0] += 1
        current = 0
        for message in messages:
            current += 1
            mes_id = str(current)
            channel.basic_publish(
                exchange="test_sharding",
                routing_key="",
                properties=pika.BasicProperties(message_id=mes_id),
                body=message,
            )
        connection.close()

    threads = []
    threads_num = 10

    for _ in range(threads_num):
        threads.append(threading.Thread(target=produce))
    for thread in threads:
        time.sleep(random.uniform(0, 1))
        thread.start()

    result1 = ""
    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    expected = messages_num * threads_num
    while time.monotonic() < deadline:
        result1 = instance.query("SELECT count() FROM test.view")
        time.sleep(1)
        if int(result1) == expected:
            break
        logging.debug(f"Result {result1} / {expected}")
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The result did not match the expected value."
        )

    result2 = instance.query("SELECT count(DISTINCT channel_id) FROM test.view")

    for thread in threads:
        thread.join()

    assert (
        int(result1) == messages_num * threads_num
    ), "ClickHouse lost some messages: {}".format(result1)
    assert int(result2) == 10


def test_rabbitmq_mv_combo(rabbitmq_cluster):
    NUM_MV = 5
    NUM_CONSUMERS = 4
    logging.getLogger("pika").propagate = False

    instance.query(
        """
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'combo',
                     rabbitmq_queue_base = 'combo',
                     rabbitmq_max_block_size = 100,
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_num_consumers = 2,
                     rabbitmq_num_queues = 5,
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_row_delimiter = '\\n';
    """
    )

    for mv_id in range(NUM_MV):
        instance.query(
            """
            DROP TABLE IF EXISTS test.combo_{0};
            DROP TABLE IF EXISTS test.combo_{0}_mv;
            CREATE TABLE test.combo_{0} (key UInt64, value UInt64)
                ENGINE = MergeTree()
                ORDER BY key;
            CREATE MATERIALIZED VIEW test.combo_{0}_mv TO test.combo_{0} AS
                SELECT * FROM test.rabbitmq;
        """.format(
                mv_id
            )
        )

    time.sleep(2)

    i = [0]
    messages_num = 10000

    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )

    def produce():
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        messages = []
        for _ in range(messages_num):
            messages.append(json.dumps({"key": i[0], "value": i[0]}))
            i[0] += 1
        for msg_id in range(messages_num):
            channel.basic_publish(
                exchange="combo",
                routing_key="",
                properties=pika.BasicProperties(message_id=str(msg_id)),
                body=messages[msg_id],
            )
        connection.close()

    threads = []
    threads_num = 20

    for _ in range(threads_num):
        threads.append(threading.Thread(target=produce))
    for thread in threads:
        time.sleep(random.uniform(0, 1))
        thread.start()

    # with threadsanitizer the speed of execution is about 8-13k rows per second.
    # so consumption of 1 mln rows will require about 125 seconds
    deadline = time.monotonic() + 180
    expected = messages_num * threads_num * NUM_MV
    while time.monotonic() < deadline:
        result = 0
        for mv_id in range(NUM_MV):
            result += int(
                instance.query("SELECT count() FROM test.combo_{0}".format(mv_id))
            )
        if int(result) == expected:
            break
        logging.debug(f"Result: {result} / {expected}")
        time.sleep(1)
    else:
        pytest.fail(
            f"Time limit of 180 seconds reached. The result did not match the expected value."
        )

    for thread in threads:
        thread.join()

    for mv_id in range(NUM_MV):
        instance.query(
            """
            DROP TABLE test.combo_{0}_mv;
            DROP TABLE test.combo_{0};
        """.format(
                mv_id
            )
        )

    assert (
        int(result) == messages_num * threads_num * NUM_MV
    ), "ClickHouse lost some messages: {}".format(result)


def test_rabbitmq_insert(rabbitmq_cluster):
    instance.query(
        """
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'insert',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_max_block_size=100,
                     rabbitmq_exchange_type = 'direct',
                     rabbitmq_routing_key_list = 'insert1',
                     rabbitmq_format = 'TSV',
                     rabbitmq_row_delimiter = '\\n';
    """
    )

    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    consumer_connection = pika.BlockingConnection(parameters)

    consumer = consumer_connection.channel()
    result = consumer.queue_declare(queue="")
    queue_name = result.method.queue
    consumer.queue_bind(exchange="insert", queue=queue_name, routing_key="insert1")

    values = []
    for i in range(50):
        values.append("({i}, {i})".format(i=i))
    values = ",".join(values)

    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        try:
            instance.query("INSERT INTO test.rabbitmq VALUES {}".format(values))
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

    insert_messages = []

    def onReceived(channel, method, properties, body):
        i = 0
        insert_messages.append(body.decode())
        if len(insert_messages) == 50:
            channel.stop_consuming()

    consumer.basic_consume(queue_name, onReceived)
    consumer.start_consuming()
    consumer_connection.close()

    result = "\n".join(insert_messages)
    rabbitmq_check_result(result, True)


def test_rabbitmq_insert_headers_exchange(rabbitmq_cluster):
    instance.query(
        """
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'insert_headers',
                     rabbitmq_exchange_type = 'headers',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_max_block_size=100,
                     rabbitmq_routing_key_list = 'test=insert,topic=headers',
                     rabbitmq_format = 'TSV',
                     rabbitmq_row_delimiter = '\\n';
    """
    )

    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    consumer_connection = pika.BlockingConnection(parameters)

    consumer = consumer_connection.channel()
    result = consumer.queue_declare(queue="")
    queue_name = result.method.queue
    consumer.queue_bind(
        exchange="insert_headers",
        queue=queue_name,
        routing_key="",
        arguments={"x-match": "all", "test": "insert", "topic": "headers"},
    )

    values = []
    for i in range(50):
        values.append("({i}, {i})".format(i=i))
    values = ",".join(values)

    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        try:
            instance.query("INSERT INTO test.rabbitmq VALUES {}".format(values))
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

    insert_messages = []

    def onReceived(channel, method, properties, body):
        i = 0
        insert_messages.append(body.decode())
        if len(insert_messages) == 50:
            channel.stop_consuming()

    consumer.basic_consume(queue_name, onReceived)
    consumer.start_consuming()
    consumer_connection.close()

    result = "\n".join(insert_messages)
    rabbitmq_check_result(result, True)


def test_rabbitmq_many_inserts(rabbitmq_cluster):
    instance.query(
        """
        DROP TABLE IF EXISTS test.rabbitmq_many;
        DROP TABLE IF EXISTS test.rabbitmq_consume;
        DROP TABLE IF EXISTS test.view_many;
        DROP TABLE IF EXISTS test.consumer_many;
        CREATE TABLE test.rabbitmq_many (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'many_inserts',
                     rabbitmq_exchange_type = 'direct',
                     rabbitmq_routing_key_list = 'insert2',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_max_block_size=100,
                     rabbitmq_format = 'TSV',
                     rabbitmq_row_delimiter = '\\n';
        CREATE TABLE test.rabbitmq_consume (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'many_inserts',
                     rabbitmq_exchange_type = 'direct',
                     rabbitmq_routing_key_list = 'insert2',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_max_block_size=100,
                     rabbitmq_format = 'TSV',
                     rabbitmq_row_delimiter = '\\n';
    """
    )

    messages_num = 10000
    values = []
    for i in range(messages_num):
        values.append("({i}, {i})".format(i=i))
    values = ",".join(values)

    def insert():
        deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
        while time.monotonic() < deadline:
            try:
                instance.query(
                    "INSERT INTO test.rabbitmq_many VALUES {}".format(values)
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

    threads = []
    threads_num = 10
    for _ in range(threads_num):
        threads.append(threading.Thread(target=insert))
    for thread in threads:
        time.sleep(random.uniform(0, 1))
        thread.start()

    instance.query(
        """
        CREATE TABLE test.view_many (key UInt64, value UInt64)
            ENGINE = MergeTree
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer_many TO test.view_many AS
            SELECT * FROM test.rabbitmq_consume;
    """
    )

    for thread in threads:
        thread.join()

    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        result = instance.query("SELECT count() FROM test.view_many")
        logging.debug(result, messages_num * threads_num)
        if int(result) == messages_num * threads_num:
            break
        time.sleep(1)
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The result did not match the expected value."
        )

    instance.query(
        """
        DROP TABLE test.rabbitmq_consume;
        DROP TABLE test.rabbitmq_many;
        DROP TABLE test.consumer_many;
        DROP TABLE test.view_many;
    """
    )

    assert (
        int(result) == messages_num * threads_num
    ), "ClickHouse lost some messages: {}".format(result)


def test_rabbitmq_overloaded_insert(rabbitmq_cluster):
    instance.query(
        """
        DROP TABLE IF EXISTS test.view_overload;
        DROP TABLE IF EXISTS test.consumer_overload;
        DROP TABLE IF EXISTS test.rabbitmq_consume;
        CREATE TABLE test.rabbitmq_consume (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'over',
                     rabbitmq_queue_base = 'over',
                     rabbitmq_exchange_type = 'direct',
                     rabbitmq_num_consumers = 2,
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_max_block_size = 100,
                     rabbitmq_routing_key_list = 'over',
                     rabbitmq_format = 'TSV',
                     rabbitmq_row_delimiter = '\\n';
        CREATE TABLE test.rabbitmq_overload (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'over',
                     rabbitmq_exchange_type = 'direct',
                     rabbitmq_routing_key_list = 'over',
                     rabbitmq_format = 'TSV',
                     rabbitmq_row_delimiter = '\\n';
        CREATE TABLE test.view_overload (key UInt64, value UInt64)
            ENGINE = MergeTree
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer_overload TO test.view_overload AS
            SELECT * FROM test.rabbitmq_consume;
    """
    )

    instance.wait_for_log_line("Started streaming to 1 attached views")

    messages_num = 100000

    def insert():
        values = []
        for i in range(messages_num):
            values.append("({i}, {i})".format(i=i))
        values = ",".join(values)

        deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
        while time.monotonic() < deadline:
            try:
                instance.query(
                    "INSERT INTO test.rabbitmq_overload VALUES {}".format(values)
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

    threads = []
    threads_num = 2
    for _ in range(threads_num):
        threads.append(threading.Thread(target=insert))
    for thread in threads:
        time.sleep(random.uniform(0, 1))
        thread.start()

    for thread in threads:
        thread.join()

    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        result = instance.query("SELECT count() FROM test.view_overload")
        expected = messages_num * threads_num
        if int(result) == expected:
            break
        logging.debug(f"Result: {result} / {expected}")
        time.sleep(1)
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The result did not match the expected value."
        )

    instance.query(
        """
        DROP TABLE test.consumer_overload SYNC;
        DROP TABLE test.view_overload SYNC;
        DROP TABLE test.rabbitmq_consume SYNC;
        DROP TABLE test.rabbitmq_overload SYNC;
    """
    )

    assert (
        int(result) == messages_num * threads_num
    ), "ClickHouse lost some messages: {}".format(result)


def test_rabbitmq_direct_exchange(rabbitmq_cluster):
    instance.query(
        """
        DROP TABLE IF EXISTS test.destination;
        CREATE TABLE test.destination(key UInt64, value UInt64)
        ENGINE = MergeTree()
        ORDER BY key
        SETTINGS old_parts_lifetime=5, cleanup_delay_period=2, cleanup_delay_period_random_add=3,
        cleanup_thread_preferred_points_per_iteration=0;
    """
    )

    num_tables = 5
    for consumer_id in range(num_tables):
        logging.debug(("Setting up table {}".format(consumer_id)))
        instance.query(
            """
            DROP TABLE IF EXISTS test.direct_exchange_{0};
            DROP TABLE IF EXISTS test.direct_exchange_{0}_mv;
            CREATE TABLE test.direct_exchange_{0} (key UInt64, value UInt64)
                ENGINE = RabbitMQ
                SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                         rabbitmq_num_consumers = 2,
                         rabbitmq_num_queues = 2,
                         rabbitmq_flush_interval_ms=1000,
                        rabbitmq_max_block_size=100,
                         rabbitmq_exchange_name = 'direct_exchange_testing',
                         rabbitmq_exchange_type = 'direct',
                         rabbitmq_routing_key_list = 'direct_{0}',
                         rabbitmq_format = 'JSONEachRow',
                         rabbitmq_row_delimiter = '\\n';
            CREATE MATERIALIZED VIEW test.direct_exchange_{0}_mv TO test.destination AS
            SELECT key, value FROM test.direct_exchange_{0};
        """.format(
                consumer_id
            )
        )

    i = [0]
    messages_num = 1000

    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    messages = []
    for _ in range(messages_num):
        messages.append(json.dumps({"key": i[0], "value": i[0]}))
        i[0] += 1

    key_num = 0
    for num in range(num_tables):
        key = "direct_" + str(key_num)
        key_num += 1
        for message in messages:
            mes_id = str(randrange(10))
            channel.basic_publish(
                exchange="direct_exchange_testing",
                routing_key=key,
                properties=pika.BasicProperties(message_id=mes_id),
                body=message,
            )

    connection.close()

    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        result = instance.query("SELECT count() FROM test.destination")
        time.sleep(1)
        if int(result) == messages_num * num_tables:
            break
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The result did not match the expected value."
        )

    for consumer_id in range(num_tables):
        instance.query(
            """
            DROP TABLE test.direct_exchange_{0}_mv;
            DROP TABLE test.direct_exchange_{0};
        """.format(
                consumer_id
            )
        )

    instance.query(
        """
        DROP TABLE IF EXISTS test.destination;
    """
    )

    assert (
        int(result) == messages_num * num_tables
    ), "ClickHouse lost some messages: {}".format(result)


def test_rabbitmq_fanout_exchange(rabbitmq_cluster):
    instance.query(
        """
        DROP TABLE IF EXISTS test.destination;
        CREATE TABLE test.destination(key UInt64, value UInt64)
        ENGINE = MergeTree()
        ORDER BY key;
    """
    )

    num_tables = 5
    for consumer_id in range(num_tables):
        logging.debug(("Setting up table {}".format(consumer_id)))
        instance.query(
            """
            DROP TABLE IF EXISTS test.fanout_exchange_{0};
            DROP TABLE IF EXISTS test.fanout_exchange_{0}_mv;
            CREATE TABLE test.fanout_exchange_{0} (key UInt64, value UInt64)
                ENGINE = RabbitMQ
                SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                         rabbitmq_num_consumers = 2,
                         rabbitmq_num_queues = 2,
                         rabbitmq_flush_interval_ms=1000,
                         rabbitmq_max_block_size=100,
                         rabbitmq_routing_key_list = 'key_{0}',
                         rabbitmq_exchange_name = 'fanout_exchange_testing',
                         rabbitmq_exchange_type = 'fanout',
                         rabbitmq_format = 'JSONEachRow',
                         rabbitmq_row_delimiter = '\\n';
            CREATE MATERIALIZED VIEW test.fanout_exchange_{0}_mv TO test.destination AS
            SELECT key, value FROM test.fanout_exchange_{0};
        """.format(
                consumer_id
            )
        )

    i = [0]
    messages_num = 1000

    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    messages = []
    for _ in range(messages_num):
        messages.append(json.dumps({"key": i[0], "value": i[0]}))
        i[0] += 1

    for msg_id in range(messages_num):
        channel.basic_publish(
            exchange="fanout_exchange_testing",
            routing_key="",
            properties=pika.BasicProperties(message_id=str(msg_id)),
            body=messages[msg_id],
        )

    connection.close()

    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        result = instance.query("SELECT count() FROM test.destination")
        time.sleep(1)
        if int(result) == messages_num * num_tables:
            break
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The result did not match the expected value."
        )

    for consumer_id in range(num_tables):
        instance.query(
            """
            DROP TABLE test.fanout_exchange_{0}_mv;
            DROP TABLE test.fanout_exchange_{0};
        """.format(
                consumer_id
            )
        )

    instance.query(
        """
        DROP TABLE test.destination;
    """
    )

    assert (
        int(result) == messages_num * num_tables
    ), "ClickHouse lost some messages: {}".format(result)


def test_rabbitmq_topic_exchange(rabbitmq_cluster):
    instance.query(
        """
        DROP TABLE IF EXISTS test.destination;
        CREATE TABLE test.destination(key UInt64, value UInt64)
        ENGINE = MergeTree()
        ORDER BY key;
    """
    )

    num_tables = 5
    for consumer_id in range(num_tables):
        logging.debug(("Setting up table {}".format(consumer_id)))
        instance.query(
            """
            DROP TABLE IF EXISTS test.topic_exchange_{0};
            DROP TABLE IF EXISTS test.topic_exchange_{0}_mv;
            CREATE TABLE test.topic_exchange_{0} (key UInt64, value UInt64)
                ENGINE = RabbitMQ
                SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                         rabbitmq_num_consumers = 2,
                         rabbitmq_num_queues = 2,
                         rabbitmq_flush_interval_ms=1000,
                         rabbitmq_max_block_size=100,
                         rabbitmq_exchange_name = 'topic_exchange_testing',
                         rabbitmq_exchange_type = 'topic',
                         rabbitmq_routing_key_list = '*.{0}',
                         rabbitmq_format = 'JSONEachRow',
                         rabbitmq_row_delimiter = '\\n';
            CREATE MATERIALIZED VIEW test.topic_exchange_{0}_mv TO test.destination AS
            SELECT key, value FROM test.topic_exchange_{0};
        """.format(
                consumer_id
            )
        )

    for consumer_id in range(num_tables):
        logging.debug(("Setting up table {}".format(num_tables + consumer_id)))
        instance.query(
            """
            DROP TABLE IF EXISTS test.topic_exchange_{0};
            DROP TABLE IF EXISTS test.topic_exchange_{0}_mv;
            CREATE TABLE test.topic_exchange_{0} (key UInt64, value UInt64)
                ENGINE = RabbitMQ
                SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                         rabbitmq_num_consumers = 2,
                         rabbitmq_num_queues = 2,
                         rabbitmq_flush_interval_ms=1000,
                         rabbitmq_max_block_size=100,
                         rabbitmq_exchange_name = 'topic_exchange_testing',
                         rabbitmq_exchange_type = 'topic',
                         rabbitmq_routing_key_list = '*.logs',
                         rabbitmq_format = 'JSONEachRow',
                         rabbitmq_row_delimiter = '\\n';
            CREATE MATERIALIZED VIEW test.topic_exchange_{0}_mv TO test.destination AS
            SELECT key, value FROM test.topic_exchange_{0};
        """.format(
                num_tables + consumer_id
            )
        )

    i = [0]
    messages_num = 1000

    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    messages = []
    for _ in range(messages_num):
        messages.append(json.dumps({"key": i[0], "value": i[0]}))
        i[0] += 1

    key_num = 0
    for num in range(num_tables):
        key = "topic." + str(key_num)
        key_num += 1
        for message in messages:
            channel.basic_publish(
                exchange="topic_exchange_testing", routing_key=key, body=message
            )

    key = "random.logs"
    current = 0
    for msg_id in range(messages_num):
        channel.basic_publish(
            exchange="topic_exchange_testing",
            routing_key=key,
            properties=pika.BasicProperties(message_id=str(msg_id)),
            body=messages[msg_id],
        )

    connection.close()

    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        result = instance.query("SELECT count() FROM test.destination")
        time.sleep(1)
        if int(result) == messages_num * num_tables + messages_num * num_tables:
            break
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The result did not match the expected value."
        )

    for consumer_id in range(num_tables * 2):
        instance.query(
            """
            DROP TABLE test.topic_exchange_{0}_mv;
            DROP TABLE test.topic_exchange_{0};
        """.format(
                consumer_id
            )
        )

    instance.query(
        """
        DROP TABLE test.destination;
    """
    )

    assert (
        int(result) == messages_num * num_tables + messages_num * num_tables
    ), "ClickHouse lost some messages: {}".format(result)


def test_rabbitmq_hash_exchange(rabbitmq_cluster):
    instance.query(
        """
        DROP TABLE IF EXISTS test.destination;
        CREATE TABLE test.destination(key UInt64, value UInt64, channel_id String)
        ENGINE = MergeTree()
        ORDER BY key;
    """
    )

    num_tables = 4
    for consumer_id in range(num_tables):
        table_name = "rabbitmq_consumer{}".format(consumer_id)
        logging.debug(("Setting up {}".format(table_name)))
        instance.query(
            """
            DROP TABLE IF EXISTS test.{0};
            DROP TABLE IF EXISTS test.{0}_mv;
            CREATE TABLE test.{0} (key UInt64, value UInt64)
                ENGINE = RabbitMQ
                SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                         rabbitmq_num_consumers = 4,
                         rabbitmq_num_queues = 2,
                         rabbitmq_exchange_type = 'consistent_hash',
                         rabbitmq_exchange_name = 'hash_exchange_testing',
                         rabbitmq_format = 'JSONEachRow',
                         rabbitmq_flush_interval_ms=1000,
                         rabbitmq_row_delimiter = '\\n';
            CREATE MATERIALIZED VIEW test.{0}_mv TO test.destination AS
                SELECT key, value, _channel_id AS channel_id FROM test.{0};
        """.format(
                table_name
            )
        )

    i = [0]
    messages_num = 500

    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )

    def produce():
        # init connection here because otherwise python rabbitmq client might fail
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        messages = []
        for _ in range(messages_num):
            messages.append(json.dumps({"key": i[0], "value": i[0]}))
            i[0] += 1
        for msg_id in range(messages_num):
            channel.basic_publish(
                exchange="hash_exchange_testing",
                routing_key=str(msg_id),
                properties=pika.BasicProperties(message_id=str(msg_id)),
                body=messages[msg_id],
            )
        connection.close()

    threads = []
    threads_num = 10

    for _ in range(threads_num):
        threads.append(threading.Thread(target=produce))
    for thread in threads:
        time.sleep(random.uniform(0, 1))
        thread.start()

    result1 = ""
    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        result1 = instance.query("SELECT count() FROM test.destination")
        time.sleep(1)
        if int(result1) == messages_num * threads_num:
            break
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The result did not match the expected value."
        )

    result2 = instance.query("SELECT count(DISTINCT channel_id) FROM test.destination")

    for consumer_id in range(num_tables):
        table_name = "rabbitmq_consumer{}".format(consumer_id)
        instance.query(
            """
            DROP TABLE test.{0}_mv;
            DROP TABLE test.{0};
        """.format(
                table_name
            )
        )

    instance.query(
        """
        DROP TABLE test.destination;
    """
    )

    for thread in threads:
        thread.join()

    assert (
        int(result1) == messages_num * threads_num
    ), "ClickHouse lost some messages: {}".format(result1)
    assert int(result2) == 4 * num_tables


def test_rabbitmq_multiple_bindings(rabbitmq_cluster):
    instance.query(
        """
        DROP TABLE IF EXISTS test.destination;
        CREATE TABLE test.destination(key UInt64, value UInt64)
        ENGINE = MergeTree()
        ORDER BY key;
    """
    )

    instance.query(
        """
        DROP TABLE IF EXISTS test.bindings;
        DROP TABLE IF EXISTS test.bindings_mv;
        CREATE TABLE test.bindings (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'multiple_bindings_testing',
                     rabbitmq_exchange_type = 'direct',
                     rabbitmq_routing_key_list = 'key1,key2,key3,key4,key5',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_max_block_size=100,
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_row_delimiter = '\\n';
        CREATE MATERIALIZED VIEW test.bindings_mv TO test.destination AS
            SELECT * FROM test.bindings;
    """
    )

    i = [0]
    messages_num = 500

    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )

    def produce():
        # init connection here because otherwise python rabbitmq client might fail
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        messages = []
        for _ in range(messages_num):
            messages.append(json.dumps({"key": i[0], "value": i[0]}))
            i[0] += 1

        keys = ["key1", "key2", "key3", "key4", "key5"]

        for key in keys:
            for message in messages:
                channel.basic_publish(
                    exchange="multiple_bindings_testing", routing_key=key, body=message
                )

        connection.close()

    threads = []
    threads_num = 10

    for _ in range(threads_num):
        threads.append(threading.Thread(target=produce))
    for thread in threads:
        time.sleep(random.uniform(0, 1))
        thread.start()

    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        result = instance.query("SELECT count() FROM test.destination")
        time.sleep(1)
        if int(result) == messages_num * threads_num * 5:
            break
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The result did not match the expected value."
        )

    for thread in threads:
        thread.join()

    instance.query(
        """
        DROP TABLE test.bindings;
        DROP TABLE test.bindings_mv;
        DROP TABLE test.destination;
    """
    )

    assert (
        int(result) == messages_num * threads_num * 5
    ), "ClickHouse lost some messages: {}".format(result)


def test_rabbitmq_headers_exchange(rabbitmq_cluster):
    instance.query(
        """
        DROP TABLE IF EXISTS test.destination;
        CREATE TABLE test.destination(key UInt64, value UInt64)
        ENGINE = MergeTree()
        ORDER BY key;
    """
    )

    num_tables_to_receive = 2
    for consumer_id in range(num_tables_to_receive):
        logging.debug(("Setting up table {}".format(consumer_id)))
        instance.query(
            """
            DROP TABLE IF EXISTS test.headers_exchange_{0};
            DROP TABLE IF EXISTS test.headers_exchange_{0}_mv;
            CREATE TABLE test.headers_exchange_{0} (key UInt64, value UInt64)
                ENGINE = RabbitMQ
                SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                         rabbitmq_num_consumers = 2,
                         rabbitmq_exchange_name = 'headers_exchange_testing',
                         rabbitmq_exchange_type = 'headers',
                         rabbitmq_flush_interval_ms=1000,
                         rabbitmq_max_block_size=100,
                         rabbitmq_routing_key_list = 'x-match=all,format=logs,type=report,year=2020',
                         rabbitmq_format = 'JSONEachRow',
                         rabbitmq_row_delimiter = '\\n';
            CREATE MATERIALIZED VIEW test.headers_exchange_{0}_mv TO test.destination AS
            SELECT key, value FROM test.headers_exchange_{0};
        """.format(
                consumer_id
            )
        )

    num_tables_to_ignore = 2
    for consumer_id in range(num_tables_to_ignore):
        logging.debug(
            ("Setting up table {}".format(consumer_id + num_tables_to_receive))
        )
        instance.query(
            """
            DROP TABLE IF EXISTS test.headers_exchange_{0};
            DROP TABLE IF EXISTS test.headers_exchange_{0}_mv;
            CREATE TABLE test.headers_exchange_{0} (key UInt64, value UInt64)
                ENGINE = RabbitMQ
                SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                         rabbitmq_exchange_name = 'headers_exchange_testing',
                         rabbitmq_exchange_type = 'headers',
                         rabbitmq_routing_key_list = 'x-match=all,format=logs,type=report,year=2019',
                         rabbitmq_format = 'JSONEachRow',
                         rabbitmq_flush_interval_ms=1000,
                         rabbitmq_max_block_size=100,
                         rabbitmq_row_delimiter = '\\n';
            CREATE MATERIALIZED VIEW test.headers_exchange_{0}_mv TO test.destination AS
            SELECT key, value FROM test.headers_exchange_{0};
        """.format(
                consumer_id + num_tables_to_receive
            )
        )

    i = [0]
    messages_num = 1000

    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    messages = []
    for _ in range(messages_num):
        messages.append(json.dumps({"key": i[0], "value": i[0]}))
        i[0] += 1

    fields = {}
    fields["format"] = "logs"
    fields["type"] = "report"
    fields["year"] = "2020"

    for msg_id in range(messages_num):
        channel.basic_publish(
            exchange="headers_exchange_testing",
            routing_key="",
            properties=pika.BasicProperties(headers=fields, message_id=str(msg_id)),
            body=messages[msg_id],
        )

    connection.close()

    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        result = instance.query("SELECT count() FROM test.destination")
        time.sleep(1)
        if int(result) == messages_num * num_tables_to_receive:
            break
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The result did not match the expected value."
        )

    for consumer_id in range(num_tables_to_receive + num_tables_to_ignore):
        instance.query(
            """
            DROP TABLE test.headers_exchange_{0}_mv;
            DROP TABLE test.headers_exchange_{0};
        """.format(
                consumer_id
            )
        )

    instance.query(
        """
        DROP TABLE test.destination;
    """
    )

    assert (
        int(result) == messages_num * num_tables_to_receive
    ), "ClickHouse lost some messages: {}".format(result)


def test_rabbitmq_virtual_columns(rabbitmq_cluster):
    instance.query(
        """
        CREATE TABLE test.rabbitmq_virtuals (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'virtuals',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_max_block_size=100,
                     rabbitmq_format = 'JSONEachRow';
        CREATE MATERIALIZED VIEW test.view Engine=Log AS
        SELECT value, key, _exchange_name, _channel_id, _delivery_tag, _redelivered FROM test.rabbitmq_virtuals;
    """
    )

    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    message_num = 10
    i = 0
    messages = []
    for _ in range(message_num):
        messages.append(json.dumps({"key": i, "value": i}))
        i += 1

    for message in messages:
        channel.basic_publish(exchange="virtuals", routing_key="", body=message)

    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        result = instance.query("SELECT count() FROM test.view")
        time.sleep(1)
        if int(result) == message_num:
            break
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The result did not match the expected value."
        )

    connection.close()

    result = instance.query(
        """
        SELECT key, value, _exchange_name, SUBSTRING(_channel_id, 1, 3), _delivery_tag, _redelivered
        FROM test.view ORDER BY key
    """
    )

    expected = """\
0	0	virtuals	1_0	1	0
1	1	virtuals	1_0	2	0
2	2	virtuals	1_0	3	0
3	3	virtuals	1_0	4	0
4	4	virtuals	1_0	5	0
5	5	virtuals	1_0	6	0
6	6	virtuals	1_0	7	0
7	7	virtuals	1_0	8	0
8	8	virtuals	1_0	9	0
9	9	virtuals	1_0	10	0
"""

    instance.query(
        """
        DROP TABLE test.rabbitmq_virtuals;
        DROP TABLE test.view;
    """
    )

    assert TSV(result) == TSV(expected)


def test_rabbitmq_virtual_columns_with_materialized_view(rabbitmq_cluster):
    instance.query(
        """
        CREATE TABLE test.rabbitmq_virtuals_mv (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'virtuals_mv',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_max_block_size=100,
                     rabbitmq_format = 'JSONEachRow';
        CREATE TABLE test.view (key UInt64, value UInt64,
            exchange_name String, channel_id String, delivery_tag UInt64, redelivered UInt8) ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
        SELECT *, _exchange_name as exchange_name, _channel_id as channel_id, _delivery_tag as delivery_tag, _redelivered as redelivered
        FROM test.rabbitmq_virtuals_mv;
    """
    )

    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    message_num = 10
    i = 0
    messages = []
    for _ in range(message_num):
        messages.append(json.dumps({"key": i, "value": i}))
        i += 1

    for message in messages:
        channel.basic_publish(exchange="virtuals_mv", routing_key="", body=message)

    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        result = instance.query("SELECT count() FROM test.view")
        time.sleep(1)
        if int(result) == message_num:
            break
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The result did not match the expected value."
        )

    connection.close()

    result = instance.query(
        "SELECT key, value, exchange_name, SUBSTRING(channel_id, 1, 3), delivery_tag, redelivered FROM test.view ORDER BY delivery_tag"
    )
    expected = """\
0	0	virtuals_mv	1_0	1	0
1	1	virtuals_mv	1_0	2	0
2	2	virtuals_mv	1_0	3	0
3	3	virtuals_mv	1_0	4	0
4	4	virtuals_mv	1_0	5	0
5	5	virtuals_mv	1_0	6	0
6	6	virtuals_mv	1_0	7	0
7	7	virtuals_mv	1_0	8	0
8	8	virtuals_mv	1_0	9	0
9	9	virtuals_mv	1_0	10	0
"""

    instance.query(
        """
        DROP TABLE test.consumer;
        DROP TABLE test.view;
        DROP TABLE test.rabbitmq_virtuals_mv
    """
    )

    assert TSV(result) == TSV(expected)


def test_rabbitmq_many_consumers_to_each_queue(rabbitmq_cluster):
    instance.query(
        """
        DROP TABLE IF EXISTS test.destination;
        CREATE TABLE test.destination(key UInt64, value UInt64, channel_id String)
        ENGINE = MergeTree()
        ORDER BY key;
    """
    )

    num_tables = 4
    for table_id in range(num_tables):
        logging.debug(("Setting up table {}".format(table_id)))
        instance.query(
            """
            DROP TABLE IF EXISTS test.many_consumers_{0};
            DROP TABLE IF EXISTS test.many_consumers_{0}_mv;
            CREATE TABLE test.many_consumers_{0} (key UInt64, value UInt64)
                ENGINE = RabbitMQ
                SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                         rabbitmq_exchange_name = 'many_consumers',
                         rabbitmq_num_queues = 2,
                         rabbitmq_num_consumers = 2,
                         rabbitmq_flush_interval_ms=1000,
                         rabbitmq_max_block_size=100,
                         rabbitmq_queue_base = 'many_consumers',
                         rabbitmq_format = 'JSONEachRow',
                         rabbitmq_row_delimiter = '\\n';
            CREATE MATERIALIZED VIEW test.many_consumers_{0}_mv TO test.destination AS
            SELECT key, value, _channel_id as channel_id FROM test.many_consumers_{0};
        """.format(
                table_id
            )
        )

    i = [0]
    messages_num = 1000

    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )

    def produce():
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        messages = []
        for _ in range(messages_num):
            messages.append(json.dumps({"key": i[0], "value": i[0]}))
            i[0] += 1
        for msg_id in range(messages_num):
            channel.basic_publish(
                exchange="many_consumers",
                routing_key="",
                properties=pika.BasicProperties(message_id=str(msg_id)),
                body=messages[msg_id],
            )
        connection.close()

    threads = []
    threads_num = 20

    for _ in range(threads_num):
        threads.append(threading.Thread(target=produce))
    for thread in threads:
        time.sleep(random.uniform(0, 1))
        thread.start()

    result1 = ""
    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        result1 = instance.query("SELECT count() FROM test.destination")
        time.sleep(1)
        if int(result1) == messages_num * threads_num:
            break
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The result did not match the expected value."
        )

    result2 = instance.query("SELECT count(DISTINCT channel_id) FROM test.destination")

    for thread in threads:
        thread.join()

    for consumer_id in range(num_tables):
        instance.query(
            """
            DROP TABLE test.many_consumers_{0};
            DROP TABLE test.many_consumers_{0}_mv;
        """.format(
                consumer_id
            )
        )

    instance.query(
        """
        DROP TABLE test.destination;
    """
    )

    assert (
        int(result1) == messages_num * threads_num
    ), "ClickHouse lost some messages: {}".format(result1)
    # 4 tables, 2 consumers for each table => 8 consumer tags
    assert int(result2) == 8


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

    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

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
        if int(instance.query("SELECT count() FROM test.view")) != 0:
            break
        time.sleep(0.1)
    else:
        pytest.fail(f"Time limit of 180 seconds reached. The count is still 0.")

    kill_rabbitmq(rabbitmq_cluster.rabbitmq_docker_id)
    time.sleep(4)
    revive_rabbitmq(
        rabbitmq_cluster.rabbitmq_docker_id, rabbitmq_cluster.rabbitmq_cookie
    )

    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        result = instance.query("SELECT count(DISTINCT key) FROM test.view")
        time.sleep(1)
        if int(result) == messages_num:
            break
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
    """
    )

    i = 0
    messages_num = 150000

    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )

    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    messages = []
    for _ in range(messages_num):
        messages.append(json.dumps({"key": i, "value": i}))
        i += 1
    for msg_id in range(messages_num):
        channel.basic_publish(
            exchange="consumer_reconnect",
            routing_key="",
            body=messages[msg_id],
            properties=pika.BasicProperties(delivery_mode=2, message_id=str(msg_id)),
        )
    connection.close()
    instance.query(
        """
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.consumer_reconnect;
    """
    )

    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        if int(instance.query("SELECT count() FROM test.view")) != 0:
            break
        logging.debug(3)
        time.sleep(0.1)
    else:
        pytest.fail(f"Time limit of 180 seconds reached. The count is still 0.")

    kill_rabbitmq(rabbitmq_cluster.rabbitmq_docker_id)
    time.sleep(8)
    revive_rabbitmq(
        rabbitmq_cluster.rabbitmq_docker_id, rabbitmq_cluster.rabbitmq_cookie
    )

    # while int(instance.query('SELECT count() FROM test.view')) == 0:
    #    time.sleep(0.1)

    # kill_rabbitmq()
    # time.sleep(2)
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


def test_rabbitmq_commit_on_block_write(rabbitmq_cluster):
    logging.getLogger("pika").propagate = False
    instance.query(
        """
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'block',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_queue_base = 'block',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_max_block_size = 100,
                     rabbitmq_row_delimiter = '\\n';
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.rabbitmq;
    """
    )

    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    cancel = threading.Event()

    i = [0]

    def produce():
        while not cancel.is_set():
            messages = []
            for _ in range(101):
                messages.append(json.dumps({"key": i[0], "value": i[0]}))
                i[0] += 1
            for message in messages:
                channel.basic_publish(exchange="block", routing_key="", body=message)

    rabbitmq_thread = threading.Thread(target=produce)
    rabbitmq_thread.start()

    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        if int(instance.query("SELECT count() FROM test.view")) != 0:
            break
        time.sleep(1)
    else:
        cancel.set()
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The count is still 0."
        )

    cancel.set()

    instance.query("DETACH TABLE test.rabbitmq;")

    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        if (
            int(
                instance.query(
                    "SELECT count() FROM system.tables WHERE database='test' AND name='rabbitmq'"
                )
            )
            != 1
        ):
            break
        time.sleep(1)
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The table 'rabbitmq' still exists."
        )

    instance.query("ATTACH TABLE test.rabbitmq;")

    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        if int(instance.query("SELECT uniqExact(key) FROM test.view")) >= i[0]:
            break
        time.sleep(1)
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The uniqExact(key) is still less than {i[0]}."
        )

    result = int(instance.query("SELECT count() == uniqExact(key) FROM test.view"))

    instance.query(
        """
        DROP TABLE test.consumer;
        DROP TABLE test.view;
    """
    )

    rabbitmq_thread.join()
    connection.close()

    assert result == 1, "Messages from RabbitMQ get duplicated!"


def test_rabbitmq_no_connection_at_startup_1(rabbitmq_cluster):
    error = instance.query_and_get_error(
        """
        CREATE TABLE test.cs (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'no_connection_at_startup:5672',
                     rabbitmq_exchange_name = 'cs',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_num_consumers = '5',
                     rabbitmq_row_delimiter = '\\n';
    """
    )
    assert "CANNOT_CONNECT_RABBITMQ" in error


def test_rabbitmq_no_connection_at_startup_2(rabbitmq_cluster):
    instance.query(
        """
        CREATE TABLE test.cs (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'cs',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_num_consumers = '5',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_max_block_size=100,
                     rabbitmq_row_delimiter = '\\n';
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.cs;
    """
    )
    instance.query("DETACH TABLE test.cs")
    rabbitmq_cluster.pause_container("rabbitmq1")
    instance.query("ATTACH TABLE test.cs")
    rabbitmq_cluster.unpause_container("rabbitmq1")

    messages_num = 1000
    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    for i in range(messages_num):
        message = json.dumps({"key": i, "value": i})
        channel.basic_publish(
            exchange="cs",
            routing_key="",
            body=message,
            properties=pika.BasicProperties(delivery_mode=2, message_id=str(i)),
        )
    connection.close()

    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        result = instance.query("SELECT count() FROM test.view")
        time.sleep(1)
        if int(result) == messages_num:
            break
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The result did not match the expected value."
        )

    instance.query(
        """
        DROP TABLE test.consumer;
        DROP TABLE test.cs;
    """
    )

    assert int(result) == messages_num, "ClickHouse lost some messages: {}".format(
        result
    )


def test_rabbitmq_format_factory_settings(rabbitmq_cluster):
    instance.query(
        """
        CREATE TABLE test.format_settings (
            id String, date DateTime
        ) ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'format_settings',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_format = 'JSONEachRow',
                     date_time_input_format = 'best_effort';
        """
    )

    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    message = json.dumps(
        {"id": "format_settings_test", "date": "2021-01-19T14:42:33.1829214Z"}
    )
    expected = instance.query(
        """SELECT parseDateTimeBestEffort(CAST('2021-01-19T14:42:33.1829214Z', 'String'))"""
    )

    channel.basic_publish(exchange="format_settings", routing_key="", body=message)
    result = ""
    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        result = instance.query("SELECT date FROM test.format_settings")
        if result == expected:
            break
        time.sleep(0.05)
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The result did not match the expected value."
        )

    instance.query(
        """
        CREATE TABLE test.view (
            id String, date DateTime
        ) ENGINE = MergeTree ORDER BY id;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.format_settings;
        """
    )

    channel.basic_publish(exchange="format_settings", routing_key="", body=message)

    result = ""
    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        result = instance.query("SELECT date FROM test.view")
        if result == expected:
            break
        time.sleep(0.05)
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The result did not match the expected value."
        )

    connection.close()
    instance.query(
        """
        DROP TABLE test.consumer;
        DROP TABLE test.format_settings;
    """
    )

    assert result == expected


def test_rabbitmq_vhost(rabbitmq_cluster):
    instance.query(
        """
        CREATE TABLE test.rabbitmq_vhost (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'vhost',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_vhost = '/'
        """
    )

    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.basic_publish(
        exchange="vhost", routing_key="", body=json.dumps({"key": 1, "value": 2})
    )
    connection.close()
    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        result = instance.query(
            "SELECT * FROM test.rabbitmq_vhost ORDER BY key", ignore_error=True
        )
        if result == "1\t2\n":
            break
        time.sleep(0.05)
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The result did not match the expected value '1\\t2\\n'."
        )


def test_rabbitmq_drop_table_properly(rabbitmq_cluster):
    instance.query(
        """
        CREATE TABLE test.rabbitmq_drop (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_exchange_name = 'drop',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_queue_base = 'rabbit_queue_drop'
        """
    )

    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    channel.basic_publish(
        exchange="drop", routing_key="", body=json.dumps({"key": 1, "value": 2})
    )
    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        result = instance.query(
            "SELECT * FROM test.rabbitmq_drop ORDER BY key", ignore_error=True
        )
        if result == "1\t2\n":
            break
        time.sleep(0.05)
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The result did not match the expected value '1\\t2\\n'."
        )

    exists = channel.queue_declare(queue="rabbit_queue_drop", passive=True)
    assert exists

    instance.query("DROP TABLE test.rabbitmq_drop")
    time.sleep(30)

    try:
        exists = channel.queue_declare(queue="rabbit_queue_drop", passive=True)
    except Exception as e:
        exists = False

    assert not exists


def test_rabbitmq_queue_settings(rabbitmq_cluster):
    instance.query(
        """
        CREATE TABLE test.rabbitmq_settings (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'rabbit_exchange',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_queue_base = 'rabbit_queue_settings',
                     rabbitmq_queue_settings_list = 'x-max-length=10,x-overflow=reject-publish'
        """
    )

    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    for i in range(50):
        channel.basic_publish(
            exchange="rabbit_exchange",
            routing_key="",
            body=json.dumps({"key": 1, "value": 2}),
        )
    connection.close()

    instance.query(
        """
        CREATE TABLE test.view (key UInt64, value UInt64)
        ENGINE = MergeTree ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.rabbitmq_settings;
        """
    )

    time.sleep(5)

    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        result = instance.query("SELECT count() FROM test.view", ignore_error=True)
        if int(result) == 10:
            break
        time.sleep(0.5)
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The result did not match the expected value of 10."
        )

    instance.query("DROP TABLE test.rabbitmq_settings")

    # queue size is 10, but 50 messages were sent, they will be dropped (setting x-overflow = reject-publish) and only 10 will remain.
    assert int(result) == 10


def test_rabbitmq_queue_consume(rabbitmq_cluster):
    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(queue="rabbit_queue", durable=True)

    i = [0]
    messages_num = 1000

    def produce():
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        messages = []
        for _ in range(messages_num):
            message = json.dumps({"key": i[0], "value": i[0]})
            channel.basic_publish(exchange="", routing_key="rabbit_queue", body=message)
            i[0] += 1

    threads = []
    threads_num = 10
    for _ in range(threads_num):
        threads.append(threading.Thread(target=produce))
    for thread in threads:
        time.sleep(random.uniform(0, 1))
        thread.start()

    instance.query(
        """
        CREATE TABLE test.rabbitmq_queue (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_queue_base = 'rabbit_queue',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_queue_consume = 1;
        CREATE TABLE test.view (key UInt64, value UInt64)
        ENGINE = MergeTree ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.rabbitmq_queue;
        """
    )

    result = ""
    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        result = instance.query("SELECT count() FROM test.view")
        if int(result) == messages_num * threads_num:
            break
        time.sleep(1)
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The result did not match the expected value."
        )

    for thread in threads:
        thread.join()

    instance.query("DROP TABLE test.rabbitmq_queue")


def test_rabbitmq_produce_consume_avro(rabbitmq_cluster):
    num_rows = 75

    instance.query(
        """
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.rabbit;
        DROP TABLE IF EXISTS test.rabbit_writer;

        CREATE TABLE test.rabbit_writer (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_format = 'Avro',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_exchange_name = 'avro',
                     rabbitmq_exchange_type = 'direct',
                     rabbitmq_routing_key_list = 'avro';

        CREATE TABLE test.rabbit (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_format = 'Avro',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_exchange_name = 'avro',
                     rabbitmq_exchange_type = 'direct',
                     rabbitmq_routing_key_list = 'avro';

        CREATE MATERIALIZED VIEW test.view Engine=Log AS
            SELECT key, value FROM test.rabbit;
    """
    )

    instance.query(
        "INSERT INTO test.rabbit_writer select number*10 as key, number*100 as value from numbers({num_rows}) SETTINGS output_format_avro_rows_in_file = 7".format(
            num_rows=num_rows
        )
    )

    # Ideally we should wait for an event
    time.sleep(3)

    expected_num_rows = instance.query(
        "SELECT COUNT(1) FROM test.view", ignore_error=True
    )
    assert int(expected_num_rows) == num_rows

    expected_max_key = instance.query(
        "SELECT max(key) FROM test.view", ignore_error=True
    )
    assert int(expected_max_key) == (num_rows - 1) * 10


def test_rabbitmq_bad_args(rabbitmq_cluster):
    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.exchange_declare(exchange="f", exchange_type="fanout")
    assert "Unable to declare exchange" in instance.query_and_get_error(
        """
        CREATE TABLE test.drop (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_exchange_name = 'f',
                     rabbitmq_format = 'JSONEachRow';
    """
    )


def test_rabbitmq_issue_30691(rabbitmq_cluster):
    instance.query(
        """
        CREATE TABLE test.rabbitmq_drop (json String)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_exchange_name = '30691',
                     rabbitmq_row_delimiter = '\\n', -- Works only if adding this setting
                     rabbitmq_format = 'LineAsString',
                     rabbitmq_queue_base = '30691';
        """
    )

    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    channel.basic_publish(
        exchange="30691",
        routing_key="",
        body=json.dumps(
            {
                "event_type": "purge",
                "as_src": 1234,
                "as_dst": 0,
                "as_path": "",
                "local_pref": 100,
                "med": 0,
                "peer_as_dst": 0,
                "ip_src": "<redacted ipv6>",
                "ip_dst": "<redacted ipv6>",
                "port_src": 443,
                "port_dst": 41930,
                "ip_proto": "tcp",
                "tos": 0,
                "stamp_inserted": "2021-10-26 15:20:00",
                "stamp_updated": "2021-10-26 15:23:14",
                "packets": 2,
                "bytes": 1216,
                "writer_id": "default_amqp/449206",
            }
        ),
    )
    result = ""
    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        result = instance.query("SELECT * FROM test.rabbitmq_drop", ignore_error=True)
        logging.debug(result)
        if result != "":
            break
        time.sleep(0.05)
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The result is still empty."
        )
    assert (
        result.strip()
        == """{"event_type": "purge", "as_src": 1234, "as_dst": 0, "as_path": "", "local_pref": 100, "med": 0, "peer_as_dst": 0, "ip_src": "<redacted ipv6>", "ip_dst": "<redacted ipv6>", "port_src": 443, "port_dst": 41930, "ip_proto": "tcp", "tos": 0, "stamp_inserted": "2021-10-26 15:20:00", "stamp_updated": "2021-10-26 15:23:14", "packets": 2, "bytes": 1216, "writer_id": "default_amqp/449206"}"""
    )


def test_rabbitmq_drop_mv(rabbitmq_cluster):
    instance.query(
        """
        CREATE TABLE test.drop_mv (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'mv',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_queue_base = 'drop_mv';
    """
    )
    instance.query(
        """
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
    """
    )
    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.drop_mv;
    """
    )

    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    messages = []
    for i in range(20):
        channel.basic_publish(
            exchange="mv", routing_key="", body=json.dumps({"key": i, "value": i})
        )

    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        res = instance.query("SELECT COUNT(*) FROM test.view")
        logging.debug(f"Current count (1): {res}")
        if int(res) == 20:
            break
        else:
            logging.debug(f"Number of rows in test.view: {res}")
        time.sleep(0.05)
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The row count did not reach 20."
        )

    instance.query("DROP VIEW test.consumer SYNC")
    for i in range(20, 40):
        channel.basic_publish(
            exchange="mv", routing_key="", body=json.dumps({"key": i, "value": i})
        )

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.drop_mv;
    """
    )
    for i in range(40, 50):
        channel.basic_publish(
            exchange="mv", routing_key="", body=json.dumps({"key": i, "value": i})
        )

    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        result = instance.query("SELECT count() FROM test.view")
        logging.debug(f"Current count (2): {result}")
        if int(result) == 50:
            break
        time.sleep(1)
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The row count did not reach 50."
        )

    result = instance.query("SELECT * FROM test.view ORDER BY key")
    rabbitmq_check_result(result, True)

    instance.query("DROP VIEW test.consumer SYNC")
    time.sleep(10)
    for i in range(50, 60):
        channel.basic_publish(
            exchange="mv", routing_key="", body=json.dumps({"key": i, "value": i})
        )
    connection.close()

    count = 0
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        count = int(instance.query("SELECT count() FROM test.drop_mv"))
        if count:
            break
        time.sleep(0.05)
    else:
        pytest.fail(f"Time limit of 30 seconds reached. The count is still 0.")

    instance.query("DROP TABLE test.drop_mv")
    assert count > 0


def test_rabbitmq_random_detach(rabbitmq_cluster):
    NUM_CONSUMERS = 2
    NUM_QUEUES = 2
    instance.query(
        """
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'random',
                     rabbitmq_queue_base = 'random',
                     rabbitmq_num_queues = 2,
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_num_consumers = 2,
                     rabbitmq_format = 'JSONEachRow';
        CREATE TABLE test.view (key UInt64, value UInt64, channel_id String)
            ENGINE = MergeTree
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT *, _channel_id AS channel_id FROM test.rabbitmq;
    """
    )

    i = [0]
    messages_num = 10000

    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )

    def produce():
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        messages = []
        for j in range(messages_num):
            messages.append(json.dumps({"key": i[0], "value": i[0]}))
            i[0] += 1
            mes_id = str(i)
            channel.basic_publish(
                exchange="random",
                routing_key="",
                properties=pika.BasicProperties(message_id=mes_id),
                body=messages[-1],
            )
        connection.close()

    threads = []
    threads_num = 20

    for _ in range(threads_num):
        threads.append(threading.Thread(target=produce))
    for thread in threads:
        time.sleep(random.uniform(0, 1))
        thread.start()

    # time.sleep(5)
    # kill_rabbitmq(rabbitmq_cluster.rabbitmq_docker_id)
    # instance.query("detach table test.rabbitmq")
    # revive_rabbitmq(rabbitmq_cluster.rabbitmq_docker_id)

    for thread in threads:
        thread.join()


def test_rabbitmq_predefined_configuration(rabbitmq_cluster):
    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    instance.query(
        """
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ(rabbit1, rabbitmq_vhost = '/')
            SETTINGS rabbitmq_flush_interval_ms=1000;
        """
    )

    channel.basic_publish(
        exchange="named", routing_key="", body=json.dumps({"key": 1, "value": 2})
    )

    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        result = instance.query(
            "SELECT * FROM test.rabbitmq ORDER BY key", ignore_error=True
        )
        if result == "1\t2\n":
            break
        time.sleep(0.05)
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The result did not match the expected value '1\\t2\\n'."
        )

    instance.restart_clickhouse()
    channel.basic_publish(
        exchange="named", routing_key="", body=json.dumps({"key": 1, "value": 2})
    )
    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        result = instance.query(
            "SELECT * FROM test.rabbitmq ORDER BY key", ignore_error=True
        )
        if result == "1\t2\n":
            break
        time.sleep(0.05)
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The result did not match the expected value '1\\t2\\n'."
        )


def test_rabbitmq_msgpack(rabbitmq_cluster):
    instance.query(
        """
        drop table if exists rabbit_in;
        drop table if exists rabbit_out;
        create table
            rabbit_in (val String)
            engine=RabbitMQ
            settings rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'xhep',
                     rabbitmq_format = 'MsgPack',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_num_consumers = 1;
        create table
            rabbit_out (val String)
            engine=RabbitMQ
            settings rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'xhep',
                     rabbitmq_format = 'MsgPack',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_num_consumers = 1;
        set stream_like_engine_allow_direct_select=1;
        insert into rabbit_out select 'kek';
        """
    )

    result = ""
    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        result = instance.query("select * from rabbit_in;")
        if result.strip() == "kek":
            break
        time.sleep(1)
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The result did not match 'kek'."
        )

    assert result.strip() == "kek"

    instance.query("drop table rabbit_in sync")
    instance.query("drop table rabbit_out sync")


def test_rabbitmq_address(rabbitmq_cluster):
    instance2.query(
        """
        drop table if exists rabbit_in;
        drop table if exists rabbit_out;
        create table
            rabbit_in (val String)
            engine=RabbitMQ
            SETTINGS rabbitmq_exchange_name = 'rxhep',
                     rabbitmq_format = 'CSV',
                     rabbitmq_num_consumers = 1,
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_address='amqp://root:clickhouse@rabbitmq1:5672/';
        create table
            rabbit_out (val String) engine=RabbitMQ
            SETTINGS rabbitmq_exchange_name = 'rxhep',
                     rabbitmq_format = 'CSV',
                     rabbitmq_num_consumers = 1,
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_address='amqp://root:clickhouse@rabbitmq1:5672/';
        set stream_like_engine_allow_direct_select=1;
        insert into rabbit_out select 'kek';
    """
    )

    result = ""
    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        result = instance2.query("select * from rabbit_in;")
        if result.strip() == "kek":
            break
        time.sleep(1)
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The result did not match 'kek'."
        )

    assert result.strip() == "kek"

    instance2.query("drop table rabbit_in sync")
    instance2.query("drop table rabbit_out sync")


def test_format_with_prefix_and_suffix(rabbitmq_cluster):
    instance.query(
        """
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'insert',
                     rabbitmq_exchange_type = 'direct',
                     rabbitmq_routing_key_list = 'custom',
                     rabbitmq_format = 'CustomSeparated';
    """
    )

    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    consumer_connection = pika.BlockingConnection(parameters)

    consumer = consumer_connection.channel()
    result = consumer.queue_declare(queue="")
    queue_name = result.method.queue
    consumer.queue_bind(exchange="insert", queue=queue_name, routing_key="custom")

    instance.query(
        "INSERT INTO test.rabbitmq select number*10 as key, number*100 as value from numbers(2) settings format_custom_result_before_delimiter='<prefix>\n', format_custom_result_after_delimiter='<suffix>\n'"
    )

    insert_messages = []

    def onReceived(channel, method, properties, body):
        message = body.decode()
        insert_messages.append(message)
        logging.debug(f"Received {len(insert_messages)} message: {message}")
        if len(insert_messages) == 2:
            channel.stop_consuming()

    consumer.basic_consume(queue_name, onReceived)

    consumer.start_consuming()
    consumer_connection.close()

    assert (
        "".join(insert_messages)
        == "<prefix>\n0\t0\n<suffix>\n<prefix>\n10\t100\n<suffix>\n"
    )


def test_max_rows_per_message(rabbitmq_cluster):
    num_rows = 5

    instance.query(
        """
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.rabbit;

        CREATE TABLE test.rabbit (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_format = 'CustomSeparated',
                     rabbitmq_exchange_name = 'custom',
                     rabbitmq_exchange_type = 'direct',
                     rabbitmq_routing_key_list = 'custom1',
                     rabbitmq_max_rows_per_message = 3,
                     rabbitmq_flush_interval_ms = 1000,
                     format_custom_result_before_delimiter = '<prefix>\n',
                     format_custom_result_after_delimiter = '<suffix>\n';

        CREATE MATERIALIZED VIEW test.view Engine=Log AS
            SELECT key, value FROM test.rabbit;
    """
    )

    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    consumer_connection = pika.BlockingConnection(parameters)

    consumer = consumer_connection.channel()
    result = consumer.queue_declare(queue="")
    queue_name = result.method.queue
    consumer.queue_bind(exchange="custom", queue=queue_name, routing_key="custom1")

    instance.query(
        f"INSERT INTO test.rabbit select number*10 as key, number*100 as value from numbers({num_rows}) settings format_custom_result_before_delimiter='<prefix>\n', format_custom_result_after_delimiter='<suffix>\n'"
    )

    insert_messages = []

    def onReceived(channel, method, properties, body):
        insert_messages.append(body.decode())
        if len(insert_messages) == 2:
            channel.stop_consuming()

    consumer.basic_consume(queue_name, onReceived)
    consumer.start_consuming()
    consumer_connection.close()

    assert len(insert_messages) == 2

    assert (
        "".join(insert_messages)
        == "<prefix>\n0\t0\n10\t100\n20\t200\n<suffix>\n<prefix>\n30\t300\n40\t400\n<suffix>\n"
    )

    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    rows = 0
    while time.monotonic() < deadline:
        rows = int(instance.query("SELECT count() FROM test.view"))
        if rows == num_rows:
            break
        time.sleep(0.05)
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The number of rows did not match {num_rows}."
        )

    assert rows == num_rows

    result = instance.query("SELECT * FROM test.view")
    assert result == "0\t0\n10\t100\n20\t200\n30\t300\n40\t400\n"


def test_row_based_formats(rabbitmq_cluster):
    num_rows = 10

    for format_name in [
        "TSV",
        "TSVWithNamesAndTypes",
        "TSKV",
        "CSV",
        "CSVWithNamesAndTypes",
        "CustomSeparatedWithNamesAndTypes",
        "Values",
        "JSON",
        "JSONEachRow",
        "JSONCompactEachRow",
        "JSONCompactEachRowWithNamesAndTypes",
        "JSONObjectEachRow",
        "Avro",
        "RowBinary",
        "RowBinaryWithNamesAndTypes",
        "MsgPack",
    ]:
        logging.debug(format_name)

        instance.query(
            f"""
            DROP TABLE IF EXISTS test.view;
            DROP TABLE IF EXISTS test.rabbit;

            CREATE TABLE test.rabbit (key UInt64, value UInt64)
                ENGINE = RabbitMQ
                SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                         rabbitmq_format = '{format_name}',
                         rabbitmq_exchange_name = '{format_name}',
                         rabbitmq_exchange_type = 'direct',
                         rabbitmq_max_block_size = 100,
                         rabbitmq_flush_interval_ms = 1000,
                         rabbitmq_routing_key_list = '{format_name}',
                         rabbitmq_max_rows_per_message = 5;

            CREATE MATERIALIZED VIEW test.view Engine=Log AS
                SELECT key, value FROM test.rabbit;
        """
        )

        credentials = pika.PlainCredentials("root", "clickhouse")
        parameters = pika.ConnectionParameters(
            rabbitmq_cluster.rabbitmq_ip,
            rabbitmq_cluster.rabbitmq_port,
            "/",
            credentials,
        )
        consumer_connection = pika.BlockingConnection(parameters)

        consumer = consumer_connection.channel()
        result = consumer.queue_declare(queue="")
        queue_name = result.method.queue
        consumer.queue_bind(
            exchange=format_name, queue=queue_name, routing_key=format_name
        )

        instance.query(
            f"INSERT INTO test.rabbit SELECT number * 10 as key, number * 100 as value FROM numbers({num_rows});"
        )

        insert_messages = 0

        def onReceived(channel, method, properties, body):
            nonlocal insert_messages
            insert_messages += 1
            if insert_messages == 2:
                channel.stop_consuming()

        consumer.basic_consume(queue_name, onReceived)
        consumer.start_consuming()
        consumer_connection.close()

        assert insert_messages == 2

        deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
        rows = 0
        while time.monotonic() < deadline:
            rows = int(instance.query("SELECT count() FROM test.view"))
            if rows == num_rows:
                break
            time.sleep(0.05)
        else:
            pytest.fail(
                f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The number of rows did not match {num_rows}."
            )

        assert rows == num_rows

        expected = ""
        for i in range(num_rows):
            expected += str(i * 10) + "\t" + str(i * 100) + "\n"

        result = instance.query("SELECT * FROM test.view")
        assert result == expected


def test_block_based_formats_1(rabbitmq_cluster):
    instance.query(
        """
        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'PrettySpace',
                     rabbitmq_exchange_type = 'direct',
                     rabbitmq_max_block_size = 100,
                     rabbitmq_flush_interval_ms = 1000,
                     rabbitmq_routing_key_list = 'PrettySpace',
                     rabbitmq_format = 'PrettySpace';
    """
    )

    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    consumer_connection = pika.BlockingConnection(parameters)

    consumer = consumer_connection.channel()
    result = consumer.queue_declare(queue="")
    queue_name = result.method.queue
    consumer.queue_bind(
        exchange="PrettySpace", queue=queue_name, routing_key="PrettySpace"
    )

    instance.query(
        "INSERT INTO test.rabbitmq SELECT number * 10 as key, number * 100 as value FROM numbers(5) settings max_block_size=2, optimize_trivial_insert_select=0, output_format_pretty_color=1, output_format_pretty_row_numbers=0;"
    )
    insert_messages = []

    def onReceived(channel, method, properties, body):
        insert_messages.append(body.decode())
        if len(insert_messages) == 3:
            channel.stop_consuming()

    consumer.basic_consume(queue_name, onReceived)
    consumer.start_consuming()
    consumer_connection.close()

    assert len(insert_messages) == 3

    data = []
    for message in insert_messages:
        splitted = message.split("\n")
        assert splitted[0] == " \x1b[1mkey\x1b[0m   \x1b[1mvalue\x1b[0m"
        assert splitted[1] == ""
        assert splitted[-1] == ""
        data += [line.split() for line in splitted[2:-1]]

    assert data == [
        ["0", "0"],
        ["10", "100"],
        ["20", "200"],
        ["30", "300"],
        ["40", "400"],
    ]


def test_block_based_formats_2(rabbitmq_cluster):
    num_rows = 100

    for format_name in [
        "JSONColumns",
        "Native",
        "Arrow",
        "Parquet",
        "ORC",
        "JSONCompactColumns",
    ]:
        logging.debug(format_name)

        instance.query(
            f"""
            DROP TABLE IF EXISTS test.view;
            DROP TABLE IF EXISTS test.rabbit;

            CREATE TABLE test.rabbit (key UInt64, value UInt64)
                ENGINE = RabbitMQ
                SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                         rabbitmq_format = '{format_name}',
                         rabbitmq_exchange_name = '{format_name}',
                         rabbitmq_exchange_type = 'direct',
                         rabbitmq_max_block_size = 100,
                         rabbitmq_flush_interval_ms = 1000,
                         rabbitmq_routing_key_list = '{format_name}';

            CREATE MATERIALIZED VIEW test.view Engine=Log AS
                SELECT key, value FROM test.rabbit;
        """
        )

        credentials = pika.PlainCredentials("root", "clickhouse")
        parameters = pika.ConnectionParameters(
            rabbitmq_cluster.rabbitmq_ip,
            rabbitmq_cluster.rabbitmq_port,
            "/",
            credentials,
        )
        consumer_connection = pika.BlockingConnection(parameters)

        consumer = consumer_connection.channel()
        result = consumer.queue_declare(queue="")
        queue_name = result.method.queue
        consumer.queue_bind(
            exchange=format_name, queue=queue_name, routing_key=format_name
        )

        instance.query(
            f"INSERT INTO test.rabbit SELECT number * 10 as key, number * 100 as value FROM numbers({num_rows}) settings max_block_size=12, optimize_trivial_insert_select=0;"
        )

        insert_messages = 0

        def onReceived(channel, method, properties, body):
            nonlocal insert_messages
            insert_messages += 1
            if insert_messages == 9:
                channel.stop_consuming()

        consumer.basic_consume(queue_name, onReceived)
        consumer.start_consuming()
        consumer_connection.close()

        assert insert_messages == 9

        deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
        rows = 0
        while time.monotonic() < deadline:
            rows = int(instance.query("SELECT count() FROM test.view"))
            if rows == num_rows:
                break
            time.sleep(0.05)
        else:
            pytest.fail(
                f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The number of rows did not match {num_rows}."
            )

        assert rows == num_rows

        result = instance.query("SELECT * FROM test.view ORDER by key")
        expected = ""
        for i in range(num_rows):
            expected += str(i * 10) + "\t" + str(i * 100) + "\n"
        assert result == expected


def test_rabbitmq_flush_by_block_size(rabbitmq_cluster):
    instance.query(
        """
         DROP TABLE IF EXISTS test.view;
         DROP TABLE IF EXISTS test.consumer;

         CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
             ENGINE = RabbitMQ
             SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                      rabbitmq_exchange_name = 'flush_by_block',
                      rabbitmq_queue_base = 'flush_by_block',
                      rabbitmq_max_block_size = 100,
                      rabbitmq_flush_interval_ms = 640000, /* should not flush by time during test */
                      rabbitmq_format = 'JSONEachRow';

         CREATE TABLE test.view (key UInt64, value UInt64)
             ENGINE = MergeTree()
             ORDER BY key;

         CREATE MATERIALIZED VIEW test.consumer TO test.view AS
             SELECT * FROM test.rabbitmq;

        SYSTEM STOP MERGES;
     """
    )

    cancel = threading.Event()

    def produce():
        credentials = pika.PlainCredentials("root", "clickhouse")
        parameters = pika.ConnectionParameters(
            rabbitmq_cluster.rabbitmq_ip,
            rabbitmq_cluster.rabbitmq_port,
            "/",
            credentials,
        )
        connection = pika.BlockingConnection(parameters)

        while not cancel.is_set():
            try:
                channel = connection.channel()
                channel.basic_publish(
                    exchange="flush_by_block",
                    routing_key="",
                    body=json.dumps({"key": 0, "value": 0}),
                )
            except Exception as e:
                logging.debug(f"Got error: {str(e)}")

    produce_thread = threading.Thread(target=produce)
    produce_thread.start()

    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        if (
            int(
                instance.query(
                    "SELECT count() FROM system.parts WHERE database = 'test' AND table = 'view' AND name = 'all_1_1_0'"
                )
            )
            != 0
        ):
            break
        time.sleep(0.5)
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The part 'all_1_1_0' is still missing."
        )

    cancel.set()
    produce_thread.join()

    # more flushes can happens during test, we need to check only result of first flush (part named all_1_1_0).
    result = instance.query("SELECT count() FROM test.view WHERE _part='all_1_1_0'")
    # logging.debug(result)

    instance.query(
        """
         DROP TABLE test.consumer;
         DROP TABLE test.view;
         DROP TABLE test.rabbitmq;
     """
    )

    # 100 = first poll should return 100 messages (and rows)
    # not waiting for stream_flush_interval_ms
    assert (
        int(result) == 100
    ), "Messages from rabbitmq should be flushed when block of size rabbitmq_max_block_size is formed!"


def test_rabbitmq_flush_by_time(rabbitmq_cluster):
    instance.query(
        """
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.consumer;

        CREATE TABLE test.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = 'flush_by_time',
                     rabbitmq_queue_base = 'flush_by_time',
                     rabbitmq_max_block_size = 100,
                     rabbitmq_flush_interval_ms = 5000,
                     rabbitmq_format = 'JSONEachRow';

        CREATE TABLE test.view (key UInt64, value UInt64, ts DateTime64(3) MATERIALIZED now64(3))
            ENGINE = MergeTree()
            ORDER BY key;
    """
    )

    cancel = threading.Event()

    def produce():
        credentials = pika.PlainCredentials("root", "clickhouse")
        parameters = pika.ConnectionParameters(
            rabbitmq_cluster.rabbitmq_ip,
            rabbitmq_cluster.rabbitmq_port,
            "/",
            credentials,
        )
        connection = pika.BlockingConnection(parameters)

        while not cancel.is_set():
            try:
                channel = connection.channel()
                channel.basic_publish(
                    exchange="flush_by_time",
                    routing_key="",
                    body=json.dumps({"key": 0, "value": 0}),
                )
                logging.debug("Produced a message")
                time.sleep(0.8)
            except Exception as e:
                logging.debug(f"Got error: {str(e)}")

    produce_thread = threading.Thread(target=produce)
    produce_thread.start()

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.rabbitmq;
    """
    )

    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        time.sleep(0.2)
        total_count = instance.query(
            "SELECT count() FROM system.parts WHERE database = 'test' AND table = 'view'"
        )
        logging.debug(f"kssenii total count: {total_count}")
        count = int(
            instance.query(
                "SELECT count() FROM system.parts WHERE database = 'test' AND table = 'view' AND name = 'all_1_1_0'"
            )
        )
        logging.debug(f"kssenii count: {count}")
        if count > 0:
            break
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The part 'all_1_1_0' is still missing."
        )

    time.sleep(12)
    result = instance.query("SELECT uniqExact(ts) FROM test.view")

    cancel.set()
    produce_thread.join()

    instance.query(
        """
        DROP TABLE test.consumer;
        DROP TABLE test.view;
        DROP TABLE test.rabbitmq;
    """
    )

    assert int(result) == 3


def test_rabbitmq_handle_error_mode_stream(rabbitmq_cluster):
    instance.query(
        """
        DROP TABLE IF EXISTS test.rabbitmq;
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.data;
        DROP TABLE IF EXISTS test.errors;
        DROP TABLE IF EXISTS test.errors_view;

        CREATE TABLE test.rabbit (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = '{}:5672',
                     rabbitmq_exchange_name = 'select',
                     rabbitmq_commit_on_select = 1,
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_row_delimiter = '\\n',
                     rabbitmq_handle_error_mode = 'stream';


        CREATE TABLE test.errors (error Nullable(String), broken_message Nullable(String))
             ENGINE = MergeTree()
             ORDER BY tuple();

        CREATE MATERIALIZED VIEW test.errors_view TO test.errors AS
                SELECT _error as error, _raw_message as broken_message FROM test.rabbit where not isNull(_error);

        CREATE TABLE test.data (key UInt64, value UInt64)
             ENGINE = MergeTree()
             ORDER BY key;

        CREATE MATERIALIZED VIEW test.view TO test.data AS
                SELECT key, value FROM test.rabbit;
        """.format(
            rabbitmq_cluster.rabbitmq_host
        )
    )

    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    messages = []
    num_rows = 50
    for i in range(num_rows):
        if i % 2 == 0:
            messages.append(json.dumps({"key": i, "value": i}))
        else:
            messages.append("Broken message " + str(i))

    for message in messages:
        channel.basic_publish(exchange="select", routing_key="", body=message)

    connection.close()
    # The order of messages in select * from test.rabbitmq is not guaranteed, so sleep to collect everything in one select
    time.sleep(1)

    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    rows = 0
    while time.monotonic() < deadline:
        rows = int(instance.query("SELECT count() FROM test.data"))
        if rows == num_rows:
            break
        time.sleep(0.05)
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The number of rows did not match {num_rows}."
        )

    assert rows == num_rows

    result = instance.query("SELECT * FROM test.data ORDER by key")
    expected = "0\t0\n" * (num_rows // 2)
    for i in range(num_rows):
        if i % 2 == 0:
            expected += str(i) + "\t" + str(i) + "\n"

    assert result == expected

    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    errors_count = 0
    while time.monotonic() < deadline:
        errors_count = int(instance.query("SELECT count() FROM test.errors"))
        if errors_count == num_rows / 2:
            break
        time.sleep(0.05)
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The number of errors ({errors_count}) did not match the expected {num_rows}."
        )

    assert errors_count == num_rows / 2

    broken_messages = instance.query(
        "SELECT broken_message FROM test.errors order by broken_message"
    )
    expected = []
    for i in range(num_rows):
        if i % 2 != 0:
            expected.append("Broken message " + str(i) + "\n")

    expected = "".join(sorted(expected))
    assert broken_messages == expected


def test_attach_broken_table(rabbitmq_cluster):
    instance.query(
        f"ATTACH TABLE rabbit_queue UUID '{uuid.uuid4()}' (`payload` String) ENGINE = RabbitMQ SETTINGS rabbitmq_host_port = 'nonexisting:5671', rabbitmq_format = 'JSONEachRow', rabbitmq_username = 'test', rabbitmq_password = 'test'"
    )

    error = instance.query_and_get_error("SELECT * FROM rabbit_queue")
    assert "CANNOT_CONNECT_RABBITMQ" in error
    error = instance.query_and_get_error("INSERT INTO rabbit_queue VALUES ('test')")
    assert "CANNOT_CONNECT_RABBITMQ" in error


def test_rabbitmq_nack_failed_insert(rabbitmq_cluster):
    table_name = "nack_failed_insert"
    exchange = f"{table_name}_exchange"

    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    channel.exchange_declare(exchange="deadl")

    result = channel.queue_declare(queue="deadq")
    queue_name = result.method.queue
    channel.queue_bind(exchange="deadl", routing_key="", queue=queue_name)

    instance3.query(
        f"""
        CREATE TABLE test.{table_name} (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = '{rabbitmq_cluster.rabbitmq_host}:5672',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_exchange_name = '{exchange}',
                     rabbitmq_format = 'JSONEachRow',
                    rabbitmq_queue_settings_list='x-dead-letter-exchange=deadl';

        DROP TABLE IF EXISTS test.view;
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;

        DROP TABLE IF EXISTS test.consumer;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.{table_name};
        """
    )

    num_rows = 25
    for i in range(num_rows):
        message = json.dumps({"key": i, "value": i}) + "\n"
        channel.basic_publish(exchange=exchange, routing_key="", body=message)

    instance3.wait_for_log_line(
        "Failed to push to views. Error: Code: 252. DB::Exception: Too many parts"
    )

    instance3.replace_in_config(
        "/etc/clickhouse-server/config.d/mergetree.xml",
        "parts_to_throw_insert>0",
        "parts_to_throw_insert>10",
    )
    instance3.restart_clickhouse()

    count = [0]

    def on_consume(channel, method, properties, body):
        channel.basic_publish(exchange=exchange, routing_key="", body=body)
        count[0] += 1
        if count[0] == num_rows:
            channel.stop_consuming()

    channel.basic_consume(queue_name, on_consume)
    channel.start_consuming()

    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    count = 0
    while time.monotonic() < deadline:
        count = int(instance3.query("SELECT count() FROM test.view"))
        if count == num_rows:
            break
        time.sleep(0.05)
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The count did not match {num_rows}."
        )

    assert count == num_rows

    instance3.query(
        f"""
        DROP TABLE test.consumer;
        DROP TABLE test.view;
        DROP TABLE test.{table_name};
    """
    )
    connection.close()


def test_rabbitmq_reject_broken_messages(rabbitmq_cluster):
    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    deadletter_exchange = "deadletter_exchange_handle_error_mode_stream"
    deadletter_queue = "deadletter_queue_handle_error_mode_stream"
    channel.exchange_declare(exchange=deadletter_exchange)

    result = channel.queue_declare(queue=deadletter_queue)
    channel.queue_bind(
        exchange=deadletter_exchange, routing_key="", queue=deadletter_queue
    )

    instance.query(
        f"""
        DROP TABLE IF EXISTS test.rabbitmq;
        DROP TABLE IF EXISTS test.view;
        DROP TABLE IF EXISTS test.data;
        DROP TABLE IF EXISTS test.errors;
        DROP TABLE IF EXISTS test.errors_view;

        CREATE TABLE test.rabbit (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = '{rabbitmq_cluster.rabbitmq_host}:5672',
                     rabbitmq_exchange_name = 'select',
                     rabbitmq_commit_on_select = 1,
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_row_delimiter = '\\n',
                     rabbitmq_handle_error_mode = 'stream',
                     rabbitmq_queue_settings_list='x-dead-letter-exchange={deadletter_exchange}';


        CREATE TABLE test.errors (error Nullable(String), broken_message Nullable(String))
             ENGINE = MergeTree()
             ORDER BY tuple();

        CREATE MATERIALIZED VIEW test.errors_view TO test.errors AS
                SELECT _error as error, _raw_message as broken_message FROM test.rabbit where not isNull(_error);

        CREATE TABLE test.data (key UInt64, value UInt64)
             ENGINE = MergeTree()
             ORDER BY key;

        CREATE MATERIALIZED VIEW test.view TO test.data AS
                SELECT key, value FROM test.rabbit;
        """
    )

    messages = []
    num_rows = 50
    for i in range(num_rows):
        if i % 2 == 0:
            messages.append(json.dumps({"key": i, "value": i}))
        else:
            messages.append("Broken message " + str(i))

    for message in messages:
        channel.basic_publish(exchange="select", routing_key="", body=message)

    time.sleep(1)

    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    rows = 0
    while time.monotonic() < deadline:
        rows = int(instance.query("SELECT count() FROM test.data"))
        if rows == num_rows:
            break
        time.sleep(1)
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The number of rows did not match {num_rows}."
        )

    assert rows == num_rows

    dead_letters = []

    def on_dead_letter(channel, method, properties, body):
        dead_letters.append(body)
        if len(dead_letters) == num_rows / 2:
            channel.stop_consuming()

    channel.basic_consume(deadletter_queue, on_dead_letter)
    channel.start_consuming()

    assert len(dead_letters) == num_rows / 2

    i = 1
    for letter in dead_letters:
        assert f"Broken message {i}" in str(letter)
        i += 2

    connection.close()
