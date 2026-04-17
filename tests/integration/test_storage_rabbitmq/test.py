import json
import logging
import re
import random
import threading
import time
import uuid
from random import randrange

import pika
import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

pytestmark = pytest.mark.timeout(1200)

DEFAULT_TIMEOUT_SEC = 60

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=[
        "configs/rabbitmq.xml",
        "configs/macros.xml",
        "configs/named_collection.xml",
        "configs/dead_letter_queue.xml",
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

# Helpers


def _test_db_name(request):
    """Generate a unique database name from the test function name."""
    name = request.node.name
    return "db_" + re.sub(r'[\[\]\-]', '_', name)


def _test_unique(request):
    """Generate a unique prefix for exchange/queue names."""
    name = request.node.originalname or request.node.name
    return re.sub(r'[\[\]\-]', '_', name)


def rabbitmq_check_result(result, check=False, reference=None):
    if reference is None:
        reference = "\n".join([f"{i}\t{i}" for i in range(50)])
    if check:
        assert TSV(result) == TSV(reference)
    else:
        return TSV(result) == TSV(reference)


# Fixtures


@pytest.fixture(scope="module")
def rabbitmq_cluster():
    try:
        cluster.start()
        logging.debug("rabbitmq_id is {}".format(instance.cluster.rabbitmq_docker_id))
        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture()
def db(request):
    """Per-test unique database."""
    name = _test_db_name(request)
    instance.query(f"CREATE DATABASE {name}")
    yield name
    instance.query(f"DROP DATABASE {name} SYNC")


@pytest.fixture()
def unique(request):
    """Per-test unique prefix for exchange/queue names."""
    return _test_unique(request)


def check_expected_result_polling(expected, query, instance=instance, timeout=DEFAULT_TIMEOUT_SEC):
    deadline = time.monotonic() + timeout
    prev_result = 0
    result = None
    while time.monotonic() < deadline:
        result = int(instance.query(query))
        # In case it's consuming successfully from RabbitMQ in latest iteration, extend the deadline
        if result > prev_result:
            deadline += 1
        prev_result = result
        if result == expected:
            break
        logging.debug(f"Result: {result} / {expected}. Now {time.monotonic()}, deadline {deadline}")
        time.sleep(1)
    else:
        pytest.fail(
            f"Time limit of {timeout} seconds reached without any RabbitMQ consumption. The result did not match the expected value."
        )
    return result


# Tests


@pytest.mark.parametrize(
    "secure",
    [
        pytest.param(0),
        pytest.param(1),
    ],
)
def test_rabbitmq_select(rabbitmq_cluster, secure, db, unique):
    if secure and instance.is_built_with_thread_sanitizer():
        pytest.skip(
            "Data races: see https://github.com/ClickHouse/ClickHouse/issues/56866"
        )

    port = cluster.rabbitmq_port
    if secure:
        port = cluster.rabbitmq_secure_port

    # MATERIALIZED and ALIAS columns are not supported in RabbitMQ engine, but we can test that it does not fail
    instance.query(
        f"""
        CREATE TABLE {db}.rabbitmq (key UInt64, value UInt64, value2 ALIAS value + 1, value3 MATERIALIZED value + 1)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = '{rabbitmq_cluster.rabbitmq_host}:{port}',
                     rabbitmq_exchange_name = '{unique}_select',
                     rabbitmq_commit_on_select = 1,
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_row_delimiter = '\\n',
                     rabbitmq_secure = {secure};
        """
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
        channel.basic_publish(exchange=f"{unique}_select", routing_key="", body=message)

    connection.close()
    # The order of messages in select * from {db}.rabbitmq is not guaranteed, so sleep to collect everything in one select
    time.sleep(1)

    result = ""
    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        result += instance.query(
            f"SELECT * FROM {db}.rabbitmq ORDER BY key", ignore_error=True
        )
        if rabbitmq_check_result(result):
            break
        time.sleep(0.05)
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The result did not match the expected value."
        )

    rabbitmq_check_result(result, True)


def test_rabbitmq_select_empty(rabbitmq_cluster, db, unique):
    instance.query(
        f"""
        CREATE TABLE {db}.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = '{rabbitmq_cluster.rabbitmq_host}:5672',
                     rabbitmq_exchange_name = '{unique}_empty',
                     rabbitmq_commit_on_select = 1,
                     rabbitmq_format = 'TSV',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_row_delimiter = '\\n';
        """
    )

    assert int(instance.query(f"SELECT count() FROM {db}.rabbitmq")) == 0


def test_rabbitmq_json_without_delimiter(rabbitmq_cluster, db, unique):
    instance.query(
        f"""
        CREATE TABLE {db}.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = '{rabbitmq_cluster.rabbitmq_host}:5672',
                     rabbitmq_commit_on_select = 1,
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_max_block_size=100,
                     rabbitmq_exchange_name = '{unique}_json',
                     rabbitmq_format = 'JSONEachRow'
        """
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
        channel.basic_publish(exchange=f"{unique}_json", routing_key="", body=message)

    messages = ""
    for i in range(25, 50):
        messages += json.dumps({"key": i, "value": i}) + "\n"
    all_messages = [messages]
    for message in all_messages:
        channel.basic_publish(exchange=f"{unique}_json", routing_key="", body=message)

    connection.close()
    time.sleep(1)

    result = ""
    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        result += instance.query(
            f"SELECT * FROM {db}.rabbitmq ORDER BY key", ignore_error=True
        )
        if rabbitmq_check_result(result):
            break

        time.sleep(0.05)
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The result did not match the expected value."
        )

    rabbitmq_check_result(result, True)


def test_rabbitmq_csv_with_delimiter(rabbitmq_cluster, db, unique):
    instance.query(
        f"""
        CREATE TABLE {db}.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = '{unique}_csv',
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
        messages.append(f"{i}, {i}")

    for message in messages:
        channel.basic_publish(exchange=f"{unique}_csv", routing_key="", body=message)

    connection.close()
    time.sleep(1)

    result = ""
    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        result += instance.query(
            f"SELECT * FROM {db}.rabbitmq ORDER BY key", ignore_error=True
        )
        if rabbitmq_check_result(result):
            break

        time.sleep(0.05)
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The result did not match the expected value."
        )

    rabbitmq_check_result(result, True)


def test_rabbitmq_tsv_with_delimiter(rabbitmq_cluster, db, unique):
    instance.query(
        f"""
        CREATE TABLE {db}.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = '{unique}_tsv',
                     rabbitmq_format = 'TSV',
                     rabbitmq_commit_on_select = 1,
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_max_block_size=100,
                     rabbitmq_queue_base = '{unique}_tsv',
                     rabbitmq_row_delimiter = '\\n';
        CREATE TABLE {db}.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW {db}.consumer TO {db}.view AS
            SELECT * FROM {db}.rabbitmq;
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
        messages.append(f"{i}\t{i}")

    for message in messages:
        channel.basic_publish(exchange=f"{unique}_tsv", routing_key="", body=message)
    connection.close()

    result = ""
    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        result = instance.query(f"SELECT * FROM {db}.view ORDER BY key")
        if rabbitmq_check_result(result):
            break
        time.sleep(0.05)
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The result did not match the expected value."
        )

    rabbitmq_check_result(result, True)


def test_rabbitmq_macros(rabbitmq_cluster, db, unique):
    # Uses ClickHouse macros for host/port/format (defined in macros.xml).
    # Exchange name uses unique prefix directly since macros can't be dynamic per-test.
    instance.query(
        f"""
        CREATE TABLE {db}.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = '{{rabbitmq_host}}:{{rabbitmq_port}}',
                     rabbitmq_commit_on_select = 1,
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_max_block_size=100,
                     rabbitmq_exchange_name = '{unique}_macro',
                     rabbitmq_format = '{{rabbitmq_format}}'
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
    channel.basic_publish(exchange=f"{unique}_macro", routing_key="", body=message)

    connection.close()
    time.sleep(1)

    result = ""
    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        result += instance.query(
            f"SELECT * FROM {db}.rabbitmq ORDER BY key", ignore_error=True
        )
        if rabbitmq_check_result(result):
            break
        time.sleep(0.05)
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The result did not match the expected value."
        )

    rabbitmq_check_result(result, True)


def test_rabbitmq_materialized_view(rabbitmq_cluster, db, unique):
    instance.query(
        f"""
        CREATE TABLE {db}.rabbitmq (key UInt64, value UInt64, dt1 DateTime MATERIALIZED now(), value2 ALIAS value + 1)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = '{unique}_mv',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_max_block_size=100,
                     rabbitmq_row_delimiter = '\\n';
        CREATE TABLE {db}.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW {db}.consumer TO {db}.view AS
            SELECT * FROM {db}.rabbitmq;

        CREATE TABLE {db}.view2 (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW {db}.consumer2 TO {db}.view2 AS
            SELECT * FROM {db}.rabbitmq group by (key, value);
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
        channel.basic_publish(exchange=f"{unique}_mv", routing_key="", body=message)

    time_limit_sec = 60
    deadline = time.monotonic() + time_limit_sec

    while time.monotonic() < deadline:
        result = instance.query(f"SELECT * FROM {db}.view ORDER BY key")
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
        result = instance.query(f"SELECT * FROM {db}.view2 ORDER BY key")
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


def test_rabbitmq_materialized_view_with_subquery(rabbitmq_cluster, db, unique):
    instance.query(
        f"""
        CREATE TABLE {db}.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = '{unique}_mvsq',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_max_block_size=100,
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_row_delimiter = '\\n';
        CREATE TABLE {db}.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW {db}.consumer TO {db}.view AS
            SELECT * FROM (SELECT * FROM {db}.rabbitmq);
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
        channel.basic_publish(exchange=f"{unique}_mvsq", routing_key="", body=message)

    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC

    while time.monotonic() < deadline:
        result = instance.query(f"SELECT * FROM {db}.view ORDER BY key")
        if rabbitmq_check_result(result):
            break
        time.sleep(0.05)
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The result did not match the expected value."
        )

    connection.close()
    rabbitmq_check_result(result, True)


def test_rabbitmq_many_materialized_views(rabbitmq_cluster, db, unique):
    instance.query(
        f"""
        SET allow_materialized_view_with_bad_select = true;
        DROP TABLE IF EXISTS {db}.view1;
        DROP TABLE IF EXISTS {db}.view2;
        DROP TABLE IF EXISTS {db}.view3;
        DROP TABLE IF EXISTS {db}.consumer1;
        DROP TABLE IF EXISTS {db}.consumer2;
        DROP TABLE IF EXISTS {db}.consumer3;
        CREATE TABLE {db}.rabbitmq (key UInt64, value UInt64, value2 ALIAS value + 1, value3 MATERIALIZED value + 1, value4 DEFAULT 1)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = '{unique}_mmv',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_max_block_size=100,
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_row_delimiter = '\\n';
        CREATE TABLE {db}.view1 (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE TABLE {db}.view2 (key UInt64, value UInt64, value2 UInt64, value3 UInt64, value4 UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE TABLE {db}.view3 (key UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW {db}.consumer1 TO {db}.view1 AS
            SELECT * FROM {db}.rabbitmq;
        CREATE MATERIALIZED VIEW {db}.consumer2 TO {db}.view2 AS
            SELECT * FROM {db}.rabbitmq;
        CREATE MATERIALIZED VIEW {db}.consumer3 TO {db}.view3 AS
            SELECT * FROM {db}.rabbitmq;
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
        channel.basic_publish(exchange=f"{unique}_mmv", routing_key="", body=message)

    is_check_passed = False
    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        result1 = instance.query(f"SELECT * FROM {db}.view1 ORDER BY key")
        result2 = instance.query(f"SELECT * FROM {db}.view2 ORDER BY key")
        result3 = instance.query(f"SELECT * FROM {db}.view3 ORDER BY key")
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
        f"""
        DROP TABLE {db}.consumer1;
        DROP TABLE {db}.consumer2;
        DROP TABLE {db}.consumer3;
        DROP TABLE {db}.view1;
        DROP TABLE {db}.view2;
        DROP TABLE {db}.view3;
    """
    )

    connection.close()


def test_rabbitmq_big_message(rabbitmq_cluster, db, unique):
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
        f"""
        CREATE TABLE {db}.rabbitmq (key UInt64, value String)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = '{unique}_big',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_max_block_size=100,
                     rabbitmq_format = 'JSONEachRow';
        CREATE TABLE {db}.view (key UInt64, value String)
            ENGINE = MergeTree
            ORDER BY key;
        CREATE MATERIALIZED VIEW {db}.consumer TO {db}.view AS
            SELECT * FROM {db}.rabbitmq;
    """
    )

    for message in messages:
        channel.basic_publish(exchange=f"{unique}_big", routing_key="", body=message)

    check_expected_result_polling(batch_messages * rabbitmq_messages, f"SELECT count() FROM {db}.view")
    connection.close()


def test_rabbitmq_sharding_between_queues_publish(rabbitmq_cluster, db, unique):
    logging.getLogger("pika").propagate = False

    instance.query(
        f"""
        CREATE TABLE {db}.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = '{unique}_test_sharding',
                     rabbitmq_num_queues = 5,
                     rabbitmq_num_consumers = 10,
                     rabbitmq_max_block_size = 100,
                     rabbitmq_flush_interval_ms=500,
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_row_delimiter = '\\n';
        CREATE TABLE {db}.view (key UInt64, value UInt64, channel_id String)
            ENGINE = MergeTree
            ORDER BY key
            SETTINGS old_parts_lifetime=5, cleanup_delay_period=2, cleanup_delay_period_random_add=3,
            cleanup_thread_preferred_points_per_iteration=0;
        CREATE MATERIALIZED VIEW {db}.consumer TO {db}.view AS
            SELECT *, _channel_id AS channel_id FROM {db}.rabbitmq;
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
                exchange=f"{unique}_test_sharding",
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

    expected = messages_num * threads_num
    check_expected_result_polling(expected, f"SELECT count() FROM {db}.view")
    result2 = instance.query(f"SELECT count(DISTINCT channel_id) FROM {db}.view")

    for thread in threads:
        thread.join()

    assert int(result2) == 10


def test_rabbitmq_mv_combo(rabbitmq_cluster, db, unique):
    NUM_MV = 5
    logging.getLogger("pika").propagate = False

    instance.query(
        f"""
        CREATE TABLE {db}.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = '{unique}_combo',
                     rabbitmq_queue_base = '{unique}_combo',
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
            f"""
            DROP TABLE IF EXISTS {db}.combo_{mv_id};
            DROP TABLE IF EXISTS {db}.combo_{mv_id}_mv;
            CREATE TABLE {db}.combo_{mv_id} (key UInt64, value UInt64)
                ENGINE = MergeTree()
                ORDER BY key;
            CREATE MATERIALIZED VIEW {db}.combo_{mv_id}_mv TO {db}.combo_{mv_id} AS
                SELECT * FROM {db}.rabbitmq;
        """
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
                exchange=f"{unique}_combo",
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
                instance.query(f"SELECT count() FROM {db}.combo_{mv_id}")
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
            f"""
            DROP TABLE {db}.combo_{mv_id}_mv;
            DROP TABLE {db}.combo_{mv_id};
        """
        )

    assert (
        int(result) == messages_num * threads_num * NUM_MV
    ), "ClickHouse lost some messages: {}".format(result)


def test_rabbitmq_insert(rabbitmq_cluster, db, unique):
    instance.query(
        f"""
        CREATE TABLE {db}.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = '{unique}_insert',
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
    consumer.queue_bind(exchange=f"{unique}_insert", queue=queue_name, routing_key="insert1")

    values = []
    for i in range(50):
        values.append("({i}, {i})".format(i=i))
    values = ",".join(values)

    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        try:
            instance.query(f"INSERT INTO {db}.rabbitmq VALUES {values}")
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


def test_rabbitmq_insert_headers_exchange(rabbitmq_cluster, db, unique):
    instance.query(
        f"""
        CREATE TABLE {db}.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = '{unique}_insert_headers',
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
        exchange=f"{unique}_insert_headers",
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
            instance.query(f"INSERT INTO {db}.rabbitmq VALUES {values}")
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


def test_rabbitmq_many_inserts(rabbitmq_cluster, db, unique):
    instance.query(
        f"""
        DROP TABLE IF EXISTS {db}.rabbitmq_many;
        DROP TABLE IF EXISTS {db}.rabbitmq_consume;
        DROP TABLE IF EXISTS {db}.view_many;
        DROP TABLE IF EXISTS {db}.consumer_many;
        CREATE TABLE {db}.rabbitmq_many (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = '{unique}_many_inserts',
                     rabbitmq_exchange_type = 'direct',
                     rabbitmq_routing_key_list = 'insert2',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_max_block_size=100,
                     rabbitmq_format = 'TSV',
                     rabbitmq_row_delimiter = '\\n';
        CREATE TABLE {db}.rabbitmq_consume (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = '{unique}_many_inserts',
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
                    f"INSERT INTO {db}.rabbitmq_many VALUES {values}"
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
        f"""
        CREATE TABLE {db}.view_many (key UInt64, value UInt64)
            ENGINE = MergeTree
            ORDER BY key;
        CREATE MATERIALIZED VIEW {db}.consumer_many TO {db}.view_many AS
            SELECT * FROM {db}.rabbitmq_consume;
    """
    )

    for thread in threads:
        thread.join()

    check_expected_result_polling(messages_num * threads_num, f"SELECT count() FROM {db}.view_many")

    instance.query(
        f"""
        DROP TABLE {db}.rabbitmq_consume;
        DROP TABLE {db}.rabbitmq_many;
        DROP TABLE {db}.consumer_many;
        DROP TABLE {db}.view_many;
    """
    )


def test_rabbitmq_overloaded_insert(rabbitmq_cluster, db, unique):
    instance.query(
        f"""
        DROP TABLE IF EXISTS {db}.view_overload;
        DROP TABLE IF EXISTS {db}.consumer_overload;
        DROP TABLE IF EXISTS {db}.rabbitmq_consume;
        CREATE TABLE {db}.rabbitmq_consume (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = '{unique}_over',
                     rabbitmq_queue_base = '{unique}_over',
                     rabbitmq_exchange_type = 'direct',
                     rabbitmq_num_consumers = 2,
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_max_block_size = 100,
                     rabbitmq_routing_key_list = 'over',
                     rabbitmq_format = 'TSV',
                     rabbitmq_row_delimiter = '\\n';
        CREATE TABLE {db}.rabbitmq_overload (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = '{unique}_over',
                     rabbitmq_exchange_type = 'direct',
                     rabbitmq_routing_key_list = 'over',
                     rabbitmq_format = 'TSV',
                     rabbitmq_row_delimiter = '\\n';
        CREATE TABLE {db}.view_overload (key UInt64, value UInt64)
            ENGINE = MergeTree
            ORDER BY key;
        CREATE MATERIALIZED VIEW {db}.consumer_overload TO {db}.view_overload AS
            SELECT * FROM {db}.rabbitmq_consume;
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
                    f"INSERT INTO {db}.rabbitmq_overload VALUES {values}"
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

    check_expected_result_polling(messages_num * threads_num, f"SELECT count() FROM {db}.view_overload")

    instance.query(
        f"""
        DROP TABLE {db}.consumer_overload SYNC;
        DROP TABLE {db}.view_overload SYNC;
        DROP TABLE {db}.rabbitmq_consume SYNC;
        DROP TABLE {db}.rabbitmq_overload SYNC;
    """
    )


def test_rabbitmq_direct_exchange(rabbitmq_cluster, db, unique):
    instance.query(
        f"""
        DROP TABLE IF EXISTS {db}.destination;
        CREATE TABLE {db}.destination(key UInt64, value UInt64)
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
            f"""
            DROP TABLE IF EXISTS {db}.direct_exchange_{consumer_id};
            DROP TABLE IF EXISTS {db}.direct_exchange_{consumer_id}_mv;
            CREATE TABLE {db}.direct_exchange_{consumer_id} (key UInt64, value UInt64)
                ENGINE = RabbitMQ
                SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                         rabbitmq_num_consumers = 2,
                         rabbitmq_num_queues = 2,
                         rabbitmq_flush_interval_ms=1000,
                        rabbitmq_max_block_size=100,
                         rabbitmq_exchange_name = '{unique}_direct_exchange_testing',
                         rabbitmq_exchange_type = 'direct',
                         rabbitmq_routing_key_list = 'direct_{consumer_id}',
                         rabbitmq_format = 'JSONEachRow',
                         rabbitmq_row_delimiter = '\\n';
            CREATE MATERIALIZED VIEW {db}.direct_exchange_{consumer_id}_mv TO {db}.destination AS
            SELECT key, value FROM {db}.direct_exchange_{consumer_id};
        """
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
                exchange=f"{unique}_direct_exchange_testing",
                routing_key=key,
                properties=pika.BasicProperties(message_id=mes_id),
                body=message,
            )

    connection.close()

    check_expected_result_polling(messages_num * num_tables, f"SELECT count() FROM {db}.destination")

    for consumer_id in range(num_tables):
        instance.query(
            f"""
            DROP TABLE {db}.direct_exchange_{consumer_id}_mv;
            DROP TABLE {db}.direct_exchange_{consumer_id};
        """
        )

    instance.query(
        f"""
        DROP TABLE IF EXISTS {db}.destination;
    """
    )


def test_rabbitmq_fanout_exchange(rabbitmq_cluster, db, unique):
    instance.query(
        f"""
        DROP TABLE IF EXISTS {db}.destination;
        CREATE TABLE {db}.destination(key UInt64, value UInt64)
        ENGINE = MergeTree()
        ORDER BY key;
    """
    )

    num_tables = 5
    for consumer_id in range(num_tables):
        logging.debug(("Setting up table {}".format(consumer_id)))
        instance.query(
            f"""
            DROP TABLE IF EXISTS {db}.fanout_exchange_{consumer_id};
            DROP TABLE IF EXISTS {db}.fanout_exchange_{consumer_id}_mv;
            CREATE TABLE {db}.fanout_exchange_{consumer_id} (key UInt64, value UInt64)
                ENGINE = RabbitMQ
                SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                         rabbitmq_num_consumers = 2,
                         rabbitmq_num_queues = 2,
                         rabbitmq_flush_interval_ms=1000,
                         rabbitmq_max_block_size=100,
                         rabbitmq_routing_key_list = 'key_{consumer_id}',
                         rabbitmq_exchange_name = '{unique}_fanout_exchange_testing',
                         rabbitmq_exchange_type = 'fanout',
                         rabbitmq_format = 'JSONEachRow',
                         rabbitmq_row_delimiter = '\\n';
            CREATE MATERIALIZED VIEW {db}.fanout_exchange_{consumer_id}_mv TO {db}.destination AS
            SELECT key, value FROM {db}.fanout_exchange_{consumer_id};
        """
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
            exchange=f"{unique}_fanout_exchange_testing",
            routing_key="",
            properties=pika.BasicProperties(message_id=str(msg_id)),
            body=messages[msg_id],
        )

    connection.close()

    check_expected_result_polling(messages_num * num_tables, f"SELECT count() FROM {db}.destination")

    for consumer_id in range(num_tables):
        instance.query(
            f"""
            DROP TABLE {db}.fanout_exchange_{consumer_id}_mv;
            DROP TABLE {db}.fanout_exchange_{consumer_id};
        """
        )

    instance.query(
        f"""
        DROP TABLE {db}.destination;
    """
    )


def test_rabbitmq_topic_exchange(rabbitmq_cluster, db, unique):
    instance.query(
        f"""
        DROP TABLE IF EXISTS {db}.destination;
        CREATE TABLE {db}.destination(key UInt64, value UInt64)
        ENGINE = MergeTree()
        ORDER BY key;
    """
    )

    num_tables = 5
    for consumer_id in range(num_tables):
        logging.debug(("Setting up table {}".format(consumer_id)))
        instance.query(
            f"""
            DROP TABLE IF EXISTS {db}.topic_exchange_{consumer_id};
            DROP TABLE IF EXISTS {db}.topic_exchange_{consumer_id}_mv;
            CREATE TABLE {db}.topic_exchange_{consumer_id} (key UInt64, value UInt64)
                ENGINE = RabbitMQ
                SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                         rabbitmq_num_consumers = 2,
                         rabbitmq_num_queues = 2,
                         rabbitmq_flush_interval_ms=1000,
                         rabbitmq_max_block_size=100,
                         rabbitmq_exchange_name = '{unique}_topic_exchange_testing',
                         rabbitmq_exchange_type = 'topic',
                         rabbitmq_routing_key_list = '*.{consumer_id}',
                         rabbitmq_format = 'JSONEachRow',
                         rabbitmq_row_delimiter = '\\n';
            CREATE MATERIALIZED VIEW {db}.topic_exchange_{consumer_id}_mv TO {db}.destination AS
            SELECT key, value FROM {db}.topic_exchange_{consumer_id};
        """
        )

    for consumer_id in range(num_tables):
        logging.debug(("Setting up table {}".format(num_tables + consumer_id)))
        instance.query(
            f"""
            DROP TABLE IF EXISTS {db}.topic_exchange_{num_tables + consumer_id};
            DROP TABLE IF EXISTS {db}.topic_exchange_{num_tables + consumer_id}_mv;
            CREATE TABLE {db}.topic_exchange_{num_tables + consumer_id} (key UInt64, value UInt64)
                ENGINE = RabbitMQ
                SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                         rabbitmq_num_consumers = 2,
                         rabbitmq_num_queues = 2,
                         rabbitmq_flush_interval_ms=1000,
                         rabbitmq_max_block_size=100,
                         rabbitmq_exchange_name = '{unique}_topic_exchange_testing',
                         rabbitmq_exchange_type = 'topic',
                         rabbitmq_routing_key_list = '*.logs',
                         rabbitmq_format = 'JSONEachRow',
                         rabbitmq_row_delimiter = '\\n';
            CREATE MATERIALIZED VIEW {db}.topic_exchange_{num_tables + consumer_id}_mv TO {db}.destination AS
            SELECT key, value FROM {db}.topic_exchange_{num_tables + consumer_id};
        """
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
                exchange=f"{unique}_topic_exchange_testing", routing_key=key, body=message
            )

    key = "random.logs"
    for msg_id in range(messages_num):
        channel.basic_publish(
            exchange=f"{unique}_topic_exchange_testing",
            routing_key=key,
            properties=pika.BasicProperties(message_id=str(msg_id)),
            body=messages[msg_id],
        )

    connection.close()

    check_expected_result_polling(messages_num * num_tables + messages_num * num_tables, f"SELECT count() FROM {db}.destination")

    for consumer_id in range(num_tables * 2):
        instance.query(
            f"""
            DROP TABLE {db}.topic_exchange_{consumer_id}_mv;
            DROP TABLE {db}.topic_exchange_{consumer_id};
        """
        )

    instance.query(
        f"""
        DROP TABLE {db}.destination;
    """
    )


def test_rabbitmq_hash_exchange(rabbitmq_cluster, db, unique):
    instance.query(
        f"""
        DROP TABLE IF EXISTS {db}.destination;
        CREATE TABLE {db}.destination(key UInt64, value UInt64, channel_id String)
        ENGINE = MergeTree()
        ORDER BY key;
    """
    )

    num_tables = 4
    for consumer_id in range(num_tables):
        table_name = "rabbitmq_consumer{}".format(consumer_id)
        logging.debug(("Setting up {}".format(table_name)))
        instance.query(
            f"""
            DROP TABLE IF EXISTS {db}.{table_name};
            DROP TABLE IF EXISTS {db}.{table_name}_mv;
            CREATE TABLE {db}.{table_name} (key UInt64, value UInt64)
                ENGINE = RabbitMQ
                SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                         rabbitmq_num_consumers = 4,
                         rabbitmq_num_queues = 2,
                         rabbitmq_exchange_type = 'consistent_hash',
                         rabbitmq_exchange_name = '{unique}_hash_exchange_testing',
                         rabbitmq_format = 'JSONEachRow',
                         rabbitmq_flush_interval_ms=1000,
                         rabbitmq_row_delimiter = '\\n';
            CREATE MATERIALIZED VIEW {db}.{table_name}_mv TO {db}.destination AS
                SELECT key, value, _channel_id AS channel_id FROM {db}.{table_name};
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
        for msg_id in range(messages_num):
            channel.basic_publish(
                exchange=f"{unique}_hash_exchange_testing",
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

    check_expected_result_polling(messages_num * threads_num, f"SELECT count() FROM {db}.destination")
    result2 = instance.query(f"SELECT count(DISTINCT channel_id) FROM {db}.destination")

    for consumer_id in range(num_tables):
        table_name = "rabbitmq_consumer{}".format(consumer_id)
        instance.query(
            f"""
            DROP TABLE {db}.{table_name}_mv;
            DROP TABLE {db}.{table_name};
        """
        )

    instance.query(
        f"""
        DROP TABLE {db}.destination;
    """
    )

    for thread in threads:
        thread.join()

    assert int(result2) == 4 * num_tables


def test_rabbitmq_multiple_bindings(rabbitmq_cluster, db, unique):
    instance.query(
        f"""
        DROP TABLE IF EXISTS {db}.destination;
        CREATE TABLE {db}.destination(key UInt64, value UInt64)
        ENGINE = MergeTree()
        ORDER BY key;
    """
    )

    instance.query(
        f"""
        DROP TABLE IF EXISTS {db}.bindings;
        DROP TABLE IF EXISTS {db}.bindings_mv;
        CREATE TABLE {db}.bindings (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = '{unique}_multiple_bindings_testing',
                     rabbitmq_exchange_type = 'direct',
                     rabbitmq_routing_key_list = 'key1,key2,key3,key4,key5',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_max_block_size=100,
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_row_delimiter = '\\n';
        CREATE MATERIALIZED VIEW {db}.bindings_mv TO {db}.destination AS
            SELECT * FROM {db}.bindings;
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
                    exchange=f"{unique}_multiple_bindings_testing", routing_key=key, body=message
                )

        connection.close()

    threads = []
    threads_num = 10

    for _ in range(threads_num):
        threads.append(threading.Thread(target=produce))
    for thread in threads:
        time.sleep(random.uniform(0, 1))
        thread.start()

    check_expected_result_polling(messages_num * threads_num * 5, f"SELECT count() FROM {db}.destination")

    for thread in threads:
        thread.join()

    instance.query(
        f"""
        DROP TABLE {db}.bindings;
        DROP TABLE {db}.bindings_mv;
        DROP TABLE {db}.destination;
    """
    )


def test_rabbitmq_headers_exchange(rabbitmq_cluster, db, unique):
    instance.query(
        f"""
        DROP TABLE IF EXISTS {db}.destination;
        CREATE TABLE {db}.destination(key UInt64, value UInt64)
        ENGINE = MergeTree()
        ORDER BY key;
    """
    )

    num_tables_to_receive = 2
    for consumer_id in range(num_tables_to_receive):
        logging.debug(("Setting up table {}".format(consumer_id)))
        instance.query(
            f"""
            DROP TABLE IF EXISTS {db}.headers_exchange_{consumer_id};
            DROP TABLE IF EXISTS {db}.headers_exchange_{consumer_id}_mv;
            CREATE TABLE {db}.headers_exchange_{consumer_id} (key UInt64, value UInt64)
                ENGINE = RabbitMQ
                SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                         rabbitmq_num_consumers = 2,
                         rabbitmq_exchange_name = '{unique}_headers_exchange_testing',
                         rabbitmq_exchange_type = 'headers',
                         rabbitmq_flush_interval_ms=1000,
                         rabbitmq_max_block_size=100,
                         rabbitmq_routing_key_list = 'x-match=all,format=logs,type=report,year=2020',
                         rabbitmq_format = 'JSONEachRow',
                         rabbitmq_row_delimiter = '\\n';
            CREATE MATERIALIZED VIEW {db}.headers_exchange_{consumer_id}_mv TO {db}.destination AS
            SELECT key, value FROM {db}.headers_exchange_{consumer_id};
        """
        )

    num_tables_to_ignore = 2
    for consumer_id in range(num_tables_to_ignore):
        logging.debug(
            ("Setting up table {}".format(consumer_id + num_tables_to_receive))
        )
        instance.query(
            f"""
            DROP TABLE IF EXISTS {db}.headers_exchange_{consumer_id + num_tables_to_receive};
            DROP TABLE IF EXISTS {db}.headers_exchange_{consumer_id + num_tables_to_receive}_mv;
            CREATE TABLE {db}.headers_exchange_{consumer_id + num_tables_to_receive} (key UInt64, value UInt64)
                ENGINE = RabbitMQ
                SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                         rabbitmq_exchange_name = '{unique}_headers_exchange_testing',
                         rabbitmq_exchange_type = 'headers',
                         rabbitmq_routing_key_list = 'x-match=all,format=logs,type=report,year=2019',
                         rabbitmq_format = 'JSONEachRow',
                         rabbitmq_flush_interval_ms=1000,
                         rabbitmq_max_block_size=100,
                         rabbitmq_row_delimiter = '\\n';
            CREATE MATERIALIZED VIEW {db}.headers_exchange_{consumer_id + num_tables_to_receive}_mv TO {db}.destination AS
            SELECT key, value FROM {db}.headers_exchange_{consumer_id + num_tables_to_receive};
        """
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
            exchange=f"{unique}_headers_exchange_testing",
            routing_key="",
            properties=pika.BasicProperties(headers=fields, message_id=str(msg_id)),
            body=messages[msg_id],
        )

    connection.close()

    check_expected_result_polling(messages_num * num_tables_to_receive, f"SELECT count() FROM {db}.destination")

    for consumer_id in range(num_tables_to_receive + num_tables_to_ignore):
        instance.query(
            f"""
            DROP TABLE {db}.headers_exchange_{consumer_id}_mv;
            DROP TABLE {db}.headers_exchange_{consumer_id};
        """
        )

    instance.query(
        f"""
        DROP TABLE {db}.destination;
    """
    )


def test_rabbitmq_virtual_columns(rabbitmq_cluster, db, unique):
    instance.query(
        f"""
        CREATE TABLE {db}.rabbitmq_virtuals (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = '{unique}_virtuals',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_max_block_size=100,
                     rabbitmq_format = 'JSONEachRow';
        CREATE MATERIALIZED VIEW {db}.view Engine=Log AS
        SELECT value, key, _exchange_name, _channel_id, _delivery_tag, _redelivered FROM {db}.rabbitmq_virtuals;
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
        channel.basic_publish(exchange=f"{unique}_virtuals", routing_key="", body=message)

    check_expected_result_polling(message_num, f"SELECT count() FROM {db}.view")

    connection.close()

    result = instance.query(
        f"""
        SELECT key, value, _exchange_name, SUBSTRING(_channel_id, 1, 3), _delivery_tag, _redelivered
        FROM {db}.view ORDER BY key
    """
    )

    exchange = f"{unique}_virtuals"
    expected = "\n".join(
        [f"{i}\t{i}\t{exchange}\t1_0\t{i+1}\t0" for i in range(10)]
    ) + "\n"

    instance.query(
        f"""
        DROP TABLE {db}.rabbitmq_virtuals;
        DROP TABLE {db}.view;
    """
    )

    assert TSV(result) == TSV(expected)


def test_rabbitmq_virtual_columns_with_materialized_view(rabbitmq_cluster, db, unique):
    instance.query(
        f"""
        CREATE TABLE {db}.rabbitmq_virtuals_mv (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = '{unique}_virtuals_mv',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_max_block_size=100,
                     rabbitmq_format = 'JSONEachRow';
        CREATE TABLE {db}.view (key UInt64, value UInt64,
            exchange_name String, channel_id String, delivery_tag UInt64, redelivered UInt8) ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW {db}.consumer TO {db}.view AS
        SELECT *, _exchange_name as exchange_name, _channel_id as channel_id, _delivery_tag as delivery_tag, _redelivered as redelivered
        FROM {db}.rabbitmq_virtuals_mv;
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
        channel.basic_publish(exchange=f"{unique}_virtuals_mv", routing_key="", body=message)

    check_expected_result_polling(message_num, f"SELECT count() FROM {db}.view")

    connection.close()

    result = instance.query(
        f"SELECT key, value, exchange_name, SUBSTRING(channel_id, 1, 3), delivery_tag, redelivered FROM {db}.view ORDER BY delivery_tag"
    )
    exchange = f"{unique}_virtuals_mv"
    expected = "\n".join(
        [f"{i}\t{i}\t{exchange}\t1_0\t{i+1}\t0" for i in range(10)]
    ) + "\n"

    instance.query(
        f"""
        DROP TABLE {db}.consumer;
        DROP TABLE {db}.view;
        DROP TABLE {db}.rabbitmq_virtuals_mv
    """
    )

    assert TSV(result) == TSV(expected)


def test_rabbitmq_many_consumers_to_each_queue(rabbitmq_cluster, db, unique):
    instance.query(
        f"""
        DROP TABLE IF EXISTS {db}.destination;
        CREATE TABLE {db}.destination(key UInt64, value UInt64, channel_id String)
        ENGINE = MergeTree()
        ORDER BY key;
    """
    )

    num_tables = 4
    for table_id in range(num_tables):
        logging.debug(("Setting up table {}".format(table_id)))
        instance.query(
            f"""
            DROP TABLE IF EXISTS {db}.many_consumers_{table_id};
            DROP TABLE IF EXISTS {db}.many_consumers_{table_id}_mv;
            CREATE TABLE {db}.many_consumers_{table_id} (key UInt64, value UInt64)
                ENGINE = RabbitMQ
                SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                         rabbitmq_exchange_name = '{unique}_many_consumers',
                         rabbitmq_num_queues = 2,
                         rabbitmq_num_consumers = 2,
                         rabbitmq_flush_interval_ms=1000,
                         rabbitmq_max_block_size=100,
                         rabbitmq_queue_base = '{unique}_many_consumers',
                         rabbitmq_format = 'JSONEachRow',
                         rabbitmq_row_delimiter = '\\n';
            CREATE MATERIALIZED VIEW {db}.many_consumers_{table_id}_mv TO {db}.destination AS
            SELECT key, value, _channel_id as channel_id FROM {db}.many_consumers_{table_id};
        """
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
                exchange=f"{unique}_many_consumers",
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

    check_expected_result_polling(messages_num * threads_num, f"SELECT count() FROM {db}.destination")

    result2 = instance.query(f"SELECT count(DISTINCT channel_id) FROM {db}.destination")

    for thread in threads:
        thread.join()

    for consumer_id in range(num_tables):
        instance.query(
            f"""
            DROP TABLE {db}.many_consumers_{consumer_id};
            DROP TABLE {db}.many_consumers_{consumer_id}_mv;
        """
        )

    instance.query(
        f"""
        DROP TABLE {db}.destination;
    """
    )

    # 4 tables, 2 consumers for each table => 8 consumer tags
    assert int(result2) == 8


def test_rabbitmq_commit_on_block_write(rabbitmq_cluster, db, unique):
    logging.getLogger("pika").propagate = False
    instance.query(
        f"""
        CREATE TABLE {db}.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = '{unique}_block',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_queue_base = '{unique}_block',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_max_block_size = 100,
                     rabbitmq_row_delimiter = '\\n';
        CREATE TABLE {db}.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW {db}.consumer TO {db}.view AS
            SELECT * FROM {db}.rabbitmq;
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
                channel.basic_publish(exchange=f"{unique}_block", routing_key="", body=message)

    rabbitmq_thread = threading.Thread(target=produce)
    rabbitmq_thread.start()

    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        if int(instance.query(f"SELECT count() FROM {db}.view")) != 0:
            break
        time.sleep(1)
    else:
        cancel.set()
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The count is still 0."
        )

    cancel.set()

    instance.query(f"DETACH TABLE {db}.rabbitmq;")

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

    instance.query(f"ATTACH TABLE {db}.rabbitmq;")

    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        if int(instance.query(f"SELECT uniqExact(key) FROM {db}.view")) >= i[0]:
            break
        time.sleep(1)
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The uniqExact(key) is still less than {i[0]}."
        )

    result = int(instance.query(f"SELECT count() == uniqExact(key) FROM {db}.view"))

    instance.query(
        f"""
        DROP TABLE {db}.consumer;
        DROP TABLE {db}.view;
    """
    )

    rabbitmq_thread.join()
    connection.close()

    assert result == 1, "Messages from RabbitMQ get duplicated!"


def test_rabbitmq_no_connection_at_startup_1(rabbitmq_cluster, db, unique):
    error = instance.query_and_get_error(
        f"""
        CREATE TABLE {db}.cs (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'no_connection_at_startup:5672',
                     rabbitmq_exchange_name = '{unique}_cs',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_num_consumers = '5',
                     rabbitmq_row_delimiter = '\\n';
    """
    )
    assert "CANNOT_CONNECT_RABBITMQ" in error


def test_rabbitmq_no_connection_at_startup_2(rabbitmq_cluster, db, unique):
    instance.query(
        f"""
        CREATE TABLE {db}.cs (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = '{unique}_cs',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_num_consumers = '5',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_max_block_size=100,
                     rabbitmq_row_delimiter = '\\n';
        CREATE TABLE {db}.view (key UInt64, value UInt64)
            ENGINE = MergeTree
            ORDER BY key;
        CREATE MATERIALIZED VIEW {db}.consumer TO {db}.view AS
            SELECT * FROM {db}.cs;
    """
    )
    instance.query(f"DETACH TABLE {db}.cs")

    with rabbitmq_cluster.pause_rabbitmq():
        instance.query(f"ATTACH TABLE {db}.cs")

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
            exchange=f"{unique}_cs",
            routing_key="",
            body=message,
            properties=pika.BasicProperties(delivery_mode=2, message_id=str(i)),
        )
    connection.close()

    check_expected_result_polling(messages_num, f"SELECT count() FROM {db}.view")

    instance.query(
        f"""
        DROP TABLE {db}.consumer;
        DROP TABLE {db}.cs;
    """
    )


def test_rabbitmq_format_factory_settings(rabbitmq_cluster, db, unique):
    instance.query(
        f"""
        CREATE TABLE {db}.format_settings (
            id String, date DateTime
        ) ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = '{unique}_format_settings',
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

    channel.basic_publish(exchange=f"{unique}_format_settings", routing_key="", body=message)
    result = ""
    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        result = instance.query(f"SELECT date FROM {db}.format_settings")
        if result == expected:
            break
        time.sleep(0.05)
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The result did not match the expected value."
        )

    instance.query(
        f"""
        CREATE TABLE {db}.view (
            id String, date DateTime
        ) ENGINE = MergeTree ORDER BY id;
        CREATE MATERIALIZED VIEW {db}.consumer TO {db}.view AS
            SELECT * FROM {db}.format_settings;
        """
    )

    channel.basic_publish(exchange=f"{unique}_format_settings", routing_key="", body=message)

    result = ""
    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        result = instance.query(f"SELECT date FROM {db}.view")
        if result == expected:
            break
        time.sleep(0.05)
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The result did not match the expected value."
        )

    connection.close()
    instance.query(
        f"""
        DROP TABLE {db}.consumer;
        DROP TABLE {db}.format_settings;
    """
    )

    assert result == expected


def test_rabbitmq_vhost(rabbitmq_cluster, db, unique):
    instance.query(
        f"""
        CREATE TABLE {db}.rabbitmq_vhost (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = '{unique}_vhost',
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
        exchange=f"{unique}_vhost", routing_key="", body=json.dumps({"key": 1, "value": 2})
    )
    connection.close()
    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        result = instance.query(
            f"SELECT * FROM {db}.rabbitmq_vhost ORDER BY key", ignore_error=True
        )
        if result == "1\t2\n":
            break
        time.sleep(0.05)
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The result did not match the expected value '1\\t2\\n'."
        )


def test_rabbitmq_drop_table_properly(rabbitmq_cluster, db, unique):
    instance.query(
        f"""
        CREATE TABLE {db}.rabbitmq_drop (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_exchange_name = '{unique}_drop',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_queue_base = '{unique}_rabbit_queue_drop'
        """
    )

    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    channel.basic_publish(
        exchange=f"{unique}_drop", routing_key="", body=json.dumps({"key": 1, "value": 2})
    )
    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        result = instance.query(
            f"SELECT * FROM {db}.rabbitmq_drop ORDER BY key", ignore_error=True
        )
        if result == "1\t2\n":
            break
        time.sleep(0.05)
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The result did not match the expected value '1\\t2\\n'."
        )

    exists = channel.queue_declare(queue=f"{unique}_rabbit_queue_drop", passive=True)
    assert exists

    instance.query(f"DROP TABLE {db}.rabbitmq_drop")
    time.sleep(30)

    try:
        exists = channel.queue_declare(queue=f"{unique}_rabbit_queue_drop", passive=True)
    except Exception as e:
        exists = False

    assert not exists


def test_rabbitmq_queue_settings(rabbitmq_cluster, db, unique):
    instance.query(
        f"""
        CREATE TABLE {db}.rabbitmq_settings (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = '{unique}_rabbit_exchange',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_queue_base = '{unique}_rabbit_queue_settings',
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
            exchange=f"{unique}_rabbit_exchange",
            routing_key="",
            body=json.dumps({"key": 1, "value": 2}),
        )
    connection.close()

    instance.query(
        f"""
        CREATE TABLE {db}.view (key UInt64, value UInt64)
        ENGINE = MergeTree ORDER BY key;
        CREATE MATERIALIZED VIEW {db}.consumer TO {db}.view AS
            SELECT * FROM {db}.rabbitmq_settings;
        """
    )

    time.sleep(5)

    check_expected_result_polling(10, f"SELECT count() FROM {db}.view")

    instance.query(f"DROP TABLE {db}.rabbitmq_settings")


def test_rabbitmq_queue_consume(rabbitmq_cluster, db, unique):
    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    queue_name = f"{unique}_rabbit_queue"
    channel.queue_declare(queue=queue_name, durable=True)

    i = [0]
    messages_num = 1000

    def produce():
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        messages = []
        for _ in range(messages_num):
            message = json.dumps({"key": i[0], "value": i[0]})
            channel.basic_publish(exchange="", routing_key=queue_name, body=message)
            i[0] += 1

    threads = []
    threads_num = 10
    for _ in range(threads_num):
        threads.append(threading.Thread(target=produce))
    for thread in threads:
        time.sleep(random.uniform(0, 1))
        thread.start()

    instance.query(
        f"""
        CREATE TABLE {db}.rabbitmq_queue (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_queue_base = '{unique}_rabbit_queue',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_queue_consume = 1;
        CREATE TABLE {db}.view (key UInt64, value UInt64)
        ENGINE = MergeTree ORDER BY key;
        CREATE MATERIALIZED VIEW {db}.consumer TO {db}.view AS
            SELECT * FROM {db}.rabbitmq_queue;
        """
    )

    check_expected_result_polling(messages_num * threads_num, f"SELECT count() FROM {db}.view")

    for thread in threads:
        thread.join()

    instance.query(f"DROP TABLE {db}.rabbitmq_queue")


def test_rabbitmq_produce_consume_avro(rabbitmq_cluster, db, unique):
    num_rows = 75

    instance.query(
        f"""
        DROP TABLE IF EXISTS {db}.view;
        DROP TABLE IF EXISTS {db}.rabbit;
        DROP TABLE IF EXISTS {db}.rabbit_writer;

        CREATE TABLE {db}.rabbit_writer (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_format = 'Avro',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_exchange_name = '{unique}_avro',
                     rabbitmq_exchange_type = 'direct',
                     rabbitmq_routing_key_list = 'avro';

        CREATE TABLE {db}.rabbit (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_format = 'Avro',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_exchange_name = '{unique}_avro',
                     rabbitmq_exchange_type = 'direct',
                     rabbitmq_routing_key_list = 'avro';

        CREATE MATERIALIZED VIEW {db}.view Engine=Log AS
            SELECT key, value FROM {db}.rabbit;
    """
    )

    instance.query(
        f"INSERT INTO {db}.rabbit_writer select number*10 as key, number*100 as value from numbers({num_rows}) SETTINGS output_format_avro_rows_in_file = 7"
    )

    # Ideally we should wait for an event
    time.sleep(3)

    expected_num_rows = instance.query(
        f"SELECT COUNT(1) FROM {db}.view", ignore_error=True
    )
    assert int(expected_num_rows) == num_rows

    expected_max_key = instance.query(
        f"SELECT max(key) FROM {db}.view", ignore_error=True
    )
    assert int(expected_max_key) == (num_rows - 1) * 10


def test_rabbitmq_bad_args(rabbitmq_cluster, db, unique):
    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.exchange_declare(exchange=f"{unique}_f", exchange_type="fanout")
    assert "Unable to declare exchange" in instance.query_and_get_error(
        f"""
        CREATE TABLE {db}.drop (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_exchange_name = '{unique}_f',
                     rabbitmq_format = 'JSONEachRow';
    """
    )


def test_rabbitmq_issue_30691(rabbitmq_cluster, db, unique):
    instance.query(
        f"""
        CREATE TABLE {db}.rabbitmq_drop (json String)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_exchange_name = '{unique}_30691',
                     rabbitmq_row_delimiter = '\\n', -- Works only if adding this setting
                     rabbitmq_format = 'LineAsString',
                     rabbitmq_queue_base = '{unique}_30691';
        """
    )

    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    channel.basic_publish(
        exchange=f"{unique}_30691",
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
        result = instance.query(f"SELECT * FROM {db}.rabbitmq_drop", ignore_error=True)
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


def test_rabbitmq_drop_mv(rabbitmq_cluster, db, unique):
    instance.query(
        f"""
        CREATE TABLE {db}.drop_mv (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = '{unique}_mv',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_queue_base = '{unique}_drop_mv';
    """
    )
    instance.query(
        f"""
        CREATE TABLE {db}.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
    """
    )
    instance.query(
        f"""
        CREATE MATERIALIZED VIEW {db}.consumer TO {db}.view AS
            SELECT * FROM {db}.drop_mv;
    """
    )

    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    for i in range(20):
        channel.basic_publish(
            exchange=f"{unique}_mv", routing_key="", body=json.dumps({"key": i, "value": i})
        )

    check_expected_result_polling(20, f"SELECT count() FROM {db}.view")

    instance.query(f"DROP VIEW {db}.consumer SYNC")
    for i in range(20, 40):
        channel.basic_publish(
            exchange=f"{unique}_mv", routing_key="", body=json.dumps({"key": i, "value": i})
        )

    instance.query(
        f"""
        CREATE MATERIALIZED VIEW {db}.consumer TO {db}.view AS
            SELECT * FROM {db}.drop_mv;
    """
    )
    for i in range(40, 50):
        channel.basic_publish(
            exchange=f"{unique}_mv", routing_key="", body=json.dumps({"key": i, "value": i})
        )

    check_expected_result_polling(50, f"SELECT count() FROM {db}.view")

    result = instance.query(f"SELECT * FROM {db}.view ORDER BY key")
    rabbitmq_check_result(result, True)

    instance.query(f"DROP VIEW {db}.consumer SYNC")
    time.sleep(10)
    for i in range(50, 60):
        channel.basic_publish(
            exchange=f"{unique}_mv", routing_key="", body=json.dumps({"key": i, "value": i})
        )
    connection.close()

    count = 0
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        count = int(instance.query(f"SELECT count() FROM {db}.drop_mv"))
        if count:
            break
        time.sleep(0.05)
    else:
        pytest.fail(f"Time limit of 30 seconds reached. The count is still 0.")

    instance.query(f"DROP TABLE {db}.drop_mv")
    assert count > 0


def test_rabbitmq_random_detach(rabbitmq_cluster, db, unique):
    instance.query(
        f"""
        CREATE TABLE {db}.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = '{unique}_random',
                     rabbitmq_queue_base = '{unique}_random',
                     rabbitmq_num_queues = 2,
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_num_consumers = 2,
                     rabbitmq_format = 'JSONEachRow';
        CREATE TABLE {db}.view (key UInt64, value UInt64, channel_id String)
            ENGINE = MergeTree
            ORDER BY key;
        CREATE MATERIALIZED VIEW {db}.consumer TO {db}.view AS
            SELECT *, _channel_id AS channel_id FROM {db}.rabbitmq;
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
                exchange=f"{unique}_random",
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
    # instance.query(f"detach table {db}.rabbitmq")
    # revive_rabbitmq(rabbitmq_cluster.rabbitmq_docker_id)

    for thread in threads:
        thread.join()


def test_rabbitmq_predefined_configuration(rabbitmq_cluster, db, unique):
    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    instance.query(
        f"""
        CREATE TABLE {db}.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ(rabbit1, rabbitmq_vhost = '/')
            SETTINGS rabbitmq_flush_interval_ms=1000;
        """
    )

    # Exchange name 'named' is hardcoded in the named collection config (named_collection.xml)
    channel.basic_publish(
        exchange="named", routing_key="", body=json.dumps({"key": 1, "value": 2})
    )

    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        result = instance.query(
            f"SELECT * FROM {db}.rabbitmq ORDER BY key", ignore_error=True
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
            f"SELECT * FROM {db}.rabbitmq ORDER BY key", ignore_error=True
        )
        if result == "1\t2\n":
            break
        time.sleep(0.05)
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The result did not match the expected value '1\\t2\\n'."
        )


def test_rabbitmq_msgpack(rabbitmq_cluster, db, unique):
    instance.query(
        f"""
        drop table if exists {db}.rabbit_in;
        drop table if exists {db}.rabbit_out;
        create table
            {db}.rabbit_in (val String)
            engine=RabbitMQ
            settings rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = '{unique}_xhep',
                     rabbitmq_format = 'MsgPack',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_num_consumers = 1;
        create table
            {db}.rabbit_out (val String)
            engine=RabbitMQ
            settings rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = '{unique}_xhep',
                     rabbitmq_format = 'MsgPack',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_num_consumers = 1;
        set stream_like_engine_allow_direct_select=1;
        insert into {db}.rabbit_out select 'kek';
        """
    )

    result = ""
    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        result = instance.query(f"select * from {db}.rabbit_in;")
        if result.strip() == "kek":
            break
        time.sleep(1)
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The result did not match 'kek'."
        )

    assert result.strip() == "kek"


def test_rabbitmq_address(rabbitmq_cluster, db, unique):
    instance2.query(f"CREATE DATABASE IF NOT EXISTS {db}")
    instance2.query(
        f"""
        drop table if exists {db}.rabbit_in;
        drop table if exists {db}.rabbit_out;
        create table
            {db}.rabbit_in (val String)
            engine=RabbitMQ
            SETTINGS rabbitmq_exchange_name = '{unique}_rxhep',
                     rabbitmq_format = 'CSV',
                     rabbitmq_num_consumers = 1,
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_address='amqp://root:clickhouse@rabbitmq1:5672/';
        create table
            {db}.rabbit_out (val String) engine=RabbitMQ
            SETTINGS rabbitmq_exchange_name = '{unique}_rxhep',
                     rabbitmq_format = 'CSV',
                     rabbitmq_num_consumers = 1,
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_address='amqp://root:clickhouse@rabbitmq1:5672/';
        set stream_like_engine_allow_direct_select=1;
        insert into {db}.rabbit_out select 'kek';
    """
    )

    result = ""
    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        result = instance2.query(f"select * from {db}.rabbit_in;")
        if result.strip() == "kek":
            break
        time.sleep(1)
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The result did not match 'kek'."
        )

    assert result.strip() == "kek"

    instance2.query(f"DROP DATABASE {db} SYNC")


def test_format_with_prefix_and_suffix(rabbitmq_cluster, db, unique):
    instance.query(
        f"""
        CREATE TABLE {db}.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = '{unique}_insert',
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
    consumer.queue_bind(exchange=f"{unique}_insert", queue=queue_name, routing_key="custom")

    instance.query(
        f"INSERT INTO {db}.rabbitmq select number*10 as key, number*100 as value from numbers(2) settings format_custom_result_before_delimiter='<prefix>\n', format_custom_result_after_delimiter='<suffix>\n'"
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


def test_max_rows_per_message(rabbitmq_cluster, db, unique):
    num_rows = 5

    instance.query(
        f"""
        DROP TABLE IF EXISTS {db}.view;
        DROP TABLE IF EXISTS {db}.rabbit;

        CREATE TABLE {db}.rabbit (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_format = 'CustomSeparated',
                     rabbitmq_exchange_name = '{unique}_custom',
                     rabbitmq_exchange_type = 'direct',
                     rabbitmq_routing_key_list = 'custom1',
                     rabbitmq_max_rows_per_message = 3,
                     rabbitmq_flush_interval_ms = 1000,
                     format_custom_result_before_delimiter = '<prefix>\n',
                     format_custom_result_after_delimiter = '<suffix>\n';

        CREATE MATERIALIZED VIEW {db}.view Engine=Log AS
            SELECT key, value FROM {db}.rabbit;
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
    consumer.queue_bind(exchange=f"{unique}_custom", queue=queue_name, routing_key="custom1")

    instance.query(
        f"INSERT INTO {db}.rabbit select number*10 as key, number*100 as value from numbers({num_rows}) settings format_custom_result_before_delimiter='<prefix>\n', format_custom_result_after_delimiter='<suffix>\n'"
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

    check_expected_result_polling(num_rows, f"SELECT count() FROM {db}.view")

    result = instance.query(f"SELECT * FROM {db}.view")
    assert result == "0\t0\n10\t100\n20\t200\n30\t300\n40\t400\n"


def test_row_based_formats(rabbitmq_cluster, db, unique):
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
            DROP TABLE IF EXISTS {db}.view;
            DROP TABLE IF EXISTS {db}.rabbit;

            CREATE TABLE {db}.rabbit (key UInt64, value UInt64)
                ENGINE = RabbitMQ
                SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                         rabbitmq_format = '{format_name}',
                         rabbitmq_exchange_name = '{format_name}',
                         rabbitmq_exchange_type = 'direct',
                         rabbitmq_max_block_size = 100,
                         rabbitmq_flush_interval_ms = 1000,
                         rabbitmq_routing_key_list = '{format_name}',
                         rabbitmq_max_rows_per_message = 5;

            CREATE MATERIALIZED VIEW {db}.view Engine=Log AS
                SELECT key, value FROM {db}.rabbit;
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
            f"INSERT INTO {db}.rabbit SELECT number * 10 as key, number * 100 as value FROM numbers({num_rows});"
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

        check_expected_result_polling(num_rows, f"SELECT count() FROM {db}.view")

        expected = ""
        for i in range(num_rows):
            expected += str(i * 10) + "\t" + str(i * 100) + "\n"

        result = instance.query(f"SELECT * FROM {db}.view")
        assert result == expected


def test_block_based_formats_1(rabbitmq_cluster, db, unique):
    instance.query(
        f"""
        CREATE TABLE {db}.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = '{unique}_PrettySpace',
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
        exchange=f"{unique}_PrettySpace", queue=queue_name, routing_key="PrettySpace"
    )

    instance.query(
        f"INSERT INTO {db}.rabbitmq SELECT number * 10 as key, number * 100 as value FROM numbers(5) settings max_block_size=2, optimize_trivial_insert_select=0, output_format_pretty_color=1, output_format_pretty_row_numbers=0;"
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
        split = message.split("\n")
        assert split[0] == " \x1b[1mkey\x1b[0m   \x1b[1mvalue\x1b[0m"
        assert split[1] == ""
        assert split[-1] == ""
        data += [line.split() for line in split[2:-1]]

    assert data == [
        ["0", "0"],
        ["10", "100"],
        ["20", "200"],
        ["30", "300"],
        ["40", "400"],
    ]


def test_block_based_formats_2(rabbitmq_cluster, db, unique):
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
            DROP TABLE IF EXISTS {db}.view;
            DROP TABLE IF EXISTS {db}.rabbit;

            CREATE TABLE {db}.rabbit (key UInt64, value UInt64)
                ENGINE = RabbitMQ
                SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                         rabbitmq_format = '{format_name}',
                         rabbitmq_exchange_name = '{format_name}',
                         rabbitmq_exchange_type = 'direct',
                         rabbitmq_max_block_size = 100,
                         rabbitmq_flush_interval_ms = 1000,
                         rabbitmq_routing_key_list = '{format_name}';

            CREATE MATERIALIZED VIEW {db}.view Engine=Log AS
                SELECT key, value FROM {db}.rabbit;
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
            f"INSERT INTO {db}.rabbit SELECT number * 10 as key, number * 100 as value FROM numbers({num_rows}) settings max_block_size=12, optimize_trivial_insert_select=0;"
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

        check_expected_result_polling(num_rows, f"SELECT count() FROM {db}.view")

        result = instance.query(f"SELECT * FROM {db}.view ORDER by key")
        expected = ""
        for i in range(num_rows):
            expected += str(i * 10) + "\t" + str(i * 100) + "\n"
        assert result == expected


def test_rabbitmq_flush_by_block_size(rabbitmq_cluster, db, unique):
    instance.query(
        f"""
         DROP TABLE IF EXISTS {db}.view;
         DROP TABLE IF EXISTS {db}.consumer;

         CREATE TABLE {db}.rabbitmq (key UInt64, value UInt64)
             ENGINE = RabbitMQ
             SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                      rabbitmq_exchange_name = '{unique}_flush_by_block',
                      rabbitmq_queue_base = '{unique}_flush_by_block',
                      rabbitmq_max_block_size = 100,
                      rabbitmq_flush_interval_ms = 640000, /* should not flush by time during test */
                      rabbitmq_format = 'JSONEachRow';

         CREATE TABLE {db}.view (key UInt64, value UInt64)
             ENGINE = MergeTree()
             ORDER BY key;

         CREATE MATERIALIZED VIEW {db}.consumer TO {db}.view AS
             SELECT * FROM {db}.rabbitmq;

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
                    exchange=f"{unique}_flush_by_block",
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
                    f"SELECT count() FROM system.parts WHERE database = '{db}' AND table = 'view' AND name = 'all_1_1_0'"
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
    result = instance.query(f"SELECT count() FROM {db}.view WHERE _part='all_1_1_0'")
    # logging.debug(result)

    instance.query(
        f"""
         DROP TABLE {db}.consumer;
         DROP TABLE {db}.view;
         DROP TABLE {db}.rabbitmq;
     """
    )

    # 100 = first poll should return 100 messages (and rows)
    # not waiting for stream_flush_interval_ms
    assert (
        int(result) == 100
    ), "Messages from rabbitmq should be flushed when block of size rabbitmq_max_block_size is formed!"


def test_rabbitmq_flush_by_time(rabbitmq_cluster, db, unique):
    instance.query(
        f"""
        DROP TABLE IF EXISTS {db}.view;
        DROP TABLE IF EXISTS {db}.consumer;

        CREATE TABLE {db}.rabbitmq (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = '{unique}_flush_by_time',
                     rabbitmq_queue_base = '{unique}_flush_by_time',
                     rabbitmq_max_block_size = 100,
                     rabbitmq_flush_interval_ms = 5000,
                     rabbitmq_format = 'JSONEachRow';

        CREATE TABLE {db}.view (key UInt64, value UInt64, ts DateTime64(3) MATERIALIZED now64(3))
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
                    exchange=f"{unique}_flush_by_time",
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
        f"""
        CREATE MATERIALIZED VIEW {db}.consumer TO {db}.view AS
            SELECT * FROM {db}.rabbitmq;
    """
    )

    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        time.sleep(0.2)
        total_count = instance.query(
            f"SELECT count() FROM system.parts WHERE database = '{db}' AND table = 'view'"
        )
        logging.debug(f"kssenii total count: {total_count}")
        count = int(
            instance.query(
                f"SELECT count() FROM system.parts WHERE database = '{db}' AND table = 'view' AND name = 'all_1_1_0'"
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
    result = instance.query(f"SELECT uniqExact(ts) FROM {db}.view")

    cancel.set()
    produce_thread.join()

    instance.query(
        f"""
        DROP TABLE {db}.consumer;
        DROP TABLE {db}.view;
        DROP TABLE {db}.rabbitmq;
    """
    )

    assert int(result) == 3


def test_rabbitmq_handle_error_mode_stream(rabbitmq_cluster, db, unique):
    instance.query(
        f"""
        DROP TABLE IF EXISTS {db}.rabbitmq;
        DROP TABLE IF EXISTS {db}.view;
        DROP TABLE IF EXISTS {db}.data;
        DROP TABLE IF EXISTS {db}.errors;
        DROP TABLE IF EXISTS {db}.errors_view;

        CREATE TABLE {db}.rabbit (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = '{rabbitmq_cluster.rabbitmq_host}:5672',
                     rabbitmq_exchange_name = '{unique}_select',
                     rabbitmq_commit_on_select = 1,
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_row_delimiter = '\\n',
                     rabbitmq_handle_error_mode = 'stream';


        CREATE TABLE {db}.errors (error Nullable(String), broken_message Nullable(String))
             ENGINE = MergeTree()
             ORDER BY tuple();

        CREATE MATERIALIZED VIEW {db}.errors_view TO {db}.errors AS
                SELECT _error as error, _raw_message as broken_message FROM {db}.rabbit where not isNull(_error);

        CREATE TABLE {db}.data (key UInt64, value UInt64)
             ENGINE = MergeTree()
             ORDER BY key;

        CREATE MATERIALIZED VIEW {db}.view TO {db}.data AS
                SELECT key, value FROM {db}.rabbit;
        """
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
        channel.basic_publish(exchange=f"{unique}_select", routing_key="", body=message)

    connection.close()
    # The order of messages in select * from {db}.rabbitmq is not guaranteed, so sleep to collect everything in one select
    time.sleep(1)

    check_expected_result_polling(num_rows, f"SELECT count() FROM {db}.data")

    result = instance.query(f"SELECT * FROM {db}.data ORDER by key")
    expected = "0\t0\n" * (num_rows // 2)
    for i in range(num_rows):
        if i % 2 == 0:
            expected += str(i) + "\t" + str(i) + "\n"

    assert result == expected

    check_expected_result_polling(num_rows / 2, f"SELECT count() FROM {db}.errors")

    broken_messages = instance.query(
        f"SELECT broken_message FROM {db}.errors order by broken_message"
    )
    expected = []
    for i in range(num_rows):
        if i % 2 != 0:
            expected.append("Broken message " + str(i) + "\n")

    expected = "".join(sorted(expected))
    assert broken_messages == expected


def test_attach_broken_table(rabbitmq_cluster, db, unique):
    table_name = f"rabbit_queue_{uuid.uuid4().hex}"
    instance.query(
        f"""
        DROP TABLE IF EXISTS {table_name};
        ATTACH TABLE {table_name} UUID '{uuid.uuid4()}' (`payload` String) ENGINE = RabbitMQ SETTINGS rabbitmq_host_port = 'nonexisting:5671', rabbitmq_format = 'JSONEachRow', rabbitmq_username = 'test', rabbitmq_password = 'test'
        """
    )

    error = instance.query_and_get_error(f"SELECT * FROM {table_name}")
    assert "CANNOT_CONNECT_RABBITMQ" in error
    error = instance.query_and_get_error(f"INSERT INTO {table_name} VALUES ('test')")
    assert "CANNOT_CONNECT_RABBITMQ" in error


def test_rabbitmq_nack_failed_insert(rabbitmq_cluster, db, unique):
    table_name = f"nack_failed_insert_{uuid.uuid4().hex}"
    exchange = f"{table_name}_exchange"

    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    deadl_exchange = f"{unique}_deadl"
    channel.exchange_declare(exchange=deadl_exchange)

    result = channel.queue_declare(queue=f"{unique}_deadq")
    queue_name = result.method.queue
    channel.queue_bind(exchange=deadl_exchange, routing_key="", queue=queue_name)

    instance.query(
        f"""
        CREATE TABLE {db}.{table_name} (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = '{rabbitmq_cluster.rabbitmq_host}:5672',
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_exchange_name = '{exchange}',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_queue_settings_list='x-dead-letter-exchange={deadl_exchange}';

        DROP TABLE IF EXISTS {db}.view;
        CREATE TABLE {db}.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;

        DROP TABLE IF EXISTS {db}.consumer;
        CREATE MATERIALIZED VIEW {db}.consumer TO {db}.view AS
            SELECT intDiv(key, if(key < 25, 0, 1)) as key, value FROM {db}.{table_name};
        """
    )

    num_rows = 25
    for i in range(num_rows):
        message = json.dumps({"key": i, "value": i}) + "\n"
        channel.basic_publish(exchange=exchange, routing_key="", body=message)

    instance.wait_for_log_line(
        "Failed to push to views. Error: Code: 153. DB::Exception: Division by zero"
    )

    count = [0]

    def on_consume(channel, method, properties, body):
        data = json.loads(body)
        message = json.dumps({"key": data["key"] + 100, "value": data["value"]}) + "\n"
        channel.basic_publish(exchange=exchange, routing_key="", body=message)
        count[0] += 1
        if count[0] == num_rows:
            channel.stop_consuming()

    channel.basic_consume(queue_name, on_consume)
    channel.start_consuming()

    check_expected_result_polling(num_rows, f"SELECT count() FROM {db}.view")

    instance.query(
        f"""
        DROP TABLE {db}.consumer;
        DROP TABLE {db}.view;
        DROP TABLE {db}.{table_name};
    """
    )
    connection.close()


def view_test(expected_num_messages, _exchange_name, db):
    result = instance.query(f"SELECT COUNT(1) FROM {db}.errors")

    assert int(result) == expected_num_messages


def dead_letter_queue_test(expected_num_messages, exchange_name, _db):
    result = instance.query(f"SELECT * FROM system.dead_letter_queue FORMAT Vertical")

    logging.debug(f"system.dead_letter_queue content is {result}")

    rows = int(
        instance.query(
            f"SELECT count() FROM system.dead_letter_queue WHERE rabbitmq_exchange_name = '{exchange_name}'"
        )
    )
    assert rows == expected_num_messages


def rabbitmq_reject_broken_messages(
    rabbitmq_cluster, db, unique, handle_error_mode, additional_dml, check_method, broken_messages_rejected
):
    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    deadletter_exchange = f"{unique}_deadletter_exchange_{handle_error_mode}"
    deadletter_queue = f"{unique}_deadletter_queue_{handle_error_mode}"
    channel.exchange_declare(exchange=deadletter_exchange)

    exchange = f"{unique}_select_{handle_error_mode}_{int(time.time())}"

    result = channel.queue_declare(queue=deadletter_queue)
    channel.queue_bind(
        exchange=deadletter_exchange, routing_key="", queue=deadletter_queue
    )

    instance.query(
        f"""
        DROP TABLE IF EXISTS {db}.rabbitmq;
        DROP TABLE IF EXISTS {db}.view;
        DROP TABLE IF EXISTS {db}.data;
        DROP TABLE IF EXISTS {db}.errors;
        DROP TABLE IF EXISTS {db}.errors_view;

        CREATE TABLE {db}.rabbit (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = '{rabbitmq_cluster.rabbitmq_host}:5672',
                     rabbitmq_exchange_name = '{exchange}',
                     rabbitmq_commit_on_select = 1,
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_row_delimiter = '\\n',
                     rabbitmq_handle_error_mode = '{handle_error_mode}',
                     rabbitmq_queue_settings_list='x-dead-letter-exchange={deadletter_exchange}';


        CREATE TABLE {db}.errors (error Nullable(String), broken_message Nullable(String))
             ENGINE = MergeTree()
             ORDER BY tuple();

        CREATE TABLE {db}.data (key UInt64, value UInt64)
             ENGINE = MergeTree()
             ORDER BY key;

        CREATE MATERIALIZED VIEW {db}.view TO {db}.data AS
                SELECT key, value FROM {db}.rabbit;

        {additional_dml};

        """
    )

    messages = []
    num_rows = 50
    num_good_messages = 0

    for i in range(num_rows):
        if (i+1) % 2 == 0:   # let's finish on good message to not miss the latest one
            messages.append(json.dumps({"key": i, "value": i}))
            num_good_messages += 1
        else:
            messages.append("Broken message " + str(i))

    for message in messages:
        channel.basic_publish(exchange=exchange, routing_key="", body=message)

    time.sleep(1)

    expected_num_rows = num_good_messages if broken_messages_rejected else num_rows

    check_expected_result_polling(expected_num_rows, f"SELECT count() FROM {db}.data")

    dead_letters = []
    num_bad_messages = num_rows - num_good_messages

    def on_dead_letter(channel, method, properties, body):
        dead_letters.append(body)
        if len(dead_letters) == num_bad_messages:
            channel.stop_consuming()

    channel.basic_consume(deadletter_queue, on_dead_letter)
    channel.start_consuming()

    assert len(dead_letters) == num_bad_messages

    i = 0
    for letter in dead_letters:
        assert f"Broken message {i}" in str(letter)
        i += 2

    result = instance.query(f"SELECT * FROM {db}.errors FORMAT Vertical")
    logging.debug(f"{db}.errors contains {result}")

    check_method(len(dead_letters), exchange, db)

    connection.close()


def test_rabbitmq_reject_broken_messages_stream(rabbitmq_cluster, db, unique):
    rabbitmq_reject_broken_messages(
        rabbitmq_cluster,
        db,
        unique,
        "stream",
        f"CREATE MATERIALIZED VIEW {db}.errors_view TO {db}.errors AS SELECT _error as error, _raw_message as broken_message FROM {db}.rabbit where not isNull(_error)",
        view_test,
        broken_messages_rejected = False,
    )


def test_rabbitmq_reject_broken_messages_dead_letter_queue(rabbitmq_cluster, db, unique):
    rabbitmq_reject_broken_messages(
        rabbitmq_cluster,
        db,
        unique,
        "dead_letter_queue",
        "",
        dead_letter_queue_test,
        broken_messages_rejected = True,
    )


def test_rabbitmq_json_type(rabbitmq_cluster, db, unique):
    instance.query(
        f"""
        SET enable_json_type=1;
        CREATE TABLE {db}.rabbitmq (data JSON)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                     rabbitmq_exchange_name = '{unique}_json_type',
                     rabbitmq_format = 'JSONAsObject',
                     rabbitmq_commit_on_select = 1,
                     rabbitmq_flush_interval_ms=1000,
                     rabbitmq_max_block_size=100,
                     rabbitmq_queue_base = '{unique}_json_type',
                     rabbitmq_row_delimiter = '\\n';
        CREATE TABLE {db}.view (a Int64)
            ENGINE = MergeTree()
            ORDER BY a;
        CREATE MATERIALIZED VIEW {db}.consumer TO {db}.view AS
            SELECT data.a::Int64 as a FROM {db}.rabbitmq;
        """
    )

    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    messages = [
        '{"a" : 1}',
        '{"a" : 2}',
    ]

    for message in messages:
        channel.basic_publish(exchange=f"{unique}_json_type", routing_key="", body=message)
    connection.close()

    while int(instance.query(f"SELECT count() FROM {db}.view")) < 2:
        time.sleep(1)

    result = instance.query(f"SELECT * FROM {db}.view ORDER BY a;")

    expected = """\
1
2
"""

    assert TSV(result) == TSV(expected)

    instance.query(
        f"""
        DROP TABLE {db}.view;
        DROP TABLE {db}.consumer;
        DROP TABLE {db}.rabbitmq;
    """
    )


def test_hiding_credentials(rabbitmq_cluster, db, unique):
    table_name = "test_hiding_credentials"
    exchange = f"{unique}_{table_name}"
    instance.query(
        f"""
        DROP TABLE IF EXISTS {db}.{table_name};
        CREATE TABLE {db}.{table_name} (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = '{rabbitmq_cluster.rabbitmq_host}:{cluster.rabbitmq_port}',
                     rabbitmq_exchange_name = '{exchange}',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_username = 'clickhouse',
                     rabbitmq_password = 'rabbitmq',
                     rabbitmq_address = 'amqp://root:clickhouse@rabbitmq1:5672/';
        """
    )

    instance.query("SYSTEM FLUSH LOGS")
    message = instance.query(f"SELECT message FROM system.text_log WHERE message ILIKE '%CREATE TABLE {db}.{table_name}%'")
    assert "rabbitmq_password = \\'[HIDDEN]\\'" in  message
    assert "rabbitmq_address = \\'amqp://root:[HIDDEN]@rabbitmq1:5672/\\'" in  message


def test_rabbitmq_default_mode_nack_on_parse_error(rabbitmq_cluster, db, unique):
    """When rabbitmq_handle_error_mode = 'default' and a message fails to parse,
    the message must be properly nack'd (not left permanently unacked).
    Regression test for https://github.com/ClickHouse/ClickHouse/issues/73541
    """
    credentials = pika.PlainCredentials("root", "clickhouse")
    parameters = pika.ConnectionParameters(
        rabbitmq_cluster.rabbitmq_ip, rabbitmq_cluster.rabbitmq_port, "/", credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    deadletter_exchange = f"{unique}_dlx"
    deadletter_queue = f"{unique}_dlq"
    channel.exchange_declare(exchange=deadletter_exchange)
    channel.queue_declare(queue=deadletter_queue)
    channel.queue_bind(exchange=deadletter_exchange, routing_key="", queue=deadletter_queue)

    exchange = f"{unique}_exchange"

    instance.query(
        f"""
        CREATE TABLE {db}.rabbit (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = '{rabbitmq_cluster.rabbitmq_host}:5672',
                     rabbitmq_exchange_name = '{exchange}',
                     rabbitmq_format = 'JSONEachRow',
                     rabbitmq_flush_interval_ms = 1000,
                     rabbitmq_queue_settings_list = 'x-dead-letter-exchange={deadletter_exchange}';

        CREATE TABLE {db}.data (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;

        CREATE MATERIALIZED VIEW {db}.view TO {db}.data AS
            SELECT key, value FROM {db}.rabbit;
        """
    )

    num_bad = 10
    for i in range(num_bad):
        # String value for a UInt64 column triggers a parse error in DEFAULT mode
        channel.basic_publish(
            exchange=exchange, routing_key="",
            body=json.dumps({"key": f"not_a_number_{i}", "value": i}),
        )

    # Wait for the error to appear in logs, proving the messages were consumed
    instance.wait_for_log_line("Failed to push to views.*Cannot parse input")

    # Bad messages must arrive in the dead-letter queue (proving they were nack'd)
    dead_letters = []

    def on_dead_letter(ch, method, properties, body):
        dead_letters.append(body)
        if len(dead_letters) == num_bad:
            ch.stop_consuming()

    channel.basic_consume(deadletter_queue, on_dead_letter, auto_ack=True)
    deadline = time.monotonic() + 30
    while len(dead_letters) < 1 and time.monotonic() < deadline:
        connection.process_data_events(time_limit=1)

    # In DEFAULT mode each streaming iteration processes one bad message before
    # the exception aborts the pipeline, so collecting all 10 takes many cycles.
    # Asserting >= 1 is sufficient: before the fix we would get 0 (messages
    # stayed permanently unacked instead of being nack'd to the DLX).
    assert len(dead_letters) >= 1, (
        "No dead-lettered messages received within 30 seconds. "
        "Messages were likely left permanently unacked instead of being nack'd."
    )

    # Now publish good messages and verify they are consumed
    num_good = 10
    for i in range(num_good):
        channel.basic_publish(
            exchange=exchange, routing_key="",
            body=json.dumps({"key": i, "value": i}),
        )

    check_expected_result_polling(num_good, f"SELECT count() FROM {db}.data")

    channel.queue_delete(deadletter_queue)
    channel.exchange_delete(deadletter_exchange)
    connection.close()
