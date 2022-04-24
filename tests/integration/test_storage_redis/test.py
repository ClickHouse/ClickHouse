import json
import os.path as p
import random
import subprocess
import threading
import logging
import time
from random import randrange
import math
import redis

import pytest
from google.protobuf.internal.encoder import _VarintBytes
from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=[
        "configs/macros.xml",
        "configs/redis.xml",
        "configs/named_collection.xml",
    ],
    user_configs=["configs/users.xml"],
    with_redis=True
)


# Helpers


def redis_check_result(result, check=False, ref_file="test_redis_json.reference"):
    fpath = p.join(p.dirname(__file__), ref_file)
    with open(fpath) as reference:
        if check:
            assert TSV(result) == TSV(reference)
        else:
            return TSV(result) == TSV(reference)


def wait_redis_to_start(redis_docker_id, timeout=180, throw=True):
    start = time.time()
    while time.time() - start < timeout:
        try:
            if instance.cluster.check_redis_is_available(redis_docker_id):
                logging.debug("Redis is available")
            time.sleep(0.5)
        except Exception as ex:
            logging.debug("Can't connect to Redis " + str(ex))
            time.sleep(0.5)

    if throw:
        raise Exception("Cannot wait Redis container")
    return False


def kill_redis(redis_id):
    p = subprocess.Popen(("docker", "stop", redis_id), stdout=subprocess.PIPE)
    p.communicate()
    return p.returncode == 0


def revive_redis(redis_id):
    p = subprocess.Popen(("docker", "start", redis_id), stdout=subprocess.PIPE)
    p.communicate()
    wait_redis_to_start(redis_id)


# Fixtures


@pytest.fixture(scope="module")
def redis_cluster():
    try:
        cluster.start()
        logging.debug("redis_id is {}".format(instance.cluster.redis_docker_id))
        instance.query("CREATE DATABASE test")

        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def redis_setup_teardown():
    print("redis is available - running test")
    yield  # run test
    instance.query("DROP DATABASE test NO DELAY")
    instance.query("CREATE DATABASE test")


# Tests


def test_redis_select(redis_cluster):
    stream_name = 'select'
    group_name = 'test_select'
    connection = redis.Redis(host=redis_cluster.redis_ip, port=cluster.redis_port, password="clickhouse")
    connection.xgroup_create(stream_name, group_name, '$', mkstream=True)

    instance.query(
        """
        CREATE TABLE test.redis (key UInt64, value UInt64)
            ENGINE = Redis
            SETTINGS redis_broker = '{}:6379',
                     redis_stream_list = '{}',
                     redis_group_name = '{}';
        """.format(
            redis_cluster.redis_host,
            stream_name,
            group_name
        )
    )

    for i in range(50):
        connection.xadd(stream_name, {"key": i, "value": i})

    # The order of messages in select * from test.redis is not guaranteed, so sleep to collect everything in one select
    connection.close()
    time.sleep(1)

    result = ""
    while True:
        result += instance.query(
            "SELECT * FROM test.redis ORDER BY key", ignore_error=True
        )
        if redis_check_result(result):
            break
    redis_check_result(result, True)


def test_redis_select_empty(redis_cluster):
    stream_name = 'empty'
    group_name = 'test_empty'
    connection = redis.Redis(redis_cluster.redis_ip, port=cluster.redis_port, password="clickhouse")
    connection.xgroup_create(stream_name, group_name, '$', mkstream=True)

    instance.query(
        """
        CREATE TABLE test.redis (key UInt64, value UInt64)
            ENGINE = Redis
            SETTINGS redis_broker = '{}:6379',
                     redis_stream_list = '{}',
                     redis_group_name = '{}';
        """.format(
            redis_cluster.redis_host,
            stream_name,
            group_name
        )
    )

    connection.close()
    assert int(instance.query("SELECT count() FROM test.redis")) == 0


def test_redis_macros(redis_cluster):
    stream_name = 'macro'
    group_name = 'test_macro'
    connection = redis.Redis(redis_cluster.redis_ip, port=redis_cluster.redis_port, password="clickhouse")
    connection.xgroup_create(stream_name, group_name, '$', mkstream=True)

    instance.query(
        """
        CREATE TABLE test.redis (key UInt64, value UInt64)
            ENGINE = Redis
            SETTINGS redis_broker = '{redis_broker}',
                     redis_stream_list = '{redis_stream_list}',
                     redis_group_name = '{redis_group_name}';
        """
    )

    for i in range(50):
        connection.xadd(stream_name, {"key": i, "value": i})

    connection.close()
    time.sleep(1)

    result = ""
    while True:
        result += instance.query(
            "SELECT * FROM test.redis ORDER BY key", ignore_error=True
        )
        if redis_check_result(result):
            break

    redis_check_result(result, True)


def test_redis_materialized_view(redis_cluster):
    stream_name = 'mv'
    group_name = 'test_mv'
    connection = redis.Redis(redis_cluster.redis_ip, port=redis_cluster.redis_port, password="clickhouse")
    connection.xgroup_create(stream_name, group_name, '$', mkstream=True)

    instance.query(
        """
        CREATE TABLE test.redis (key UInt64, value UInt64)
            ENGINE = Redis
            SETTINGS redis_broker = '{}:6379',
                     redis_stream_list = '{}',
                     redis_group_name = '{}';
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.redis;

        CREATE TABLE test.view2 (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer2 TO test.view2 AS
            SELECT * FROM test.redis group by (key, value);
        """.format(
                redis_cluster.redis_host,
                stream_name,
                group_name
            )
    )

    for i in range(50):
        connection.xadd(stream_name, {"key": i, "value": i})

    time_limit_sec = 60
    deadline = time.monotonic() + time_limit_sec

    result = None
    while time.monotonic() < deadline:
        result = instance.query("SELECT * FROM test.view ORDER BY key")
        if redis_check_result(result):
            break

    redis_check_result(result, True)

    deadline = time.monotonic() + time_limit_sec

    while time.monotonic() < deadline:
        result = instance.query("SELECT * FROM test.view2 ORDER BY key")
        if redis_check_result(result):
            break

    redis_check_result(result, True)
    connection.close()


def test_redis_materialized_view_with_subquery(redis_cluster):
    stream_name = 'mvsq'
    group_name = 'test_mvsq'
    connection = redis.Redis(redis_cluster.redis_ip, port=redis_cluster.redis_port, password="clickhouse")
    connection.xgroup_create(stream_name, group_name, '$', mkstream=True)

    instance.query(
        """
        CREATE TABLE test.redis (key UInt64, value UInt64)
            ENGINE = Redis
            SETTINGS redis_broker = '{}:6379',
                     redis_stream_list = '{}',
                     redis_group_name = '{}';
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM (SELECT * FROM test.redis);
        """.format(
                redis_cluster.redis_host,
                stream_name,
                group_name
            )
    )

    for i in range(50):
        connection.xadd(stream_name, {"key": i, "value": i})

    while True:
        result = instance.query("SELECT * FROM test.view ORDER BY key")
        if redis_check_result(result):
            break

    connection.close()
    redis_check_result(result, True)


def test_redis_many_materialized_views(redis_cluster):
    stream_name = 'mmv'
    group_name = 'test_mmv'
    connection = redis.Redis(redis_cluster.redis_ip, port=redis_cluster.redis_port, password="clickhouse")
    connection.xgroup_create(stream_name, group_name, '$', mkstream=True)

    instance.query(
        """
        DROP TABLE IF EXISTS test.view1;
        DROP TABLE IF EXISTS test.view2;
        DROP TABLE IF EXISTS test.consumer1;
        DROP TABLE IF EXISTS test.consumer2;
        CREATE TABLE test.redis (key UInt64, value UInt64)
            ENGINE = Redis
            SETTINGS redis_broker = '{}:6379',
                     redis_stream_list = '{}',
                     redis_group_name = '{}',
                     redis_num_consumers = 1;
        CREATE TABLE test.view1 (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE TABLE test.view2 (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer1 TO test.view1 AS
            SELECT * FROM test.redis;
        CREATE MATERIALIZED VIEW test.consumer2 TO test.view2 AS
            SELECT * FROM test.redis;
        """.format(
                redis_cluster.redis_host,
                stream_name,
                group_name
            )
    )

    for i in range(50):
        connection.xadd(stream_name, {"key": i, "value": i})

    while True:
        result1 = instance.query("SELECT * FROM test.view1 ORDER BY key")
        result2 = instance.query("SELECT * FROM test.view2 ORDER BY key")
        if redis_check_result(result1) and redis_check_result(result2):
            break

    instance.query(
        """
        DROP TABLE test.consumer1;
        DROP TABLE test.consumer2;
        DROP TABLE test.view1;
        DROP TABLE test.view2;
    """
    )

    connection.close()
    redis_check_result(result1, True)
    redis_check_result(result2, True)


def test_redis_big_message(redis_cluster):
    # Create messages of size ~100Kb
    redis_messages = 1000
    batch_size = 1000
    messages = [
        {"key": i, "value": "x" * 100 * batch_size}
        for i in range(redis_messages)
    ]

    stream_name = 'big'
    group_name = 'test_big'
    connection = redis.Redis(redis_cluster.redis_ip, port=redis_cluster.redis_port, password="clickhouse")
    connection.xgroup_create(stream_name, group_name, '$', mkstream=True)

    instance.query(
        """
        CREATE TABLE test.redis (key UInt64, value String)
            ENGINE = Redis
            SETTINGS redis_broker = '{}:6379',
                     redis_stream_list = '{}',
                     redis_group_name = '{}';
        CREATE TABLE test.view (key UInt64, value String)
            ENGINE = MergeTree
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.redis;
        """.format(
                redis_cluster.redis_host,
                stream_name,
                group_name
            )
    )

    for message in messages:
        connection.xadd(stream_name, message)

    while True:
        result = instance.query("SELECT count() FROM test.view")
        if int(result) == redis_messages:
            break

    connection.close()

    assert (
        int(result) == redis_messages
    ), "ClickHouse lost some messages: {}".format(result)


def test_redis_multiple_streams_and_consumers(redis_cluster):
    NUM_STREAMS = 10
    NUM_CONSUMERS = 10
    streams = []
    group_name = 'test_multiple'
    connection = redis.Redis(redis_cluster.redis_ip, port=redis_cluster.redis_port, password="clickhouse")
    for i in range(NUM_STREAMS):
        streams.append('multiple_{}'.format(i))
        connection.xgroup_create(streams[-1], group_name, '$', mkstream=True)

    instance.query(
        """
        CREATE TABLE test.redis (key UInt64, value UInt64)
            ENGINE = Redis
            SETTINGS redis_broker = '{}:6379',
                     redis_stream_list = '{}',
                     redis_group_name = '{}',
                     redis_num_consumers = {};
        CREATE TABLE test.view (key UInt64, value UInt64, stream_id String)
            ENGINE = MergeTree
            ORDER BY key
            SETTINGS old_parts_lifetime=5, cleanup_delay_period=2, cleanup_delay_period_random_add=3;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT *, _stream AS stream_id FROM test.redis;
        """.format(
                redis_cluster.redis_host,
                ','.join(streams),
                group_name,
                NUM_CONSUMERS
            )
    )

    i = [0]
    messages_num = 10000

    def produce(stream_name):
        messages = []
        for _ in range(messages_num):
            messages.append({"key": i[0], "value": i[0]})
            i[0] += 1

        for message in messages:
            connection.xadd(stream_name, message)
        connection.close()

    threads = []
    threads_num = NUM_STREAMS * 2

    for j in range(threads_num):
        threads.append(threading.Thread(target=produce, args=(streams[j % len(streams)],)))

    for thread in threads:
        time.sleep(random.uniform(0, 1))
        thread.start()

    for _ in range(20):
        result1 = instance.query("SELECT count() FROM test.view")
        time.sleep(1)
        if int(result1) == messages_num * threads_num:
            break

    result2 = instance.query("SELECT count(DISTINCT stream_id) FROM test.view")

    for thread in threads:
        thread.join()

    assert (
        int(result1) == messages_num * threads_num
    ), "ClickHouse lost some messages"
    assert int(result2) == 10


def test_redis_mv_combo(redis_cluster):
    NUM_STREAMS = 2
    NUM_CONSUMERS = 5
    NUM_MV = 5

    streams = []
    group_name = 'test_combo'
    connection = redis.Redis(redis_cluster.redis_ip, port=redis_cluster.redis_port, password="clickhouse")
    for i in range(NUM_STREAMS):
        streams.append('combo_{}'.format(i))
        connection.xgroup_create(streams[-1], group_name, '$', mkstream=True)

    instance.query(
        """
        CREATE TABLE test.redis (key UInt64, value UInt64)
            ENGINE = Redis
            SETTINGS redis_broker = '{}:6379',
                     redis_stream_list = '{}',
                     redis_group_name = '{}',
                     redis_num_consumers = {};
        """.format(
            redis_cluster.redis_host,
            ','.join(streams),
            group_name,
            NUM_CONSUMERS
        )
    )

    query = ""
    for mv_id in range(NUM_MV):
        query += """
            DROP TABLE IF EXISTS test.combo_{0};
            DROP TABLE IF EXISTS test.combo_{0}_mv;
            CREATE TABLE test.combo_{0} (key UInt64, value UInt64)
                ENGINE = MergeTree()
                ORDER BY key;
            CREATE MATERIALIZED VIEW test.combo_{0}_mv TO test.combo_{0} AS
                SELECT * FROM test.redis;
        """.format(
                mv_id
            )
    instance.query(query)

    i = [0]
    messages_num = 1000

    def produce(stream_name):
        messages = []
        for _ in range(messages_num):
            messages.append({"key": i[0], "value": i[0]})
            i[0] += 1

        for message in messages:
            connection.xadd(stream_name, message)
        connection.close()

    threads = []
    threads_num = NUM_STREAMS * 5

    for j in range(threads_num):
        threads.append(threading.Thread(target=produce, args=(streams[j % len(streams)],)))

    for thread in threads:
        time.sleep(random.uniform(0, 1))
        thread.start()

    last_result = -1
    result = 0
    for _ in range(20):
        last_result = result
        result = 0
        for mv_id in range(NUM_MV):
            num = int(
                instance.query("SELECT count() FROM test.combo_{0}".format(mv_id))
            )
            result += num
            logging.debug(num)
        if int(result) == messages_num * threads_num * NUM_MV:
            break
        time.sleep(1)

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


def test_redis_insert(redis_cluster):
    stream_name = 'insert'
    group_name = 'test_insert'
    connection = redis.Redis(redis_cluster.redis_ip, port=redis_cluster.redis_port, password="clickhouse")
    connection.xgroup_create(stream_name, group_name, '$', mkstream=True)

    instance.query(
        """
        CREATE TABLE test.redis (key UInt64, value UInt64)
            ENGINE = Redis
            SETTINGS redis_broker = '{}:6379',
                     redis_stream_list = '{}',
                     redis_group_name = '{}';
        """.format(
            redis_cluster.redis_host,
            stream_name,
            group_name
        )
    )


    values = []
    for i in range(50):
        values.append("({i}, {i})".format(i=i))
    values = ",".join(values)

    while True:
        try:
            instance.query("INSERT INTO test.redis VALUES {}".format(values))
            break
        except QueryRuntimeException as e:
            if "Local: Timed out." in str(e):
                continue
            else:
                raise

    for _ in range(10):
        insert_messages = connection.xread({stream_name: "0-0"}, count=50)

        # xread returns list of lists of topics and messages - select first topic and its messages
        insert_messages = insert_messages[0][1]
        logging.warning(insert_messages)
        if len(insert_messages) == 50:
            break

    result = "\n".join(map(lambda x: x[1]["key".encode()].decode() + "\t" + x[1]["value".encode()].decode(), insert_messages))
    redis_check_result(result, True)


def test_redis_many_inserts(redis_cluster):
    stream_name = 'many_inserts'
    group_name = 'test_many_inserts'
    connection = redis.Redis(redis_cluster.redis_ip, port=redis_cluster.redis_port, password="clickhouse")
    connection.xgroup_create(stream_name, group_name, '$', mkstream=True)

    instance.query(
        """
        DROP TABLE IF EXISTS test.redis_many;
        DROP TABLE IF EXISTS test.redis_consume;
        DROP TABLE IF EXISTS test.view_many;
        DROP TABLE IF EXISTS test.consumer_many;
        CREATE TABLE test.redis_many (key UInt64, value UInt64)
            ENGINE = Redis
            SETTINGS redis_broker = '{0}:6379',
                     redis_stream_list = '{1}',
                     redis_group_name = '{2}';
        CREATE TABLE test.redis_consume (key UInt64, value UInt64)
            ENGINE = Redis
            SETTINGS redis_broker = '{0}:6379',
                     redis_stream_list = '{1}',
                     redis_group_name = '{2}';
        """.format(
            redis_cluster.redis_host,
            stream_name,
            group_name
        )
    )

    messages_num = 10000
    values = []
    for i in range(messages_num):
        values.append("({i}, {i})".format(i=i))
    values = ",".join(values)

    def insert():
        while True:
            try:
                instance.query(
                    "INSERT INTO test.redis_many VALUES {}".format(values)
                )
                break
            except QueryRuntimeException as e:
                if "Local: Timed out." in str(e):
                    continue
                else:
                    raise

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
            SELECT * FROM test.redis_consume;
    """
    )

    for thread in threads:
        thread.join()

    while True:
        result = instance.query("SELECT count() FROM test.view_many")
        print(result, messages_num * threads_num)
        if int(result) == messages_num * threads_num:
            break
        time.sleep(1)

    instance.query(
        """
        DROP TABLE test.redis_consume;
        DROP TABLE test.redis_many;
        DROP TABLE test.consumer_many;
        DROP TABLE test.view_many;
    """
    )

    assert (
        int(result) == messages_num * threads_num
    ), "ClickHouse lost some messages: {}".format(result)


def test_redis_overloaded_insert(redis_cluster):
    stream_name = 'over'
    group_name = 'test_over'
    connection = redis.Redis(redis_cluster.redis_ip, port=redis_cluster.redis_port, password="clickhouse")
    connection.xgroup_create(stream_name, group_name, '$', mkstream=True)

    instance.query(
        """
        DROP TABLE IF EXISTS test.view_overload;
        DROP TABLE IF EXISTS test.consumer_overload;
        DROP TABLE IF EXISTS test.redis_consume;
        CREATE TABLE test.redis_consume (key UInt64, value UInt64)
            ENGINE = Redis
            SETTINGS redis_broker = '{0}',
                     redis_stream_list = '{1}',
                     redis_num_consumers = 5,
                     redis_max_block_size = 10000,
                     redis_group_name = '{2}';
        CREATE TABLE test.redis_overload (key UInt64, value UInt64)
            ENGINE = Redis
            SETTINGS redis_broker = '{0}',
                     redis_stream_list = '{1}',
                     redis_group_name = '{2}';
        CREATE TABLE test.view_overload (key UInt64, value UInt64)
            ENGINE = MergeTree
            ORDER BY key
            SETTINGS old_parts_lifetime=5, cleanup_delay_period=2, cleanup_delay_period_random_add=3;
        CREATE MATERIALIZED VIEW test.consumer_overload TO test.view_overload AS
            SELECT * FROM test.redis_consume;
        """.format(
            redis_cluster.redis_host,
            stream_name,
            group_name
        )
    )

    messages_num = 100000

    def insert():
        values = []
        for i in range(messages_num):
            values.append("({i}, {i})".format(i=i))
        values = ",".join(values)

        while True:
            try:
                instance.query(
                    "INSERT INTO test.redis_overload VALUES {}".format(values)
                )
                break
            except QueryRuntimeException as e:
                if "Local: Timed out." in str(e):
                    continue
                else:
                    raise

    threads = []
    threads_num = 5
    for _ in range(threads_num):
        threads.append(threading.Thread(target=insert))
    for thread in threads:
        time.sleep(random.uniform(0, 1))
        thread.start()

    while True:
        result = instance.query("SELECT count() FROM test.view_overload")
        time.sleep(1)
        if int(result) == messages_num * threads_num:
            break

    instance.query(
        """
        DROP TABLE test.consumer_overload;
        DROP TABLE test.view_overload;
        DROP TABLE test.redis_consume;
        DROP TABLE test.redis_overload;
    """
    )

    for thread in threads:
        thread.join()

    assert (
        int(result) == messages_num * threads_num
    ), "ClickHouse lost some messages: {}".format(result)


# def test_redis_direct_exchange(redis_cluster):
#     instance.query(
#         """
#         DROP TABLE IF EXISTS test.destination;
#         CREATE TABLE test.destination(key UInt64, value UInt64)
#         ENGINE = MergeTree()
#         ORDER BY key
#         SETTINGS old_parts_lifetime=5, cleanup_delay_period=2, cleanup_delay_period_random_add=3;
#     """
#     )
#
#     num_tables = 5
#     for consumer_id in range(num_tables):
#         print(("Setting up table {}".format(consumer_id)))
#         instance.query(
#             """
#             DROP TABLE IF EXISTS test.direct_exchange_{0};
#             DROP TABLE IF EXISTS test.direct_exchange_{0}_mv;
#             CREATE TABLE test.direct_exchange_{0} (key UInt64, value UInt64)
#                 ENGINE = Redis
#                 SETTINGS redis_broker = 'redis1:6379',
#                          redis_num_consumers = 2,
#                          redis_num_queues = 2,
#                          redis_stream_list = 'direct_exchange_testing',
#                          redis_exchange_type = 'direct',
#                          redis_routing_key_list = 'direct_{0}',
#                          redis_format = 'JSONEachRow',
#                          redis_row_delimiter = '\\n';
#             CREATE MATERIALIZED VIEW test.direct_exchange_{0}_mv TO test.destination AS
#             SELECT key, value FROM test.direct_exchange_{0};
#         """.format(
#                 consumer_id
#             )
#         )
#
#     i = [0]
#     messages_num = 1000
#
#     credentials = pika.PlainCredentials("root", "clickhouse")
#     parameters = pika.ConnectionParameters(
#         redis_cluster.redis_ip, redis_cluster.redis_port, "/", credentials
#     )
#     connection = pika.BlockingConnection(parameters)
#     channel = connection.channel()
#
#     messages = []
#     for _ in range(messages_num):
#         messages.append(json.dumps({"key": i[0], "value": i[0]}))
#         i[0] += 1
#
#     key_num = 0
#     for num in range(num_tables):
#         key = "direct_" + str(key_num)
#         key_num += 1
#         for message in messages:
#             mes_id = str(randrange(10))
#             channel.basic_publish(
#                 exchange="direct_exchange_testing",
#                 routing_key=key,
#                 properties=pika.BasicProperties(message_id=mes_id),
#                 body=message,
#             )
#
#     connection.close()
#
#     while True:
#         result = instance.query("SELECT count() FROM test.destination")
#         time.sleep(1)
#         if int(result) == messages_num * num_tables:
#             break
#
#     for consumer_id in range(num_tables):
#         instance.query(
#             """
#             DROP TABLE test.direct_exchange_{0}_mv;
#             DROP TABLE test.direct_exchange_{0};
#         """.format(
#                 consumer_id
#             )
#         )
#
#     instance.query(
#         """
#         DROP TABLE IF EXISTS test.destination;
#     """
#     )
#
#     assert (
#         int(result) == messages_num * num_tables
#     ), "ClickHouse lost some messages: {}".format(result)
#
#
# def test_redis_fanout_exchange(redis_cluster):
#     instance.query(
#         """
#         DROP TABLE IF EXISTS test.destination;
#         CREATE TABLE test.destination(key UInt64, value UInt64)
#         ENGINE = MergeTree()
#         ORDER BY key;
#     """
#     )
#
#     num_tables = 5
#     for consumer_id in range(num_tables):
#         print(("Setting up table {}".format(consumer_id)))
#         instance.query(
#             """
#             DROP TABLE IF EXISTS test.fanout_exchange_{0};
#             DROP TABLE IF EXISTS test.fanout_exchange_{0}_mv;
#             CREATE TABLE test.fanout_exchange_{0} (key UInt64, value UInt64)
#                 ENGINE = Redis
#                 SETTINGS redis_broker = 'redis1:6379',
#                          redis_num_consumers = 2,
#                          redis_num_queues = 2,
#                          redis_routing_key_list = 'key_{0}',
#                          redis_stream_list = 'fanout_exchange_testing',
#                          redis_exchange_type = 'fanout',
#                          redis_format = 'JSONEachRow',
#                          redis_row_delimiter = '\\n';
#             CREATE MATERIALIZED VIEW test.fanout_exchange_{0}_mv TO test.destination AS
#             SELECT key, value FROM test.fanout_exchange_{0};
#         """.format(
#                 consumer_id
#             )
#         )
#
#     i = [0]
#     messages_num = 1000
#
#     credentials = pika.PlainCredentials("root", "clickhouse")
#     parameters = pika.ConnectionParameters(
#         redis_cluster.redis_ip, redis_cluster.redis_port, "/", credentials
#     )
#     connection = pika.BlockingConnection(parameters)
#     channel = connection.channel()
#
#     messages = []
#     for _ in range(messages_num):
#         messages.append(json.dumps({"key": i[0], "value": i[0]}))
#         i[0] += 1
#
#     for msg_id in range(messages_num):
#         channel.basic_publish(
#             exchange="fanout_exchange_testing",
#             routing_key="",
#             properties=pika.BasicProperties(message_id=str(msg_id)),
#             body=messages[msg_id],
#         )
#
#     connection.close()
#
#     while True:
#         result = instance.query("SELECT count() FROM test.destination")
#         time.sleep(1)
#         if int(result) == messages_num * num_tables:
#             break
#
#     for consumer_id in range(num_tables):
#         instance.query(
#             """
#             DROP TABLE test.fanout_exchange_{0}_mv;
#             DROP TABLE test.fanout_exchange_{0};
#         """.format(
#                 consumer_id
#             )
#         )
#
#     instance.query(
#         """
#         DROP TABLE test.destination;
#     """
#     )
#
#     assert (
#         int(result) == messages_num * num_tables
#     ), "ClickHouse lost some messages: {}".format(result)
#
#
# def test_redis_topic_exchange(redis_cluster):
#     instance.query(
#         """
#         DROP TABLE IF EXISTS test.destination;
#         CREATE TABLE test.destination(key UInt64, value UInt64)
#         ENGINE = MergeTree()
#         ORDER BY key;
#     """
#     )
#
#     num_tables = 5
#     for consumer_id in range(num_tables):
#         print(("Setting up table {}".format(consumer_id)))
#         instance.query(
#             """
#             DROP TABLE IF EXISTS test.topic_exchange_{0};
#             DROP TABLE IF EXISTS test.topic_exchange_{0}_mv;
#             CREATE TABLE test.topic_exchange_{0} (key UInt64, value UInt64)
#                 ENGINE = Redis
#                 SETTINGS redis_broker = 'redis1:6379',
#                          redis_num_consumers = 2,
#                          redis_num_queues = 2,
#                          redis_stream_list = 'topic_exchange_testing',
#                          redis_exchange_type = 'topic',
#                          redis_routing_key_list = '*.{0}',
#                          redis_format = 'JSONEachRow',
#                          redis_row_delimiter = '\\n';
#             CREATE MATERIALIZED VIEW test.topic_exchange_{0}_mv TO test.destination AS
#             SELECT key, value FROM test.topic_exchange_{0};
#         """.format(
#                 consumer_id
#             )
#         )
#
#     for consumer_id in range(num_tables):
#         print(("Setting up table {}".format(num_tables + consumer_id)))
#         instance.query(
#             """
#             DROP TABLE IF EXISTS test.topic_exchange_{0};
#             DROP TABLE IF EXISTS test.topic_exchange_{0}_mv;
#             CREATE TABLE test.topic_exchange_{0} (key UInt64, value UInt64)
#                 ENGINE = Redis
#                 SETTINGS redis_broker = 'redis1:6379',
#                          redis_num_consumers = 2,
#                          redis_num_queues = 2,
#                          redis_stream_list = 'topic_exchange_testing',
#                          redis_exchange_type = 'topic',
#                          redis_routing_key_list = '*.logs',
#                          redis_format = 'JSONEachRow',
#                          redis_row_delimiter = '\\n';
#             CREATE MATERIALIZED VIEW test.topic_exchange_{0}_mv TO test.destination AS
#             SELECT key, value FROM test.topic_exchange_{0};
#         """.format(
#                 num_tables + consumer_id
#             )
#         )
#
#     i = [0]
#     messages_num = 1000
#
#     credentials = pika.PlainCredentials("root", "clickhouse")
#     parameters = pika.ConnectionParameters(
#         redis_cluster.redis_ip, redis_cluster.redis_port, "/", credentials
#     )
#     connection = pika.BlockingConnection(parameters)
#     channel = connection.channel()
#
#     messages = []
#     for _ in range(messages_num):
#         messages.append(json.dumps({"key": i[0], "value": i[0]}))
#         i[0] += 1
#
#     key_num = 0
#     for num in range(num_tables):
#         key = "topic." + str(key_num)
#         key_num += 1
#         for message in messages:
#             channel.basic_publish(
#                 exchange="topic_exchange_testing", routing_key=key, body=message
#             )
#
#     key = "random.logs"
#     current = 0
#     for msg_id in range(messages_num):
#         channel.basic_publish(
#             exchange="topic_exchange_testing",
#             routing_key=key,
#             properties=pika.BasicProperties(message_id=str(msg_id)),
#             body=messages[msg_id],
#         )
#
#     connection.close()
#
#     while True:
#         result = instance.query("SELECT count() FROM test.destination")
#         time.sleep(1)
#         if int(result) == messages_num * num_tables + messages_num * num_tables:
#             break
#
#     for consumer_id in range(num_tables * 2):
#         instance.query(
#             """
#             DROP TABLE test.topic_exchange_{0}_mv;
#             DROP TABLE test.topic_exchange_{0};
#         """.format(
#                 consumer_id
#             )
#         )
#
#     instance.query(
#         """
#         DROP TABLE test.destination;
#     """
#     )
#
#     assert (
#         int(result) == messages_num * num_tables + messages_num * num_tables
#     ), "ClickHouse lost some messages: {}".format(result)
#
#
# def test_redis_hash_exchange(redis_cluster):
#     instance.query(
#         """
#         DROP TABLE IF EXISTS test.destination;
#         CREATE TABLE test.destination(key UInt64, value UInt64, channel_id String)
#         ENGINE = MergeTree()
#         ORDER BY key;
#     """
#     )
#
#     num_tables = 4
#     for consumer_id in range(num_tables):
#         table_name = "redis_consumer{}".format(consumer_id)
#         print(("Setting up {}".format(table_name)))
#         instance.query(
#             """
#             DROP TABLE IF EXISTS test.{0};
#             DROP TABLE IF EXISTS test.{0}_mv;
#             CREATE TABLE test.{0} (key UInt64, value UInt64)
#                 ENGINE = Redis
#                 SETTINGS redis_broker = 'redis1:6379',
#                          redis_num_consumers = 4,
#                          redis_num_queues = 2,
#                          redis_exchange_type = 'consistent_hash',
#                          redis_stream_list = 'hash_exchange_testing',
#                          redis_format = 'JSONEachRow',
#                          redis_row_delimiter = '\\n';
#             CREATE MATERIALIZED VIEW test.{0}_mv TO test.destination AS
#                 SELECT key, value, _channel_id AS channel_id FROM test.{0};
#         """.format(
#                 table_name
#             )
#         )
#
#     i = [0]
#     messages_num = 500
#
#     credentials = pika.PlainCredentials("root", "clickhouse")
#     parameters = pika.ConnectionParameters(
#         redis_cluster.redis_ip, redis_cluster.redis_port, "/", credentials
#     )
#
#     def produce():
#         # init connection here because otherwise python redis client might fail
#         connection = pika.BlockingConnection(parameters)
#         channel = connection.channel()
#         messages = []
#         for _ in range(messages_num):
#             messages.append(json.dumps({"key": i[0], "value": i[0]}))
#             i[0] += 1
#         for msg_id in range(messages_num):
#             channel.basic_publish(
#                 exchange="hash_exchange_testing",
#                 routing_key=str(msg_id),
#                 properties=pika.BasicProperties(message_id=str(msg_id)),
#                 body=messages[msg_id],
#             )
#         connection.close()
#
#     threads = []
#     threads_num = 10
#
#     for _ in range(threads_num):
#         threads.append(threading.Thread(target=produce))
#     for thread in threads:
#         time.sleep(random.uniform(0, 1))
#         thread.start()
#
#     result1 = ""
#     while True:
#         result1 = instance.query("SELECT count() FROM test.destination")
#         time.sleep(1)
#         if int(result1) == messages_num * threads_num:
#             break
#
#     result2 = instance.query("SELECT count(DISTINCT channel_id) FROM test.destination")
#
#     for consumer_id in range(num_tables):
#         table_name = "redis_consumer{}".format(consumer_id)
#         instance.query(
#             """
#             DROP TABLE test.{0}_mv;
#             DROP TABLE test.{0};
#         """.format(
#                 table_name
#             )
#         )
#
#     instance.query(
#         """
#         DROP TABLE test.destination;
#     """
#     )
#
#     for thread in threads:
#         thread.join()
#
#     assert (
#         int(result1) == messages_num * threads_num
#     ), "ClickHouse lost some messages: {}".format(result)
#     assert int(result2) == 4 * num_tables
#
#
# def test_redis_multiple_bindings(redis_cluster):
#     instance.query(
#         """
#         DROP TABLE IF EXISTS test.destination;
#         CREATE TABLE test.destination(key UInt64, value UInt64)
#         ENGINE = MergeTree()
#         ORDER BY key;
#     """
#     )
#
#     instance.query(
#         """
#         DROP TABLE IF EXISTS test.bindings;
#         DROP TABLE IF EXISTS test.bindings_mv;
#         CREATE TABLE test.bindings (key UInt64, value UInt64)
#             ENGINE = Redis
#             SETTINGS redis_broker = 'redis1:6379',
#                      redis_stream_list = 'multiple_bindings_testing',
#                      redis_exchange_type = 'direct',
#                      redis_routing_key_list = 'key1,key2,key3,key4,key5',
#                      redis_format = 'JSONEachRow',
#                      redis_row_delimiter = '\\n';
#         CREATE MATERIALIZED VIEW test.bindings_mv TO test.destination AS
#             SELECT * FROM test.bindings;
#     """
#     )
#
#     i = [0]
#     messages_num = 500
#
#     credentials = pika.PlainCredentials("root", "clickhouse")
#     parameters = pika.ConnectionParameters(
#         redis_cluster.redis_ip, redis_cluster.redis_port, "/", credentials
#     )
#
#     def produce():
#         # init connection here because otherwise python redis client might fail
#         connection = pika.BlockingConnection(parameters)
#         channel = connection.channel()
#
#         messages = []
#         for _ in range(messages_num):
#             messages.append(json.dumps({"key": i[0], "value": i[0]}))
#             i[0] += 1
#
#         keys = ["key1", "key2", "key3", "key4", "key5"]
#
#         for key in keys:
#             for message in messages:
#                 channel.basic_publish(
#                     exchange="multiple_bindings_testing", routing_key=key, body=message
#                 )
#
#         connection.close()
#
#     threads = []
#     threads_num = 10
#
#     for _ in range(threads_num):
#         threads.append(threading.Thread(target=produce))
#     for thread in threads:
#         time.sleep(random.uniform(0, 1))
#         thread.start()
#
#     while True:
#         result = instance.query("SELECT count() FROM test.destination")
#         time.sleep(1)
#         if int(result) == messages_num * threads_num * 5:
#             break
#
#     for thread in threads:
#         thread.join()
#
#     instance.query(
#         """
#         DROP TABLE test.bindings;
#         DROP TABLE test.bindings_mv;
#         DROP TABLE test.destination;
#     """
#     )
#
#     assert (
#         int(result) == messages_num * threads_num * 5
#     ), "ClickHouse lost some messages: {}".format(result)
#
#
# def test_redis_headers_exchange(redis_cluster):
#     instance.query(
#         """
#         DROP TABLE IF EXISTS test.destination;
#         CREATE TABLE test.destination(key UInt64, value UInt64)
#         ENGINE = MergeTree()
#         ORDER BY key;
#     """
#     )
#
#     num_tables_to_receive = 2
#     for consumer_id in range(num_tables_to_receive):
#         print(("Setting up table {}".format(consumer_id)))
#         instance.query(
#             """
#             DROP TABLE IF EXISTS test.headers_exchange_{0};
#             DROP TABLE IF EXISTS test.headers_exchange_{0}_mv;
#             CREATE TABLE test.headers_exchange_{0} (key UInt64, value UInt64)
#                 ENGINE = Redis
#                 SETTINGS redis_broker = 'redis1:6379',
#                          redis_num_consumers = 2,
#                          redis_stream_list = 'headers_exchange_testing',
#                          redis_exchange_type = 'headers',
#                          redis_routing_key_list = 'x-match=all,format=logs,type=report,year=2020',
#                          redis_format = 'JSONEachRow',
#                          redis_row_delimiter = '\\n';
#             CREATE MATERIALIZED VIEW test.headers_exchange_{0}_mv TO test.destination AS
#             SELECT key, value FROM test.headers_exchange_{0};
#         """.format(
#                 consumer_id
#             )
#         )
#
#     num_tables_to_ignore = 2
#     for consumer_id in range(num_tables_to_ignore):
#         print(("Setting up table {}".format(consumer_id + num_tables_to_receive)))
#         instance.query(
#             """
#             DROP TABLE IF EXISTS test.headers_exchange_{0};
#             DROP TABLE IF EXISTS test.headers_exchange_{0}_mv;
#             CREATE TABLE test.headers_exchange_{0} (key UInt64, value UInt64)
#                 ENGINE = Redis
#                 SETTINGS redis_broker = 'redis1:6379',
#                          redis_stream_list = 'headers_exchange_testing',
#                          redis_exchange_type = 'headers',
#                          redis_routing_key_list = 'x-match=all,format=logs,type=report,year=2019',
#                          redis_format = 'JSONEachRow',
#                          redis_row_delimiter = '\\n';
#             CREATE MATERIALIZED VIEW test.headers_exchange_{0}_mv TO test.destination AS
#             SELECT key, value FROM test.headers_exchange_{0};
#         """.format(
#                 consumer_id + num_tables_to_receive
#             )
#         )
#
#     i = [0]
#     messages_num = 1000
#
#     credentials = pika.PlainCredentials("root", "clickhouse")
#     parameters = pika.ConnectionParameters(
#         redis_cluster.redis_ip, redis_cluster.redis_port, "/", credentials
#     )
#     connection = pika.BlockingConnection(parameters)
#     channel = connection.channel()
#
#     messages = []
#     for _ in range(messages_num):
#         messages.append(json.dumps({"key": i[0], "value": i[0]}))
#         i[0] += 1
#
#     fields = {}
#     fields["format"] = "logs"
#     fields["type"] = "report"
#     fields["year"] = "2020"
#
#     for msg_id in range(messages_num):
#         channel.basic_publish(
#             exchange="headers_exchange_testing",
#             routing_key="",
#             properties=pika.BasicProperties(headers=fields, message_id=str(msg_id)),
#             body=messages[msg_id],
#         )
#
#     connection.close()
#
#     while True:
#         result = instance.query("SELECT count() FROM test.destination")
#         time.sleep(1)
#         if int(result) == messages_num * num_tables_to_receive:
#             break
#
#     for consumer_id in range(num_tables_to_receive + num_tables_to_ignore):
#         instance.query(
#             """
#             DROP TABLE test.headers_exchange_{0}_mv;
#             DROP TABLE test.headers_exchange_{0};
#         """.format(
#                 consumer_id
#             )
#         )
#
#     instance.query(
#         """
#         DROP TABLE test.destination;
#     """
#     )
#
#     assert (
#         int(result) == messages_num * num_tables_to_receive
#     ), "ClickHouse lost some messages: {}".format(result)
#
#
def test_redis_virtual_columns(redis_cluster):
    stream_name = 'virtuals'
    group_name = 'test_virtuals'
    connection = redis.Redis(redis_cluster.redis_ip, port=redis_cluster.redis_port, password="clickhouse")
    connection.xgroup_create(stream_name, group_name, '$', mkstream=True)

    instance.query(
        """
        CREATE TABLE test.redis_virtuals (key UInt64, value UInt64)
            ENGINE = Redis
            SETTINGS redis_broker = '{}:6379',
                     redis_stream_list = '{}',
                     redis_group_name = '{}';
        CREATE MATERIALIZED VIEW test.view Engine=Log AS
        SELECT value, key, _stream, _key, _timestamp, _sequence_number FROM test.redis_virtuals;
        """.format(
                redis_cluster.redis_host,
                stream_name,
                group_name
            )
    )

    message_num = 10
    for i in range(message_num):
        connection.xadd(stream_name, {"key": i, "value": i}, str(i + 1) + "-0")

    while True:
        result = instance.query("SELECT count() FROM test.view")
        time.sleep(1)
        if int(result) == message_num:
            break

    connection.close()

    result = instance.query(
        """
        SELECT key, value, _stream, _key, _timestamp, _sequence_number
        FROM test.view ORDER BY key
    """
    )

    expected = """\
0	0	virtuals	1-0	1	0
1	1	virtuals	2-0	2	0
2	2	virtuals	3-0	3	0
3	3	virtuals	4-0	4	0
4	4	virtuals	5-0	5	0
5	5	virtuals	6-0	6	0
6	6	virtuals	7-0	7	0
7	7	virtuals	8-0	8	0
8	8	virtuals	9-0	9	0
9	9	virtuals	10-0	10	0
"""

    instance.query(
        """
        DROP TABLE test.redis_virtuals;
        DROP TABLE test.view;
    """
    )

    assert TSV(result) == TSV(expected)


def test_redis_virtual_columns_with_materialized_view(redis_cluster):
    stream_name = 'virtuals_mv'
    group_name = 'test_virtuals_mv'
    connection = redis.Redis(redis_cluster.redis_ip, port=redis_cluster.redis_port, password="clickhouse")
    connection.xgroup_create(stream_name, group_name, '$', mkstream=True)

    instance.query(
        """
        CREATE TABLE test.redis_virtuals_mv (key UInt64, value UInt64)
            ENGINE = Redis
            SETTINGS redis_broker = '{}:6379',
                     redis_stream_list = '{}',
                     redis_group_name = '{}';
        CREATE TABLE test.view (key UInt64, value UInt64,
            stream String, _key String, timestamp UInt8, sequence_number UInt8) ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
        SELECT *, _stream as stream, _key, _timestamp as timestamp, _sequence_number as sequence_number
        FROM test.redis_virtuals_mv;
        """.format(
                redis_cluster.redis_host,
                stream_name,
                group_name
            )
    )

    message_num = 10
    for i in range(message_num):
        connection.xadd(stream_name, {"key": i, "value": i}, str(i + 1) + "-0")

    while True:
        result = instance.query("SELECT count() FROM test.view")
        time.sleep(1)
        if int(result) == message_num:
            break

    connection.close()

    result = instance.query(
        "SELECT key, value, stream, SUBSTRING(channel_id, 1, 3), delivery_tag, redelivered FROM test.view ORDER BY delivery_tag"
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
        DROP TABLE test.redis_virtuals_mv
    """
    )

    assert TSV(result) == TSV(expected)
#
#
# def test_redis_many_consumers_to_each_queue(redis_cluster):
#     instance.query(
#         """
#         DROP TABLE IF EXISTS test.destination;
#         CREATE TABLE test.destination(key UInt64, value UInt64, channel_id String)
#         ENGINE = MergeTree()
#         ORDER BY key;
#     """
#     )
#
#     num_tables = 4
#     for table_id in range(num_tables):
#         print(("Setting up table {}".format(table_id)))
#         instance.query(
#             """
#             DROP TABLE IF EXISTS test.many_consumers_{0};
#             DROP TABLE IF EXISTS test.many_consumers_{0}_mv;
#             CREATE TABLE test.many_consumers_{0} (key UInt64, value UInt64)
#                 ENGINE = Redis
#                 SETTINGS redis_broker = 'redis1:6379',
#                          redis_stream_list = 'many_consumers',
#                          redis_num_queues = 2,
#                          redis_num_consumers = 2,
#                          redis_queue_base = 'many_consumers',
#                          redis_format = 'JSONEachRow',
#                          redis_row_delimiter = '\\n';
#             CREATE MATERIALIZED VIEW test.many_consumers_{0}_mv TO test.destination AS
#             SELECT key, value, _channel_id as channel_id FROM test.many_consumers_{0};
#         """.format(
#                 table_id
#             )
#         )
#
#     i = [0]
#     messages_num = 1000
#
#     credentials = pika.PlainCredentials("root", "clickhouse")
#     parameters = pika.ConnectionParameters(
#         redis_cluster.redis_ip, redis_cluster.redis_port, "/", credentials
#     )
#
#     def produce():
#         connection = pika.BlockingConnection(parameters)
#         channel = connection.channel()
#
#         messages = []
#         for _ in range(messages_num):
#             messages.append(json.dumps({"key": i[0], "value": i[0]}))
#             i[0] += 1
#         for msg_id in range(messages_num):
#             channel.basic_publish(
#                 exchange="many_consumers",
#                 routing_key="",
#                 properties=pika.BasicProperties(message_id=str(msg_id)),
#                 body=messages[msg_id],
#             )
#         connection.close()
#
#     threads = []
#     threads_num = 20
#
#     for _ in range(threads_num):
#         threads.append(threading.Thread(target=produce))
#     for thread in threads:
#         time.sleep(random.uniform(0, 1))
#         thread.start()
#
#     result1 = ""
#     while True:
#         result1 = instance.query("SELECT count() FROM test.destination")
#         time.sleep(1)
#         if int(result1) == messages_num * threads_num:
#             break
#
#     result2 = instance.query("SELECT count(DISTINCT channel_id) FROM test.destination")
#
#     for thread in threads:
#         thread.join()
#
#     for consumer_id in range(num_tables):
#         instance.query(
#             """
#             DROP TABLE test.many_consumers_{0};
#             DROP TABLE test.many_consumers_{0}_mv;
#         """.format(
#                 consumer_id
#             )
#         )
#
#     instance.query(
#         """
#         DROP TABLE test.destination;
#     """
#     )
#
#     assert (
#         int(result1) == messages_num * threads_num
#     ), "ClickHouse lost some messages: {}".format(result)
#     # 4 tables, 2 consumers for each table => 8 consumer tags
#     assert int(result2) == 8
#
#
# def test_redis_restore_failed_connection_without_losses_1(redis_cluster):
#     instance.query(
#         """
#         DROP TABLE IF EXISTS test.consume;
#         CREATE TABLE test.view (key UInt64, value UInt64)
#             ENGINE = MergeTree
#             ORDER BY key;
#         CREATE TABLE test.consume (key UInt64, value UInt64)
#             ENGINE = Redis
#             SETTINGS redis_broker = 'redis1:6379',
#                      redis_stream_list = 'producer_reconnect',
#                      redis_format = 'JSONEachRow',
#                      redis_num_consumers = 2,
#                      redis_row_delimiter = '\\n';
#         CREATE MATERIALIZED VIEW test.consumer TO test.view AS
#             SELECT * FROM test.consume;
#         DROP TABLE IF EXISTS test.producer_reconnect;
#         CREATE TABLE test.producer_reconnect (key UInt64, value UInt64)
#             ENGINE = Redis
#             SETTINGS redis_broker = 'redis1:6379',
#                      redis_stream_list = 'producer_reconnect',
#                      redis_persistent = '1',
#                      redis_format = 'JSONEachRow',
#                      redis_row_delimiter = '\\n';
#     """
#     )
#
#     credentials = pika.PlainCredentials("root", "clickhouse")
#     parameters = pika.ConnectionParameters(
#         redis_cluster.redis_ip, redis_cluster.redis_port, "/", credentials
#     )
#     connection = pika.BlockingConnection(parameters)
#     channel = connection.channel()
#
#     messages_num = 100000
#     values = []
#     for i in range(messages_num):
#         values.append("({i}, {i})".format(i=i))
#     values = ",".join(values)
#
#     while True:
#         try:
#             instance.query(
#                 "INSERT INTO test.producer_reconnect VALUES {}".format(values)
#             )
#             break
#         except QueryRuntimeException as e:
#             if "Local: Timed out." in str(e):
#                 continue
#             else:
#                 raise
#
#     while int(instance.query("SELECT count() FROM test.view")) == 0:
#         time.sleep(0.1)
#
#     kill_redis(redis_cluster.redis_docker_id)
#     time.sleep(4)
#     revive_redis(redis_cluster.redis_docker_id)
#
#     while True:
#         result = instance.query("SELECT count(DISTINCT key) FROM test.view")
#         time.sleep(1)
#         if int(result) == messages_num:
#             break
#
#     instance.query(
#         """
#         DROP TABLE test.consume;
#         DROP TABLE test.producer_reconnect;
#     """
#     )
#
#     assert int(result) == messages_num, "ClickHouse lost some messages: {}".format(
#         result
#     )
#
#
# def test_redis_restore_failed_connection_without_losses_2(redis_cluster):
#     instance.query(
#         """
#         CREATE TABLE test.consumer_reconnect (key UInt64, value UInt64)
#             ENGINE = Redis
#             SETTINGS redis_broker = 'redis1:6379',
#                      redis_stream_list = 'consumer_reconnect',
#                      redis_num_consumers = 10,
#                      redis_num_queues = 10,
#                      redis_format = 'JSONEachRow',
#                      redis_row_delimiter = '\\n';
#     """
#     )
#
#     i = 0
#     messages_num = 150000
#
#     credentials = pika.PlainCredentials("root", "clickhouse")
#     parameters = pika.ConnectionParameters(
#         redis_cluster.redis_ip, redis_cluster.redis_port, "/", credentials
#     )
#
#     connection = pika.BlockingConnection(parameters)
#     channel = connection.channel()
#     messages = []
#     for _ in range(messages_num):
#         messages.append(json.dumps({"key": i, "value": i}))
#         i += 1
#     for msg_id in range(messages_num):
#         channel.basic_publish(
#             exchange="consumer_reconnect",
#             routing_key="",
#             body=messages[msg_id],
#             properties=pika.BasicProperties(delivery_mode=2, message_id=str(msg_id)),
#         )
#     connection.close()
#     instance.query(
#         """
#         CREATE TABLE test.view (key UInt64, value UInt64)
#             ENGINE = MergeTree
#             ORDER BY key;
#         CREATE MATERIALIZED VIEW test.consumer TO test.view AS
#             SELECT * FROM test.consumer_reconnect;
#     """
#     )
#
#     while int(instance.query("SELECT count() FROM test.view")) == 0:
#         print(3)
#         time.sleep(0.1)
#
#     kill_redis(redis_cluster.redis_docker_id)
#     time.sleep(8)
#     revive_redis(redis_cluster.redis_docker_id)
#
#     # while int(instance.query('SELECT count() FROM test.view')) == 0:
#     #    time.sleep(0.1)
#
#     # kill_redis()
#     # time.sleep(2)
#     # revive_redis()
#
#     while True:
#         result = instance.query("SELECT count(DISTINCT key) FROM test.view")
#         time.sleep(1)
#         if int(result) == messages_num:
#             break
#
#     instance.query(
#         """
#         DROP TABLE test.consumer;
#         DROP TABLE test.consumer_reconnect;
#     """
#     )
#
#     assert int(result) == messages_num, "ClickHouse lost some messages: {}".format(
#         result
#     )
#
#
# def test_redis_commit_on_block_write(redis_cluster):
#     instance.query(
#         """
#         CREATE TABLE test.redis (key UInt64, value UInt64)
#             ENGINE = Redis
#             SETTINGS redis_broker = 'redis1:6379',
#                      redis_stream_list = 'block',
#                      redis_format = 'JSONEachRow',
#                      redis_queue_base = 'block',
#                      redis_max_block_size = 100,
#                      redis_row_delimiter = '\\n';
#         CREATE TABLE test.view (key UInt64, value UInt64)
#             ENGINE = MergeTree()
#             ORDER BY key;
#         CREATE MATERIALIZED VIEW test.consumer TO test.view AS
#             SELECT * FROM test.redis;
#     """
#     )
#
#     credentials = pika.PlainCredentials("root", "clickhouse")
#     parameters = pika.ConnectionParameters(
#         redis_cluster.redis_ip, redis_cluster.redis_port, "/", credentials
#     )
#     connection = pika.BlockingConnection(parameters)
#     channel = connection.channel()
#
#     cancel = threading.Event()
#
#     i = [0]
#
#     def produce():
#         while not cancel.is_set():
#             messages = []
#             for _ in range(101):
#                 messages.append(json.dumps({"key": i[0], "value": i[0]}))
#                 i[0] += 1
#             for message in messages:
#                 channel.basic_publish(exchange="block", routing_key="", body=message)
#
#     redis_thread = threading.Thread(target=produce)
#     redis_thread.start()
#
#     while int(instance.query("SELECT count() FROM test.view")) == 0:
#         time.sleep(1)
#
#     cancel.set()
#
#     instance.query("DETACH TABLE test.redis;")
#
#     while (
#         int(
#             instance.query(
#                 "SELECT count() FROM system.tables WHERE database='test' AND name='redis'"
#             )
#         )
#         == 1
#     ):
#         time.sleep(1)
#
#     instance.query("ATTACH TABLE test.redis;")
#
#     while int(instance.query("SELECT uniqExact(key) FROM test.view")) < i[0]:
#         time.sleep(1)
#
#     result = int(instance.query("SELECT count() == uniqExact(key) FROM test.view"))
#
#     instance.query(
#         """
#         DROP TABLE test.consumer;
#         DROP TABLE test.view;
#     """
#     )
#
#     redis_thread.join()
#     connection.close()
#
#     assert result == 1, "Messages from redis get duplicated!"
#
#
# def test_redis_no_connection_at_startup_1(redis_cluster):
#     # no connection when table is initialized
#     redis_cluster.pause_container("redis1")
#     instance.query_and_get_error(
#         """
#         CREATE TABLE test.cs (key UInt64, value UInt64)
#             ENGINE = Redis
#             SETTINGS redis_broker = 'redis1:6379',
#                      redis_stream_list = 'cs',
#                      redis_format = 'JSONEachRow',
#                      redis_num_consumers = '5',
#                      redis_row_delimiter = '\\n';
#     """
#     )
#     redis_cluster.unpause_container("redis1")
#
#
# def test_redis_no_connection_at_startup_2(redis_cluster):
#     instance.query(
#         """
#         CREATE TABLE test.cs (key UInt64, value UInt64)
#             ENGINE = Redis
#             SETTINGS redis_broker = 'redis1:6379',
#                      redis_stream_list = 'cs',
#                      redis_format = 'JSONEachRow',
#                      redis_num_consumers = '5',
#                      redis_row_delimiter = '\\n';
#         CREATE TABLE test.view (key UInt64, value UInt64)
#             ENGINE = MergeTree
#             ORDER BY key;
#         CREATE MATERIALIZED VIEW test.consumer TO test.view AS
#             SELECT * FROM test.cs;
#     """
#     )
#     instance.query("DETACH TABLE test.cs")
#     redis_cluster.pause_container("redis1")
#     instance.query("ATTACH TABLE test.cs")
#     redis_cluster.unpause_container("redis1")
#
#     messages_num = 1000
#     credentials = pika.PlainCredentials("root", "clickhouse")
#     parameters = pika.ConnectionParameters(
#         redis_cluster.redis_ip, redis_cluster.redis_port, "/", credentials
#     )
#     connection = pika.BlockingConnection(parameters)
#     channel = connection.channel()
#     for i in range(messages_num):
#         message = json.dumps({"key": i, "value": i})
#         channel.basic_publish(
#             exchange="cs",
#             routing_key="",
#             body=message,
#             properties=pika.BasicProperties(delivery_mode=2, message_id=str(i)),
#         )
#     connection.close()
#
#     while True:
#         result = instance.query("SELECT count() FROM test.view")
#         time.sleep(1)
#         if int(result) == messages_num:
#             break
#
#     instance.query(
#         """
#         DROP TABLE test.consumer;
#         DROP TABLE test.cs;
#     """
#     )
#
#     assert int(result) == messages_num, "ClickHouse lost some messages: {}".format(
#         result
#     )
#
#
# def test_redis_format_factory_settings(redis_cluster):
#     instance.query(
#         """
#         CREATE TABLE test.format_settings (
#             id String, date DateTime
#         ) ENGINE = Redis
#             SETTINGS redis_broker = 'redis1:6379',
#                      redis_stream_list = 'format_settings',
#                      redis_format = 'JSONEachRow',
#                      date_time_input_format = 'best_effort';
#         """
#     )
#
#     credentials = pika.PlainCredentials("root", "clickhouse")
#     parameters = pika.ConnectionParameters(
#         redis_cluster.redis_ip, redis_cluster.redis_port, "/", credentials
#     )
#     connection = pika.BlockingConnection(parameters)
#     channel = connection.channel()
#
#     message = json.dumps(
#         {"id": "format_settings_test", "date": "2021-01-19T14:42:33.1829214Z"}
#     )
#     expected = instance.query(
#         """SELECT parseDateTimeBestEffort(CAST('2021-01-19T14:42:33.1829214Z', 'String'))"""
#     )
#
#     channel.basic_publish(exchange="format_settings", routing_key="", body=message)
#     result = ""
#     while True:
#         result = instance.query("SELECT date FROM test.format_settings")
#         if result == expected:
#             break
#
#     instance.query(
#         """
#         CREATE TABLE test.view (
#             id String, date DateTime
#         ) ENGINE = MergeTree ORDER BY id;
#         CREATE MATERIALIZED VIEW test.consumer TO test.view AS
#             SELECT * FROM test.format_settings;
#         """
#     )
#
#     channel.basic_publish(exchange="format_settings", routing_key="", body=message)
#     result = ""
#     while True:
#         result = instance.query("SELECT date FROM test.view")
#         if result == expected:
#             break
#
#     connection.close()
#     instance.query(
#         """
#         DROP TABLE test.consumer;
#         DROP TABLE test.format_settings;
#     """
#     )
#
#     assert result == expected
#
#
# def test_redis_vhost(redis_cluster):
#     instance.query(
#         """
#         CREATE TABLE test.redis_vhost (key UInt64, value UInt64)
#             ENGINE = Redis
#             SETTINGS redis_broker = 'redis1:6379',
#                      redis_stream_list = 'vhost',
#                      redis_format = 'JSONEachRow',
#                      redis_vhost = '/'
#         """
#     )
#
#     credentials = pika.PlainCredentials("root", "clickhouse")
#     parameters = pika.ConnectionParameters(
#         redis_cluster.redis_ip, redis_cluster.redis_port, "/", credentials
#     )
#     connection = pika.BlockingConnection(parameters)
#     channel = connection.channel()
#     channel.basic_publish(
#         exchange="vhost", routing_key="", body=json.dumps({"key": 1, "value": 2})
#     )
#     connection.close()
#     while True:
#         result = instance.query(
#             "SELECT * FROM test.redis_vhost ORDER BY key", ignore_error=True
#         )
#         if result == "1\t2\n":
#             break
#
#
# def test_redis_drop_table_properly(redis_cluster):
#     instance.query(
#         """
#         CREATE TABLE test.redis_drop (key UInt64, value UInt64)
#             ENGINE = Redis
#             SETTINGS redis_broker = 'redis1:6379',
#                      redis_stream_list = 'drop',
#                      redis_format = 'JSONEachRow',
#                      redis_queue_base = 'rabbit_queue_drop'
#         """
#     )
#
#     credentials = pika.PlainCredentials("root", "clickhouse")
#     parameters = pika.ConnectionParameters(
#         redis_cluster.redis_ip, redis_cluster.redis_port, "/", credentials
#     )
#     connection = pika.BlockingConnection(parameters)
#     channel = connection.channel()
#
#     channel.basic_publish(
#         exchange="drop", routing_key="", body=json.dumps({"key": 1, "value": 2})
#     )
#     while True:
#         result = instance.query(
#             "SELECT * FROM test.redis_drop ORDER BY key", ignore_error=True
#         )
#         if result == "1\t2\n":
#             break
#
#     exists = channel.queue_declare(queue="rabbit_queue_drop", passive=True)
#     assert exists
#
#     instance.query("DROP TABLE test.redis_drop")
#     time.sleep(30)
#
#     try:
#         exists = channel.queue_declare(
#             callback, queue="rabbit_queue_drop", passive=True
#         )
#     except Exception as e:
#         exists = False
#
#     assert not exists
#
#
# def test_redis_queue_settings(redis_cluster):
#     instance.query(
#         """
#         CREATE TABLE test.redis_settings (key UInt64, value UInt64)
#             ENGINE = Redis
#             SETTINGS redis_broker = 'redis1:6379',
#                      redis_stream_list = 'rabbit_exchange',
#                      redis_format = 'JSONEachRow',
#                      redis_queue_base = 'rabbit_queue_settings',
#                      redis_queue_settings_list = 'x-max-length=10,x-overflow=reject-publish'
#         """
#     )
#
#     credentials = pika.PlainCredentials("root", "clickhouse")
#     parameters = pika.ConnectionParameters(
#         redis_cluster.redis_ip, redis_cluster.redis_port, "/", credentials
#     )
#     connection = pika.BlockingConnection(parameters)
#     channel = connection.channel()
#
#     for i in range(50):
#         channel.basic_publish(
#             exchange="rabbit_exchange",
#             routing_key="",
#             body=json.dumps({"key": 1, "value": 2}),
#         )
#     connection.close()
#
#     instance.query(
#         """
#         CREATE TABLE test.view (key UInt64, value UInt64)
#         ENGINE = MergeTree ORDER BY key;
#         CREATE MATERIALIZED VIEW test.consumer TO test.view AS
#             SELECT * FROM test.redis_settings;
#         """
#     )
#
#     time.sleep(5)
#
#     result = instance.query(
#         "SELECT count() FROM test.redis_settings", ignore_error=True
#     )
#     while int(result) != 10:
#         time.sleep(0.5)
#         result = instance.query("SELECT count() FROM test.view", ignore_error=True)
#
#     instance.query("DROP TABLE test.redis_settings")
#
#     # queue size is 10, but 50 messages were sent, they will be dropped (setting x-overflow = reject-publish) and only 10 will remain.
#     assert int(result) == 10
#
#
# def test_redis_queue_consume(redis_cluster):
#     credentials = pika.PlainCredentials("root", "clickhouse")
#     parameters = pika.ConnectionParameters(
#         redis_cluster.redis_ip, redis_cluster.redis_port, "/", credentials
#     )
#     connection = pika.BlockingConnection(parameters)
#     channel = connection.channel()
#     channel.queue_declare(queue="rabbit_queue", durable=True)
#
#     i = [0]
#     messages_num = 1000
#
#     def produce():
#         connection = pika.BlockingConnection(parameters)
#         channel = connection.channel()
#         messages = []
#         for _ in range(messages_num):
#             message = json.dumps({"key": i[0], "value": i[0]})
#             channel.basic_publish(exchange="", routing_key="rabbit_queue", body=message)
#             i[0] += 1
#
#     threads = []
#     threads_num = 10
#     for _ in range(threads_num):
#         threads.append(threading.Thread(target=produce))
#     for thread in threads:
#         time.sleep(random.uniform(0, 1))
#         thread.start()
#
#     instance.query(
#         """
#         CREATE TABLE test.redis_queue (key UInt64, value UInt64)
#             ENGINE = Redis
#             SETTINGS redis_broker = 'redis1:6379',
#                      redis_format = 'JSONEachRow',
#                      redis_queue_base = 'rabbit_queue',
#                      redis_queue_consume = 1;
#         CREATE TABLE test.view (key UInt64, value UInt64)
#         ENGINE = MergeTree ORDER BY key;
#         CREATE MATERIALIZED VIEW test.consumer TO test.view AS
#             SELECT * FROM test.redis_queue;
#         """
#     )
#
#     result = ""
#     while True:
#         result = instance.query("SELECT count() FROM test.view")
#         if int(result) == messages_num * threads_num:
#             break
#         time.sleep(1)
#
#     for thread in threads:
#         thread.join()
#
#     instance.query("DROP TABLE test.redis_queue")
#
#
# def test_redis_produce_consume_avro(redis_cluster):
#     num_rows = 75
#
#     instance.query(
#         """
#         DROP TABLE IF EXISTS test.view;
#         DROP TABLE IF EXISTS test.rabbit;
#         DROP TABLE IF EXISTS test.rabbit_writer;
#
#         CREATE TABLE test.rabbit_writer (key UInt64, value UInt64)
#             ENGINE = Redis
#             SETTINGS redis_broker = 'redis1:6379',
#                      redis_format = 'Avro',
#                      redis_stream_list = 'avro',
#                      redis_exchange_type = 'direct',
#                      redis_routing_key_list = 'avro';
#
#         CREATE TABLE test.rabbit (key UInt64, value UInt64)
#             ENGINE = Redis
#             SETTINGS redis_broker = 'redis1:6379',
#                      redis_format = 'Avro',
#                      redis_stream_list = 'avro',
#                      redis_exchange_type = 'direct',
#                      redis_routing_key_list = 'avro';
#
#         CREATE MATERIALIZED VIEW test.view Engine=Log AS
#             SELECT key, value FROM test.rabbit;
#     """
#     )
#
#     instance.query(
#         "INSERT INTO test.rabbit_writer select number*10 as key, number*100 as value from numbers({num_rows}) SETTINGS output_format_avro_rows_in_file = 7".format(
#             num_rows=num_rows
#         )
#     )
#
#     # Ideally we should wait for an event
#     time.sleep(3)
#
#     expected_num_rows = instance.query(
#         "SELECT COUNT(1) FROM test.view", ignore_error=True
#     )
#     assert int(expected_num_rows) == num_rows
#
#     expected_max_key = instance.query(
#         "SELECT max(key) FROM test.view", ignore_error=True
#     )
#     assert int(expected_max_key) == (num_rows - 1) * 10
#
#
# def test_redis_bad_args(redis_cluster):
#     credentials = pika.PlainCredentials("root", "clickhouse")
#     parameters = pika.ConnectionParameters(
#         redis_cluster.redis_ip, redis_cluster.redis_port, "/", credentials
#     )
#     connection = pika.BlockingConnection(parameters)
#     channel = connection.channel()
#     channel.exchange_declare(exchange="f", exchange_type="fanout")
#     instance.query_and_get_error(
#         """
#         CREATE TABLE test.drop (key UInt64, value UInt64)
#             ENGINE = Redis
#             SETTINGS redis_broker = 'redis1:6379',
#                      redis_stream_list = 'f',
#                      redis_format = 'JSONEachRow';
#     """
#     )
#
#
#
# def test_redis_drop_mv(redis_cluster):
#     instance.query(
#         """
#         CREATE TABLE test.redis (key UInt64, value UInt64)
#             ENGINE = Redis
#             SETTINGS redis_broker = 'redis1:6379',
#                      redis_stream_list = 'mv',
#                      redis_format = 'JSONEachRow',
#                      redis_queue_base = 'drop_mv';
#         CREATE TABLE test.view (key UInt64, value UInt64)
#             ENGINE = MergeTree()
#             ORDER BY key;
#         CREATE MATERIALIZED VIEW test.consumer TO test.view AS
#             SELECT * FROM test.redis;
#     """
#     )
#
#     credentials = pika.PlainCredentials("root", "clickhouse")
#     parameters = pika.ConnectionParameters(
#         redis_cluster.redis_ip, redis_cluster.redis_port, "/", credentials
#     )
#     connection = pika.BlockingConnection(parameters)
#     channel = connection.channel()
#
#     messages = []
#     for i in range(20):
#         channel.basic_publish(
#             exchange="mv", routing_key="", body=json.dumps({"key": i, "value": i})
#         )
#
#     instance.query("DROP VIEW test.consumer")
#     for i in range(20, 40):
#         channel.basic_publish(
#             exchange="mv", routing_key="", body=json.dumps({"key": i, "value": i})
#         )
#
#     instance.query(
#         """
#         CREATE MATERIALIZED VIEW test.consumer TO test.view AS
#             SELECT * FROM test.redis;
#     """
#     )
#     for i in range(40, 50):
#         channel.basic_publish(
#             exchange="mv", routing_key="", body=json.dumps({"key": i, "value": i})
#         )
#
#     while True:
#         result = instance.query("SELECT * FROM test.view ORDER BY key")
#         if redis_check_result(result):
#             break
#
#     redis_check_result(result, True)
#
#     instance.query("DROP VIEW test.consumer")
#     for i in range(50, 60):
#         channel.basic_publish(
#             exchange="mv", routing_key="", body=json.dumps({"key": i, "value": i})
#         )
#     connection.close()
#
#     count = 0
#     while True:
#         count = int(instance.query("SELECT count() FROM test.redis"))
#         if count:
#             break
#
#     assert count > 0
#
#
# def test_redis_random_detach(redis_cluster):
#     NUM_CONSUMERS = 2
#     NUM_QUEUES = 2
#     instance.query(
#         """
#         CREATE TABLE test.redis (key UInt64, value UInt64)
#             ENGINE = Redis
#             SETTINGS redis_broker = 'redis1:6379',
#                      redis_stream_list = 'random',
#                      redis_queue_base = 'random',
#                      redis_num_queues = 2,
#                      redis_num_consumers = 2,
#                      redis_format = 'JSONEachRow';
#         CREATE TABLE test.view (key UInt64, value UInt64, channel_id String)
#             ENGINE = MergeTree
#             ORDER BY key;
#         CREATE MATERIALIZED VIEW test.consumer TO test.view AS
#             SELECT *, _channel_id AS channel_id FROM test.redis;
#     """
#     )
#
#     i = [0]
#     messages_num = 10000
#
#     credentials = pika.PlainCredentials("root", "clickhouse")
#     parameters = pika.ConnectionParameters(
#         redis_cluster.redis_ip, redis_cluster.redis_port, "/", credentials
#     )
#
#     def produce():
#         connection = pika.BlockingConnection(parameters)
#         channel = connection.channel()
#
#         messages = []
#         for i in range(messages_num):
#             messages.append(json.dumps({"key": i[0], "value": i[0]}))
#             i[0] += 1
#             mes_id = str(i)
#             channel.basic_publish(
#                 exchange="test_sharding",
#                 routing_key="",
#                 properties=pika.BasicProperties(message_id=mes_id),
#                 body=message,
#             )
#         connection.close()
#
#     threads = []
#     threads_num = 20
#
#     for _ in range(threads_num):
#         threads.append(threading.Thread(target=produce))
#     for thread in threads:
#         time.sleep(random.uniform(0, 1))
#         thread.start()
#
#     # time.sleep(5)
#     # kill_redis(redis_cluster.redis_docker_id)
#     # instance.query("detach table test.redis")
#     # revive_redis(redis_cluster.redis_docker_id)
#
#     for thread in threads:
#         thread.join()
#
#
# def test_redis_predefined_configuration(redis_cluster):
#     credentials = pika.PlainCredentials("root", "clickhouse")
#     parameters = pika.ConnectionParameters(
#         redis_cluster.redis_ip, redis_cluster.redis_port, "/", credentials
#     )
#     connection = pika.BlockingConnection(parameters)
#     channel = connection.channel()
#
#     instance.query(
#         """
#         CREATE TABLE test.redis (key UInt64, value UInt64)
#             ENGINE = Redis(rabbit1, redis_vhost = '/') """
#     )
#
#     channel.basic_publish(
#         exchange="named", routing_key="", body=json.dumps({"key": 1, "value": 2})
#     )
#     while True:
#         result = instance.query(
#             "SELECT * FROM test.redis ORDER BY key", ignore_error=True
#         )
#         if result == "1\t2\n":
#             break


if __name__ == "__main__":
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
