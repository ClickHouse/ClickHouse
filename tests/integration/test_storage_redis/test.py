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
from helpers.cluster import ClickHouseCluster, check_redis_is_available
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
            if check_redis_is_available(redis_docker_id):
                logging.debug("Redis is available")
                return
            time.sleep(0.5)
        except Exception as ex:
            logging.debug("Can't connect to Redis " + str(ex))
            time.sleep(0.5)


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
            ENGINE = RedisStreams
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
            ENGINE = RedisStreams
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
            ENGINE = RedisStreams
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
            ENGINE = RedisStreams
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
            ENGINE = RedisStreams
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
            ENGINE = RedisStreams
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
            ENGINE = RedisStreams
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
            ENGINE = RedisStreams
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

    while True:
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
            ENGINE = RedisStreams
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

    time.sleep(2)
    for mv_id in range(NUM_MV):
        instance.query("SELECT count() FROM test.combo_{0}".format(mv_id))

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
    threads_num = NUM_STREAMS * 5

    for j in range(threads_num):
        threads.append(threading.Thread(target=produce, args=(streams[j % len(streams)],)))

    for thread in threads:
        time.sleep(random.uniform(0, 1))
        thread.start()

    for _ in range(20):
        result = 0
        for mv_id in range(NUM_MV):
            num = int(
                instance.query("SELECT count() FROM test.combo_{0}".format(mv_id))
            )
            logging.warning(str(num) + '\n')
            result += num
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
            ENGINE = RedisStreams
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

    while True:
        insert_messages = connection.xread({stream_name: "0-0"}, count=50)

        # xread returns list of lists of topics and messages - select first topic and its messages
        insert_messages = insert_messages[0][1]
        logging.warning(insert_messages)
        if len(insert_messages) == 50:
            break

    result = "\n".join(map(lambda x: x[1]["key".encode()].decode() + "\t" + x[1]["value".encode()].decode(), insert_messages))
    redis_check_result(result, True)


def test_redis_insert_into_table_with_many_streams_wrong(redis_cluster):
    stream_names = ['insert_many_streams_wrong1', 'insert_many_streams_wrong2']
    group_name = 'test_insert_many_streams_wrong'
    connection = redis.Redis(redis_cluster.redis_ip, port=redis_cluster.redis_port, password="clickhouse")
    for stream_name in stream_names:
        connection.xgroup_create(stream_name, group_name, '$', mkstream=True)

    instance.query(
        """
        CREATE TABLE test.redis (key UInt64, value UInt64)
            ENGINE = RedisStreams
            SETTINGS redis_broker = '{}:6379',
                     redis_stream_list = '{}',
                     redis_group_name = '{}';
        """.format(
            redis_cluster.redis_host,
            ','.join(stream_names),
            group_name
        )
    )

    values = []
    for i in range(50):
        values.append("({i}, {i})".format(i=i))
    values = ",".join(values)

    instance.query_and_get_error("INSERT INTO test.redis VALUES {}".format(values))


def test_redis_insert_into_table_with_many_streams_right(redis_cluster):
    stream_names = ['insert_many_streams_right1', 'insert_many_streams_right2']
    group_name = 'test_insert_many_streams_right'
    connection = redis.Redis(redis_cluster.redis_ip, port=redis_cluster.redis_port, password="clickhouse")
    for stream_name in stream_names:
        connection.xgroup_create(stream_name, group_name, '$', mkstream=True)

    instance.query(
        """
        CREATE TABLE test.redis (key UInt64, value UInt64)
            ENGINE = RedisStreams
            SETTINGS redis_broker = '{}:6379',
                     redis_stream_list = '{}',
                     redis_group_name = '{}';
        """.format(
            redis_cluster.redis_host,
            ','.join(stream_names),
            group_name
        )
    )

    values = []
    for i in range(50):
        values.append("({i}, {i})".format(i=i))
    values = ",".join(values)

    while True:
        try:
            instance.query("INSERT INTO test.redis SETTINGS stream_like_engine_insert_queue = 'insert_many_stream_rights1' VALUES {}".format(values))
            break
        except QueryRuntimeException as e:
            if "Local: Timed out." in str(e):
                continue
            else:
                raise

    while True:
        insert_messages = connection.xread({'insert_many_stream_rights1': "0-0"}, count=50)

        # xread returns list of lists of topics and messages - select first topic and its messages
        insert_messages = insert_messages[0][1]
        if len(insert_messages) == 50:
            break

    result = "\n".join(
        map(lambda x: x[1]["key".encode()].decode() + "\t" + x[1]["value".encode()].decode(), insert_messages))
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
            ENGINE = RedisStreams
            SETTINGS redis_broker = '{0}:6379',
                     redis_stream_list = '{1}',
                     redis_group_name = '{2}';
        CREATE TABLE test.redis_consume (key UInt64, value UInt64)
            ENGINE = RedisStreams
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
            ENGINE = RedisStreams
            SETTINGS redis_broker = '{0}',
                     redis_stream_list = '{1}',
                     redis_num_consumers = 5,
                     redis_max_block_size = 10000,
                     redis_group_name = '{2}';
        CREATE TABLE test.redis_overload (key UInt64, value UInt64)
            ENGINE = RedisStreams
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


def test_redis_virtual_columns(redis_cluster):
    stream_name = 'virtuals'
    group_name = 'test_virtuals'
    connection = redis.Redis(redis_cluster.redis_ip, port=redis_cluster.redis_port, password="clickhouse")
    connection.xgroup_create(stream_name, group_name, '$', mkstream=True)

    instance.query(
        """
        CREATE TABLE test.redis_virtuals (key UInt64, value UInt64)
            ENGINE = RedisStreams
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
            ENGINE = RedisStreams
            SETTINGS redis_broker = '{}:6379',
                     redis_stream_list = '{}',
                     redis_group_name = '{}';
        CREATE TABLE test.view (key UInt64, value UInt64,
            stream String, message_id String, timestamp UInt8, sequence_number UInt8) ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
        SELECT *, _stream as stream, _key as message_id, _timestamp as timestamp, _sequence_number as sequence_number
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
        "SELECT key, value, stream, message_id, timestamp, sequence_number FROM test.view ORDER BY key"
    )
    expected = """\
0	0	virtuals_mv	1-0	1	0
1	1	virtuals_mv	2-0	2	0
2	2	virtuals_mv	3-0	3	0
3	3	virtuals_mv	4-0	4	0
4	4	virtuals_mv	5-0	5	0
5	5	virtuals_mv	6-0	6	0
6	6	virtuals_mv	7-0	7	0
7	7	virtuals_mv	8-0	8	0
8	8	virtuals_mv	9-0	9	0
9	9	virtuals_mv	10-0	10	0
"""

    instance.query(
        """
        DROP TABLE test.consumer;
        DROP TABLE test.view;
        DROP TABLE test.redis_virtuals_mv
    """
    )

    assert TSV(result) == TSV(expected)


def test_redis_many_consumers_to_each_stream(redis_cluster):
    NUM_STREAMS = 2
    NUM_TABLES = 4

    streams = []
    group_name = 'test_many_consumers'
    connection = redis.Redis(redis_cluster.redis_ip, port=redis_cluster.redis_port, password="clickhouse")
    for i in range(NUM_STREAMS):
        streams.append('many_consumers_{}'.format(i))

    for stream in streams:
        connection.xgroup_create(stream, group_name, '$', mkstream=True)

    instance.query(
        """
        DROP TABLE IF EXISTS test.destination;
        CREATE TABLE test.destination(key UInt64, value UInt64, consumer_id String)
        ENGINE = MergeTree()
        ORDER BY key;
    """
    )
    query = ""
    for table_id in range(NUM_TABLES):
        query += """
            DROP TABLE IF EXISTS test.many_consumers_{0};
            DROP TABLE IF EXISTS test.many_consumers_{0}_mv;
            CREATE TABLE test.many_consumers_{0} (key UInt64, value UInt64)
                ENGINE = RedisStreams
                SETTINGS redis_broker = '{1}:6379',
                        redis_stream_list = '{2}',
                        redis_group_name = '{3}',
                        redis_num_consumers = 2,
                        redis_max_block_size = 500;
            CREATE MATERIALIZED VIEW test.many_consumers_{0}_mv TO test.destination AS
            SELECT key, value, _consumer AS consumer_id FROM test.many_consumers_{0};
        """.format(
                table_id,
                redis_cluster.redis_host,
                ','.join(streams),
                group_name,
            )

    instance.query(query)

    i = [0]
    messages_num = 5000

    def produce(stream_name):
        messages = []
        for _ in range(messages_num):
            messages.append({"key": i[0], "value": i[0]})
            i[0] += 1

        for message in messages:
            connection.xadd(stream_name, message)
        connection.close()

    threads = []
    threads_num = 5 * NUM_STREAMS

    for j in range(threads_num):
        threads.append(threading.Thread(target=produce, args=(streams[j % len(streams)],)))
    for thread in threads:
        time.sleep(random.uniform(0, 1))
        thread.start()

    while True:
        result1 = instance.query("SELECT count() FROM test.destination")
        time.sleep(1)
        if int(result1) == messages_num * threads_num:
            break

    result2 = instance.query("SELECT count(DISTINCT consumer_id) FROM test.destination")

    for thread in threads:
        thread.join()

    for consumer_id in range(NUM_TABLES):
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


def test_redis_many_consumers_with_threads_to_each_stream(redis_cluster):
    NUM_STREAMS = 2
    NUM_TABLES = 4

    streams = []
    group_name = 'test_many_consumers_with_threads'
    connection = redis.Redis(redis_cluster.redis_ip, port=redis_cluster.redis_port, password="clickhouse")
    for i in range(NUM_STREAMS):
        streams.append('many_consumers_with_threads_{}'.format(i))

    for stream in streams:
        connection.xgroup_create(stream, group_name, '$', mkstream=True)

    instance.query(
        """
        DROP TABLE IF EXISTS test.destination;
        CREATE TABLE test.destination(key UInt64, value UInt64, consumer_id String)
        ENGINE = MergeTree()
        ORDER BY key;
    """
    )
    query = ""
    for table_id in range(NUM_TABLES):
        query += """
            DROP TABLE IF EXISTS test.many_consumers_with_threads_{0};
            DROP TABLE IF EXISTS test.many_consumers_with_threads_{0}_mv;
            CREATE TABLE test.many_consumers_with_threads_{0} (key UInt64, value UInt64)
                ENGINE = RedisStreams
                SETTINGS redis_broker = '{1}:6379',
                        redis_stream_list = '{2}',
                        redis_group_name = '{3}',
                        redis_num_consumers = 2,
                        redis_thread_per_consumer = true;
            CREATE MATERIALIZED VIEW test.many_consumers_with_threads_{0}_mv TO test.destination AS
            SELECT key, value, _consumer AS consumer_id FROM test.many_consumers_with_threads_{0};
        """.format(
                table_id,
                redis_cluster.redis_host,
                ','.join(streams),
                group_name,
            )

    instance.query(query)

    i = [0]
    messages_num = 5000

    def produce(stream_name):
        messages = []
        for _ in range(messages_num):
            messages.append({"key": i[0], "value": i[0]})
            i[0] += 1

        for message in messages:
            connection.xadd(stream_name, message)
        connection.close()

    threads = []
    threads_num = 5 * NUM_STREAMS

    for j in range(threads_num):
        threads.append(threading.Thread(target=produce, args=(streams[j % len(streams)],)))
    for thread in threads:
        time.sleep(random.uniform(0, 1))
        thread.start()

    while True:
        result1 = instance.query("SELECT count() FROM test.destination")
        time.sleep(1)
        if int(result1) == messages_num * threads_num:
            break

    result2 = instance.query("SELECT count(DISTINCT consumer_id) FROM test.destination")

    for thread in threads:
        thread.join()

    for consumer_id in range(NUM_TABLES):
        instance.query(
            """
            DROP TABLE test.many_consumers_with_threads_{0};
            DROP TABLE test.many_consumers_with_threads_{0}_mv;
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


def test_redis_restore_failed_connection_without_losses(redis_cluster):
    stream_name = 'with_losses'
    group_name = 'test_with_losses'
    connection = redis.Redis(redis_cluster.redis_ip, port=redis_cluster.redis_port, password="clickhouse")
    connection.xgroup_create(stream_name, group_name, '$', mkstream=True)

    instance.query(
        """
        CREATE TABLE test.consumer_reconnect (key UInt64, value UInt64)
            ENGINE = RedisStreams
            SETTINGS redis_broker = '{}:6379',
                     redis_stream_list = '{}',
                     redis_group_name = '{}',
                     redis_num_consumers = 10;
        """.format(
                redis_cluster.redis_host,
                stream_name,
                group_name
            )
    )

    messages_num = 150000

    for i in range(messages_num):
        connection.xadd(stream_name, {"key": i, "value": i})
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

    while int(instance.query("SELECT count() FROM test.view")) == 0:
        time.sleep(0.1)

    kill_redis(redis_cluster.redis_docker_id)
    time.sleep(8)
    revive_redis(redis_cluster.redis_docker_id)

    while True:
        result = instance.query("SELECT count(DISTINCT key) FROM test.view")
        time.sleep(1)
        if int(result) == messages_num:
            break

    instance.query(
        """
        DROP TABLE test.consumer;
        DROP TABLE test.consumer_reconnect;
    """
    )

    assert int(result) == messages_num, "ClickHouse lost some messages: {}".format(
        result
    )


def test_redis_no_connection_at_startup_1(redis_cluster):
    # no connection when table is initialized
    stream_name = 'cs'
    group_name = 'test_cs'

    redis_cluster.pause_container("redis1")
    instance.query_and_get_error(
        """
        CREATE TABLE test.cs (key UInt64, value UInt64)
            ENGINE = RedisStreams
            SETTINGS redis_broker = '{}:6379',
                     redis_stream_list = '{}',
                     redis_group_name = '{}';
        """.format(
                redis_cluster.redis_host,
                stream_name,
                group_name
            )
    )
    redis_cluster.unpause_container("redis1")


def test_redis_no_connection_at_startup_2(redis_cluster):
    stream_name = 'no_connection'
    group_name = 'test_no_connection'
    connection = redis.Redis(redis_cluster.redis_ip, port=redis_cluster.redis_port, password="clickhouse")
    connection.xgroup_create(stream_name, group_name, '$', mkstream=True)

    instance.query(
        """
        CREATE TABLE test.cs (key UInt64, value UInt64)
            ENGINE = RedisStreams
            SETTINGS redis_broker = '{}:6379',
                     redis_stream_list = '{}',
                     redis_group_name = '{}',
                     redis_num_consumers = '5';
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.cs;
        """.format(
                redis_cluster.redis_host,
                stream_name,
                group_name
            )
    )
    instance.query("DETACH TABLE test.cs")
    redis_cluster.pause_container("redis1")
    instance.query("ATTACH TABLE test.cs")
    redis_cluster.unpause_container("redis1")

    messages_num = 1000
    for i in range(messages_num):
        connection.xadd(stream_name, {"key": i, "value": i})
    connection.close()

    while True:
        result = instance.query("SELECT count() FROM test.view")
        time.sleep(1)
        if int(result) == messages_num:
            break

    instance.query(
        """
        DROP TABLE test.consumer;
        DROP TABLE test.cs;
    """
    )

    assert int(result) == messages_num, "ClickHouse lost some messages: {}".format(
        result
    )


def test_redis_json_format_factory_settings(redis_cluster):
    stream_name = 'format_settings'
    group_name = 'test_format_settings'
    connection = redis.Redis(redis_cluster.redis_ip, port=redis_cluster.redis_port, password="clickhouse")
    connection.xgroup_create(stream_name, group_name, '$', mkstream=True)

    instance.query(
        """
        CREATE TABLE test.format_settings (
            id String, date DateTime
        ) ENGINE = RedisStreams
            SETTINGS redis_broker = '{}:6379',
                     redis_stream_list = '{}',
                     redis_group_name = '{}',
                     date_time_input_format = 'best_effort';
        """.format(
                redis_cluster.redis_host,
                stream_name,
                group_name
            )
    )

    connection.xadd(stream_name, {"id": "format_settings_test", "date": "2021-01-19T14:42:33.1829214Z"})
    expected = instance.query(
        """SELECT parseDateTimeBestEffort(CAST('2021-01-19T14:42:33.1829214Z', 'String'))"""
    )


    result = ""
    while True:
        result = instance.query("SELECT date FROM test.format_settings")
        if result == expected:
            break

    instance.query(
        """
        CREATE TABLE test.view (
            id String, date DateTime
        ) ENGINE = MergeTree ORDER BY id;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.format_settings;
        """
    )

    connection.xadd(stream_name, {"id": "format_settings_test", "date": "2021-01-19T14:42:33.1829214Z"})
    result = ""
    while True:
        result = instance.query("SELECT date FROM test.view")
        if result == expected:
            break

    connection.close()
    instance.query(
        """
        DROP TABLE test.consumer;
        DROP TABLE test.format_settings;
    """
    )

    assert result == expected


def test_redis_manage_groups_properly(redis_cluster):
    stream_name = 'manage_groups'
    group_name = 'test_manage_groups'
    connection = redis.Redis(redis_cluster.redis_ip, port=redis_cluster.redis_port, password="clickhouse")

    instance.query(
        """
        CREATE TABLE test.redis_drop (key UInt64, value UInt64)
            ENGINE = RedisStreams
            SETTINGS redis_broker = '{}:6379',
                     redis_stream_list = '{}',
                     redis_group_name = '{}',
                     redis_manage_consumer_groups = true;
        """.format(
                redis_cluster.redis_host,
                stream_name,
                group_name
            )
    )

    connection.xadd(stream_name, {"key": 1, "value": 2})

    while True:
        result = instance.query(
            "SELECT * FROM test.redis_drop ORDER BY key", ignore_error=True
        )
        if result == "1\t2\n":
            break

    exists = False
    try:
        connection.xgroup_create(stream_name, group_name, '$')
    except Exception as e:
        exists = "BUSYGROUP" in str(e)
    assert exists

    instance.query("DROP TABLE test.redis_drop")
    time.sleep(10)

    try:
        connection.xgroup_create(stream_name, group_name, '$')
        exists = False
    except Exception as e:
        exists = True

    assert not exists


def test_redis_consume_stream(redis_cluster):
    stream_name = 'consume_stream'
    group_name = 'test_consume_stream'
    connection = redis.Redis(redis_cluster.redis_ip, port=redis_cluster.redis_port, password="clickhouse")

    i = [0]
    messages_num = 1000

    def produce():
        for _ in range(messages_num):
            connection.xadd(stream_name, {"key": i[0], "value": i[0]})
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
        CREATE TABLE test.redis_stream (key UInt64, value UInt64)
            ENGINE = RedisStreams
            SETTINGS redis_broker = '{}:6379',
                     redis_stream_list = '{}',
                     redis_group_name = '{}',
                     redis_manage_consumer_groups = true,
                     redis_consumer_groups_start_id = '0-0';
        CREATE TABLE test.view (key UInt64, value UInt64)
        ENGINE = MergeTree ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.redis_stream;
        """.format(
                redis_cluster.redis_host,
                stream_name,
                group_name
            )
    )

    result = ""
    while True:
        result = instance.query("SELECT count() FROM test.view")
        if int(result) == messages_num * threads_num:
            break
        time.sleep(1)

    for thread in threads:
        thread.join()

    instance.query("DROP TABLE test.redis_stream")


def test_redis_bad_args(redis_cluster):
    instance.query_and_get_error(
        """
        CREATE TABLE test.drop (key UInt64, value UInt64)
            ENGINE = RedisStreams
            SETTINGS redis_broker = 'redis1:6379',
                     redis_stream_list = 'f',
                     redis_manage_consumer_groups = true,
                     redis_consumer_groups_start_id = '0-0';
        """
    )


def test_redis_drop_mv(redis_cluster):
    stream_name = 'drop_mv'
    group_name = 'test_drop_mv'
    connection = redis.Redis(redis_cluster.redis_ip, port=redis_cluster.redis_port, password="clickhouse")
    connection.xgroup_create(stream_name, group_name, '$', mkstream=True)

    instance.query(
        """
        CREATE TABLE test.redis (key UInt64, value UInt64)
            ENGINE = RedisStreams
            SETTINGS redis_broker = '{}:6379',
                     redis_stream_list = '{}',
                     redis_group_name = '{}';
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.redis;
        """.format(
                redis_cluster.redis_host,
                stream_name,
                group_name
            )
    )

    messages = []
    for i in range(20):
        connection.xadd(stream_name, {"key": i, "value": i})

    instance.query("DROP VIEW test.consumer")
    for i in range(20, 40):
        connection.xadd(stream_name, {"key": i, "value": i})

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.redis;
    """
    )
    for i in range(40, 50):
        connection.xadd(stream_name, {"key": i, "value": i})

    while True:
        result = instance.query("SELECT * FROM test.view ORDER BY key")
        if redis_check_result(result):
            break

    redis_check_result(result, True)

    instance.query("DROP VIEW test.consumer")
    time.sleep(10)
    for i in range(50, 60):
        connection.xadd(stream_name, {"key": i, "value": i})
    connection.close()

    count = 0
    while True:
        count = int(instance.query("SELECT count() FROM test.redis"))
        if count:
            break

    assert count > 0


def test_redis_predefined_configuration(redis_cluster):
    connection = redis.Redis(redis_cluster.redis_ip, port=redis_cluster.redis_port, password="clickhouse")

    instance.query(
        """
        CREATE TABLE test.redis (key UInt64, value UInt64)
            ENGINE = RedisStreams(redis1) """
    )

    connection.xadd('named', {"key": 1, "value": 2})
    while True:
        result = instance.query(
            "SELECT * FROM test.redis ORDER BY key", ignore_error=True
        )
        if result == "1\t2\n":
            break


def test_redis_not_ack_on_select(redis_cluster):
    stream_name = 'not_ack_on_select'
    group_name = 'test_not_ack_on_select'
    connection = redis.Redis(redis_cluster.redis_ip, port=redis_cluster.redis_port, password="clickhouse")
    connection.xgroup_create(stream_name, group_name, '$', mkstream=True)

    instance.query(
        """
        CREATE TABLE test.redis (key UInt64, value UInt64)
            ENGINE = RedisStreams
            SETTINGS redis_broker = '{}:6379',
                     redis_stream_list = '{}',
                     redis_group_name = '{}',
                     redis_ack_on_select = false;
        """.format(
                redis_cluster.redis_host,
                stream_name,
                group_name
            )
    )

    connection.xadd(stream_name, {"key": 1, "value": 2})
    while True:
        result = instance.query(
            "SELECT * FROM test.redis ORDER BY key", ignore_error=True
        )
        if result == "1\t2\n":
            break

    time.sleep(5)

    while True:
        result = instance.query(
            "SELECT * FROM test.redis ORDER BY key", ignore_error=True
        )
        if result == "1\t2\n":
            break


if __name__ == "__main__":
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
