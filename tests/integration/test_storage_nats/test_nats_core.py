import asyncio
import json
import logging
import math
import nats
import os.path as p
import random
import subprocess
import threading
import time
from random import randrange

import pytest
from google.protobuf.internal.encoder import _VarintBytes

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster, nats_connect_ssl
from helpers.config_cluster import nats_user, nats_pass
from helpers.test_tools import TSV

from . import common as nats_helpers
from . import nats_pb2

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=[
        "configs/nats.xml",
        "configs/macros.xml",
        "configs/named_collection.xml",
    ],
    user_configs=["configs/users.xml"],
    with_nats=True,
    clickhouse_path_dir="clickhouse_path",
)

# Helpers

async def publish_messages(cluster_inst, subject, messages=(), bytes=None):
    nc = await nats_helpers.nats_connect_ssl(cluster_inst)
    logging.debug("NATS connection status: " + str(nc.is_connected))

    for message in messages:
        await nc.publish(subject, message.encode())
    if bytes is not None:
        await nc.publish(subject, bytes)
    await nc.flush()
    logging.debug("Finished publishing to " + subject)

    await nc.close()


# Fixtures


@pytest.fixture(scope="module")
def nats_cluster():
    try:
        cluster.start()
        logging.debug("nats_id is {}".format(instance.cluster.nats_docker_id))

        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def nats_setup_teardown():
    logging.debug("NATS is available - running test")

    instance.query("DROP DATABASE IF EXISTS test SYNC")
    instance.query("CREATE DATABASE test")

    yield  # run test

    instance.query("DROP DATABASE test")


# Tests

def test_nats_select_empty(nats_cluster):
    instance.query(
        """
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = 'empty',
                     nats_format = 'TSV',
                     nats_row_delimiter = '\\n';
        """
    )

    assert int(instance.query("SELECT count() FROM test.nats")) == 0


def test_nats_select(nats_cluster):
    instance.query(
        """
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = 'select',
                     nats_format = 'JSONEachRow',
                     nats_row_delimiter = '\\n';
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        """
    )
    nats_helpers.wait_for_table_is_ready(instance, "test.nats")

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.nats;
        """
    )
    nats_helpers.wait_for_mv_attached_to_table(instance, "test.nats")

    messages = []
    for i in range(50):
        messages.append(json.dumps({"key": i, "value": i}))
    asyncio.run(publish_messages(nats_cluster, "select", messages))

    nats_helpers.check_query_result(instance, "SELECT * FROM test.view ORDER BY key")


def test_nats_json_without_delimiter(nats_cluster):
    instance.query(
        """
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = 'json',
                     nats_format = 'JSONEachRow';
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        """
    )
    nats_helpers.wait_for_table_is_ready(instance, "test.nats")

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.nats;
        """
    )
    nats_helpers.wait_for_mv_attached_to_table(instance, "test.nats")
    
    messages = ""
    for i in range(25):
        messages += json.dumps({"key": i, "value": i}) + "\n"

    all_messages = [messages]
    asyncio.run(publish_messages(nats_cluster, "json", all_messages))

    messages = ""
    for i in range(25, 50):
        messages += json.dumps({"key": i, "value": i}) + "\n"
    all_messages = [messages]
    asyncio.run(publish_messages(nats_cluster, "json", all_messages))

    nats_helpers.check_query_result(instance, "SELECT * FROM test.view ORDER BY key")


def test_nats_csv_with_delimiter(nats_cluster):
    instance.query(
        """
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = 'csv',
                     nats_format = 'CSV',
                     nats_row_delimiter = '\\n';
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        """
    )
    nats_helpers.wait_for_table_is_ready(instance, "test.nats")

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.nats;
        """
    )
    nats_helpers.wait_for_mv_attached_to_table(instance, "test.nats")

    messages = []
    for i in range(50):
        messages.append("{i}, {i}".format(i=i))

    asyncio.run(publish_messages(nats_cluster, "csv", messages))
    time.sleep(1)

    nats_helpers.check_query_result(instance, "SELECT * FROM test.view ORDER BY key")


def test_nats_tsv_with_delimiter(nats_cluster):
    instance.query(
        """
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = 'tsv',
                     nats_format = 'TSV',
                     nats_row_delimiter = '\\n';
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        """
    )
    nats_helpers.wait_for_table_is_ready(instance, "test.nats")

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.nats;
        """
    )
    nats_helpers.wait_for_mv_attached_to_table(instance, "test.nats")

    messages = []
    for i in range(50):
        messages.append("{i}\t{i}".format(i=i))

    asyncio.run(publish_messages(nats_cluster, "tsv", messages))

    nats_helpers.check_query_result(instance, "SELECT * FROM test.view ORDER BY key")

#


def test_nats_macros(nats_cluster):
    instance.query(
        """
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = '{nats_url}',
                     nats_subjects = '{nats_subjects}',
                     nats_format = '{nats_format}';
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        """
    )
    nats_helpers.wait_for_table_is_ready(instance, "test.nats")

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.nats;
        """
    )
    nats_helpers.wait_for_mv_attached_to_table(instance, "test.nats")
    
    message = ""
    for i in range(50):
        message += json.dumps({"key": i, "value": i}) + "\n"
    asyncio.run(publish_messages(nats_cluster, "test_subject", [message]))

    nats_helpers.check_query_result(instance, "SELECT * FROM test.view ORDER BY key")


def test_nats_materialized_view(nats_cluster):
    instance.query(
        """
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = 'mv',
                     nats_format = 'JSONEachRow',
                     nats_row_delimiter = '\\n';
        CREATE TABLE test.view1 (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE TABLE test.view2 (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        """
    )
    nats_helpers.wait_for_table_is_ready(instance, "test.nats")

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer1 TO test.view1 AS
            SELECT * FROM test.nats;
        CREATE MATERIALIZED VIEW test.consumer2 TO test.view2 AS
            SELECT * FROM test.nats;
        """
    )
    nats_helpers.wait_for_mv_attached_to_table(instance, "test.nats")
    
    messages = []
    for i in range(50):
        messages.append(json.dumps({"key": i, "value": i}))
    asyncio.run(publish_messages(nats_cluster, "mv", messages))

    nats_helpers.check_query_result(instance, "SELECT * FROM test.view1 ORDER BY key")
    nats_helpers.check_query_result(instance, "SELECT * FROM test.view2 ORDER BY key")


def test_nats_materialized_view_with_subquery(nats_cluster):
    instance.query(
        """
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = 'mvsq',
                     nats_format = 'JSONEachRow',
                     nats_row_delimiter = '\\n';
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        """
    )
    nats_helpers.wait_for_table_is_ready(instance, "test.nats")

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM (SELECT * FROM test.nats);
        """
    )
    nats_helpers.wait_for_mv_attached_to_table(instance, "test.nats")
    
    messages = []
    for i in range(50):
        messages.append(json.dumps({"key": i, "value": i}))
    asyncio.run(publish_messages(nats_cluster, "mvsq", messages))

    nats_helpers.check_query_result(instance, "SELECT * FROM test.view ORDER BY key")


def test_nats_protobuf(nats_cluster):
    instance.query(
        """
        CREATE TABLE test.nats (key UInt64, value String)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = 'pb',
                     nats_format = 'Protobuf',
                     nats_schema = 'nats.proto:ProtoKeyValue';
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        """
    )
    nats_helpers.wait_for_table_is_ready(instance, "test.nats")

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.nats;
        """
    )
    nats_helpers.wait_for_mv_attached_to_table(instance, "test.nats")

    def produce_messages(range):
        data = b""
        for i in range:
            msg = nats_pb2.ProtoKeyValue()
            msg.key = i
            msg.value = str(i)
            serialized_msg = msg.SerializeToString()
            data = data + _VarintBytes(len(serialized_msg)) + serialized_msg
        asyncio.run(publish_messages(nats_cluster, "pb", bytes=data))
    
    produce_messages(range(0, 20))
    produce_messages(range(20, 21))
    produce_messages(range(21, 50))

    nats_helpers.check_query_result(instance, "SELECT * FROM test.view ORDER BY key")


def test_nats_big_message(nats_cluster):
    # Create batchs of messages of size ~100Kb
    nats_messages = 100
    batch_messages = 1000
    messages = [
        json.dumps({"key": i, "value": "x" * 100}) * batch_messages
        for i in range(nats_messages)
    ]

    instance.query(
        """
        CREATE TABLE test.nats (key UInt64, value String)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = 'big',
                     nats_format = 'JSONEachRow';
        CREATE TABLE test.view (key UInt64, value String)
            ENGINE = MergeTree
            ORDER BY key;
        """
    )
    nats_helpers.wait_for_table_is_ready(instance, "test.nats")

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.nats;
        """
    )
    nats_helpers.wait_for_mv_attached_to_table(instance, "test.nats")

    asyncio.run(publish_messages(nats_cluster, "big", messages))

    result = instance.query_with_retry(
        "SELECT count() FROM test.view",
        retry_count = 300,
        sleep_time = 1,
        check_callback = lambda num_rows: int(num_rows) == batch_messages * nats_messages)

    assert (
        int(result) == nats_messages * batch_messages
    ), "ClickHouse lost some messages: {}".format(result)


def test_nats_mv_combo(nats_cluster):
    NUM_MV = 5
    NUM_CONSUMERS = 4

    instance.query(
        """
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = 'combo',
                     nats_num_consumers = {},
                     nats_format = 'JSONEachRow',
                     nats_row_delimiter = '\\n';
        """.format(
            NUM_CONSUMERS
        )
    )
    nats_helpers.wait_for_table_is_ready(instance, "test.nats")

    for mv_id in range(NUM_MV):
        instance.query(
            """
            CREATE TABLE test.combo_{0} (key UInt64, value UInt64)
                ENGINE = MergeTree()
                ORDER BY key;
            CREATE MATERIALIZED VIEW test.combo_{0}_mv TO test.combo_{0} AS
                SELECT * FROM test.nats;
            """.format(
                mv_id
            )
        )
    nats_helpers.wait_for_mv_attached_to_table(instance, "test.nats")

    i = [0]
    messages_num = 10000

    def produce():
        messages = []
        for _ in range(messages_num):
            messages.append(json.dumps({"key": i[0], "value": i[0]}))
            i[0] += 1
        asyncio.run(publish_messages(nats_cluster, "combo", messages))

    threads = []
    threads_num = 20

    for _ in range(threads_num):
        threads.append(threading.Thread(target=produce))
    for thread in threads:
        time.sleep(random.uniform(0, 1))
        thread.start()

    time_limit_sec = 300
    deadline = time.monotonic() + time_limit_sec

    while time.monotonic() < deadline:
        result = 0
        for mv_id in range(NUM_MV):
            result += int(
                instance.query("SELECT count() FROM test.combo_{0}".format(mv_id))
            )
        if int(result) == messages_num * threads_num * NUM_MV:
            break
        time.sleep(1)

    for thread in threads:
        thread.join()

    assert (
        int(result) == messages_num * threads_num * NUM_MV
    ), "ClickHouse lost some messages: {}".format(result)


def test_nats_insert(nats_cluster):
    instance.query(
        """
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = 'insert',
                     nats_format = 'TSV',
                     nats_row_delimiter = '\\n';
    """
    )
    nats_helpers.wait_for_table_is_ready(instance, "test.nats")

    values = []
    for i in range(50):
        values.append("({i}, {i})".format(i=i))
    values = ",".join(values)

    insert_messages = []

    async def sub_to_nats():
        nc = await nats_connect_ssl(nats_cluster)
        sub = await nc.subscribe("insert")
        await sub.unsubscribe(50)
        async for msg in sub.messages:
            insert_messages.append(msg.data.decode())

        await sub.drain()
        await nc.drain()

    def run_sub():
        asyncio.run(sub_to_nats())

    thread = threading.Thread(target=run_sub)
    thread.start()
    time.sleep(1)

    instance.query_with_retry("INSERT INTO test.nats VALUES {}".format(values))
    thread.join()

    result = "\n".join(insert_messages)
    nats_helpers.check_result(result, True)


def test_fetching_messages_without_mv(nats_cluster):
    instance.query(
        """
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = 'insert',
                     nats_queue_group = 'consumers_group',
                     nats_format = 'TSV',
                     nats_row_delimiter = '\\n';
    """
    )
    nats_helpers.wait_for_table_is_ready(instance, "test.nats")

    values = []
    for i in range(50):
        values.append("({i}, {i})".format(i=i))
    values = ",".join(values)

    insert_messages = []

    async def sub_to_nats():
        nc = await nats_connect_ssl(nats_cluster)
        sub = await nc.subscribe("insert", "consumers_group")
        
        try:
            for i in range(50):
                msg = await sub.next_msg(120)
                insert_messages.append(msg.data.decode())
        except asyncio.exceptions.TimeoutError as ex:
            logging.debug("recieve message timeout: " + str(ex))

        await sub.drain()
        await nc.drain()

    def run_sub():
        asyncio.run(sub_to_nats())

    thread = threading.Thread(target=run_sub)
    thread.start()
    time.sleep(1)

    instance.query_with_retry("INSERT INTO test.nats VALUES {}".format(values))
    thread.join()

    result = "\n".join(insert_messages)
    nats_helpers.check_result(result, True)


def test_nats_many_subjects_insert_wrong(nats_cluster):
    instance.query(
        """
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = 'insert1,insert2.>,insert3.*.foo',
                     nats_format = 'TSV',
                     nats_row_delimiter = '\\n';
    """
    )
    nats_helpers.wait_for_table_is_ready(instance, "test.nats")

    values = []
    for i in range(50):
        values.append("({i}, {i})".format(i=i))
    values = ",".join(values)


    # This NATS engine reads from multiple subjects
    assert(
        "This NATS engine reads from multiple subjects. You must specify `stream_like_engine_insert_queue` to choose the subject to write to." 
        in 
        instance.query_and_get_error("INSERT INTO test.nats VALUES {}".format(values)))

    # Can not publish to wildcard subject
    assert(
        "Can not publish to wildcard subject"
        in
        instance.query_and_get_error("INSERT INTO test.nats SETTINGS stream_like_engine_insert_queue='insert2.>' VALUES {}".format(values))
    )
    assert(
        "Can not publish to wildcard subject"
        in
        instance.query_and_get_error("INSERT INTO test.nats SETTINGS stream_like_engine_insert_queue='insert3.*.foo' VALUES {}".format(values)))
    
    # Selected subject is not among engine subjects
    assert(
        "Selected subject is not among engine subjects"
        in
        instance.query_and_get_error("INSERT INTO test.nats SETTINGS stream_like_engine_insert_queue='insert4' VALUES {}".format(values)))
    assert(
        "Selected subject is not among engine subjects"
        in
        instance.query_and_get_error("INSERT INTO test.nats SETTINGS stream_like_engine_insert_queue='insert3.foo.baz' VALUES {}".format(values)))
    assert(
        "Selected subject is not among engine subjects"
        in
        instance.query_and_get_error("INSERT INTO test.nats SETTINGS stream_like_engine_insert_queue='foo.insert2' VALUES {}".format(values)))


def test_nats_many_subjects_insert_right(nats_cluster):
    instance.query(
        """
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = 'right_insert1,right_insert2',
                     nats_format = 'TSV',
                     nats_row_delimiter = '\\n';
    """
    )
    nats_helpers.wait_for_table_is_ready(instance, "test.nats")

    values = []
    for i in range(50):
        values.append("({i}, {i})".format(i=i))
    values = ",".join(values)

    insert_messages = []

    async def sub_to_nats():
        nc = await nats_connect_ssl(nats_cluster)
        sub = await nc.subscribe("right_insert1")
        await sub.unsubscribe(50)
        async for msg in sub.messages:
            insert_messages.append(msg.data.decode())

        await sub.drain()
        await nc.drain()

    def run_sub():
        asyncio.run(sub_to_nats())

    thread = threading.Thread(target=run_sub)
    thread.start()
    time.sleep(1)

    instance.query_with_retry("INSERT INTO test.nats SETTINGS stream_like_engine_insert_queue='right_insert1' VALUES {}".format(values))
    thread.join()

    result = "\n".join(insert_messages)
    nats_helpers.check_result(result, True)


def test_nats_many_inserts(nats_cluster):
    instance.query(
        """
        CREATE TABLE test.nats_many (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = 'many_inserts',
                     nats_format = 'TSV',
                     nats_row_delimiter = '\\n';
        CREATE TABLE test.nats_consume (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = 'many_inserts',
                     nats_format = 'TSV',
                     nats_row_delimiter = '\\n';
        CREATE TABLE test.view_many (key UInt64, value UInt64)
            ENGINE = MergeTree
            ORDER BY key;
        """
    )
    nats_helpers.wait_for_table_is_ready(instance, "test.nats_consume")
    nats_helpers.wait_for_table_is_ready(instance, "test.nats_many")

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer_many TO test.view_many AS
            SELECT * FROM test.nats_consume;
        """
    )
    nats_helpers.wait_for_mv_attached_to_table(instance, "test.nats_consume")

    messages_num = 10000
    values = []
    for i in range(messages_num):
        values.append("({i}, {i})".format(i=i))
    values = ",".join(values)

    threads = []
    threads_num = 10
    for _ in range(threads_num):
        threads.append(threading.Thread(target = lambda: instance.query_with_retry("INSERT INTO test.nats_many VALUES {}".format(values))))
    for thread in threads:
        time.sleep(random.uniform(0, 1))
        thread.start()

    for thread in threads:
        thread.join()

    result = instance.query_with_retry(
        "SELECT count() FROM test.view_many",
        retry_count = 300,
        sleep_time = 1,
        check_callback = lambda num_rows: int(num_rows) >= messages_num * threads_num)
    assert (
        int(result) == messages_num * threads_num
    ), "ClickHouse lost some messages or got duplicated ones. Total count: {}".format(
        result
    )


def test_nats_overloaded_insert(nats_cluster):
    instance.query(
        """
        CREATE TABLE test.nats_consume (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = 'over',
                     nats_num_consumers = 5,
                     nats_max_block_size = 10000,
                     nats_format = 'TSV',
                     nats_row_delimiter = '\\n';
        CREATE TABLE test.nats_overload (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = 'over',
                     nats_format = 'TSV',
                     nats_row_delimiter = '\\n';
        CREATE TABLE test.view_overload (key UInt64, value UInt64)
            ENGINE = MergeTree
            ORDER BY key
            SETTINGS old_parts_lifetime=5, cleanup_delay_period=2, cleanup_delay_period_random_add=3,
            cleanup_thread_preferred_points_per_iteration=0;
        """
    )
    nats_helpers.wait_for_table_is_ready(instance, "test.nats_consume")
    nats_helpers.wait_for_table_is_ready(instance, "test.nats_overload")

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer_overload TO test.view_overload AS
            SELECT * FROM test.nats_consume;
        """
    )
    nats_helpers.wait_for_mv_attached_to_table(instance, "test.nats_consume")

    messages_num = 100000

    def insert():
        values = []
        for i in range(messages_num):
            values.append("({i}, {i})".format(i=i))
        values = ",".join(values)

        instance.query_with_retry("INSERT INTO test.nats_overload VALUES {}".format(values))

    threads = []
    threads_num = 5
    for _ in range(threads_num):
        threads.append(threading.Thread(target=insert))
    for thread in threads:
        time.sleep(random.uniform(0, 1))
        thread.start()

    result = instance.query_with_retry(
        "SELECT count() FROM test.view_overload",
        retry_count = 300,
        sleep_time = 1,
        check_callback = lambda num_rows: int(num_rows) >= messages_num * threads_num)

    for thread in threads:
        thread.join()

    assert (
        int(result) == messages_num * threads_num
    ), "ClickHouse lost some messages or got duplicated ones. Total count: {}".format(
        result
    )


def test_nats_virtual_column(nats_cluster):
    instance.query(
        """
        CREATE TABLE test.nats_virtuals (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = 'virtuals',
                     nats_format = 'JSONEachRow';
        """
    )
    nats_helpers.wait_for_table_is_ready(instance, "test.nats_virtuals")

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.view Engine=Log AS
        SELECT value, key, _subject FROM test.nats_virtuals;
        """
    )
    nats_helpers.wait_for_mv_attached_to_table(instance, "test.nats_virtuals")
    
    message_num = 10
    i = 0
    messages = []
    for _ in range(message_num):
        messages.append(json.dumps({"key": i, "value": i}))
        i += 1

    asyncio.run(publish_messages(nats_cluster, "virtuals", messages))

    result = instance.query_with_retry("SELECT count() FROM test.view", check_callback = lambda num_rows: int(num_rows) == message_num)
    assert int(result) == message_num

    result = instance.query(
        """
        SELECT key, value, _subject
        FROM test.view ORDER BY key
        """
    )

    expected = """\
0	0	virtuals
1	1	virtuals
2	2	virtuals
3	3	virtuals
4	4	virtuals
5	5	virtuals
6	6	virtuals
7	7	virtuals
8	8	virtuals
9	9	virtuals
"""

    assert TSV(result) == TSV(expected)


def test_nats_virtual_column_with_materialized_view(nats_cluster):
    instance.query(
        """
        CREATE TABLE test.nats_virtuals_mv (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = 'virtuals_mv',
                     nats_format = 'JSONEachRow';
        CREATE TABLE test.view (key UInt64, value UInt64, subject String) ENGINE = MergeTree()
            ORDER BY key;
        """
    )
    nats_helpers.wait_for_table_is_ready(instance, "test.nats_virtuals_mv")

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
        SELECT *, _subject as subject
        FROM test.nats_virtuals_mv;
        """
    )
    nats_helpers.wait_for_mv_attached_to_table(instance, "test.nats_virtuals_mv")
    
    message_num = 10
    i = 0
    messages = []
    for _ in range(message_num):
        messages.append(json.dumps({"key": i, "value": i}))
        i += 1

    asyncio.run(publish_messages(nats_cluster, "virtuals_mv", messages))

    result = instance.query_with_retry("SELECT count() FROM test.view", check_callback = lambda num_rows: int(num_rows) == message_num)
    assert int(result) == message_num

    result = instance.query("SELECT key, value, subject FROM test.view ORDER BY key")
    expected = """\
0	0	virtuals_mv
1	1	virtuals_mv
2	2	virtuals_mv
3	3	virtuals_mv
4	4	virtuals_mv
5	5	virtuals_mv
6	6	virtuals_mv
7	7	virtuals_mv
8	8	virtuals_mv
9	9	virtuals_mv
"""

    assert TSV(result) == TSV(expected)


def test_nats_many_consumers_to_each_queue(nats_cluster):
    instance.query(
        """
        CREATE TABLE test.destination(key UInt64, value UInt64)
        ENGINE = MergeTree()
        ORDER BY key;
        """
    )

    num_tables = 4
    for table_id in range(num_tables):
        logging.debug(("Setting up table {}".format(table_id)))
        instance.query(
            """
            CREATE TABLE test.many_consumers_{0} (key UInt64, value UInt64)
                ENGINE = NATS
                SETTINGS nats_url = 'nats1:4444',
                         nats_subjects = 'many_consumers',
                         nats_num_consumers = 2,
                         nats_queue_group = 'many_consumers',
                         nats_format = 'JSONEachRow',
                         nats_row_delimiter = '\\n';
            """.format(
                table_id
            )
        )
        nats_helpers.wait_for_table_is_ready(instance, "test.many_consumers_{}".format(table_id))

    for table_id in range(num_tables):
        logging.debug(("Setting up table mv {}".format(table_id)))
        instance.query(
            """
            CREATE MATERIALIZED VIEW test.many_consumers_{0}_mv TO test.destination AS
            SELECT key, value FROM test.many_consumers_{0};
        """.format(
                table_id
            )
        )
        nats_helpers.wait_for_mv_attached_to_table(instance, "test.many_consumers_{0}".format(table_id))

    i = [0]
    messages_num = 1000

    def produce():
        messages = []
        for _ in range(messages_num):
            messages.append(json.dumps({"key": i[0], "value": i[0]}))
            i[0] += 1
        asyncio.run(publish_messages(nats_cluster, "many_consumers", messages))

    threads = []
    threads_num = 20

    for _ in range(threads_num):
        threads.append(threading.Thread(target=produce))
    for thread in threads:
        time.sleep(random.uniform(0, 1))
        thread.start()

    result = instance.query_with_retry("SELECT count() FROM test.destination", check_callback = lambda num_rows: int(num_rows) == messages_num * threads_num)

    for thread in threads:
        thread.join()

    assert (
        int(result) == messages_num * threads_num
    ), "ClickHouse lost some messages: {}".format(result)


def test_nats_restore_failed_connection_without_losses_on_write(nats_cluster):
    instance.query(
        """
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree
            ORDER BY key;
        CREATE TABLE test.consume (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = 'producer_reconnect',
                     nats_format = 'JSONEachRow',
                     nats_num_consumers = 2,
                     nats_row_delimiter = '\\n';
        CREATE TABLE test.producer_reconnect (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = 'producer_reconnect',
                     nats_format = 'JSONEachRow',
                     nats_row_delimiter = '\\n';
    """
    )
    
    nats_helpers.wait_for_table_is_ready(instance, "test.consume")
    nats_helpers.wait_for_table_is_ready(instance, "test.producer_reconnect")

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.consume;
        """
    )
    nats_helpers.wait_for_mv_attached_to_table(instance, "test.consume")

    messages_num = 100000
    values = []
    for i in range(messages_num):
        values.append("({i}, {i})".format(i=i))
    values = ",".join(values)

    instance.query_with_retry("INSERT INTO test.producer_reconnect VALUES {}".format(values))
    instance.query_with_retry("SELECT count() FROM test.view", sleep_time = 0.1, check_callback = lambda num_rows: int(num_rows) != 0)

    nats_helpers.kill_nats(nats_cluster)
    time.sleep(4)
    nats_helpers.revive_nats(nats_cluster)

    result = instance.query_with_retry("SELECT count(DISTINCT key) FROM test.view", check_callback = lambda num_rows: int(num_rows) == messages_num)

    assert int(result) == messages_num, "ClickHouse lost some messages: {}".format(
        result
    )


def test_nats_no_connection_at_startup_1(nats_cluster):
    assert( 
        "Cannot connect to Nats"
        in
        instance.query_and_get_error(
            """
            CREATE TABLE test.cs (key UInt64, value UInt64)
                ENGINE = NATS
                SETTINGS nats_url = 'invalid_nats_url:4444',
                        nats_subjects = 'cs',
                        nats_format = 'JSONEachRow',
                        nats_num_consumers = '5',
                        nats_row_delimiter = '\\n';
            """
        ))
    assert "Table `cs` doesn't exist" in instance.query_and_get_error("SHOW TABLE test.cs;")


def test_nats_no_connection_at_startup_2(nats_cluster):
    instance.query(
        """
        CREATE TABLE test.cs (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = 'cs',
                     nats_format = 'JSONEachRow',
                     nats_num_consumers = '5',
                     nats_row_delimiter = '\\n';
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.cs;
        """
    )

    instance.query(
        """
        DETACH TABLE test.cs;
        DROP VIEW test.consumer;
        """
    )
    with nats_cluster.pause_container("nats1"):
        nats_helpers.wait_nats_paused(nats_cluster)
        instance.query("ATTACH TABLE test.cs")

    nats_helpers.wait_for_table_is_ready(instance, "test.cs")

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.cs;
        """
    )
    nats_helpers.wait_for_mv_attached_to_table(instance, "test.cs")

    messages_num = 1000
    messages = []
    for i in range(messages_num):
        messages.append(json.dumps({"key": i, "value": i}))
    asyncio.run(publish_messages(nats_cluster, "cs", messages))

    result = instance.query_with_retry(
        "SELECT count() FROM test.view", 
        retry_count = 20, 
        sleep_time = 1, 
        check_callback = lambda num_rows: int(num_rows) == messages_num)
    assert int(result) == messages_num, "ClickHouse lost some messages: {}".format(result)


def test_nats_format_factory_settings(nats_cluster):
    instance.query(
        """
        CREATE TABLE test.format_settings (
            id String, date DateTime
        ) ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = 'format_settings',
                     nats_format = 'JSONEachRow',
                     date_time_input_format = 'best_effort';
        CREATE TABLE test.view (
            id String, date DateTime
        ) ENGINE = MergeTree ORDER BY id;
        """
    )
    nats_helpers.wait_for_table_is_ready(instance, "test.format_settings")

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.format_settings;
        """
    )
    nats_helpers.wait_for_mv_attached_to_table(instance, "test.format_settings")


    message = json.dumps(
        {"id": "format_settings_test", "date": "2021-01-19T14:42:33.1829214Z"}
    )
    expected = instance.query(
        """SELECT parseDateTimeBestEffort(CAST('2021-01-19T14:42:33.1829214Z', 'String'))"""
    )

    asyncio.run(publish_messages(nats_cluster, "format_settings", [message]))
    result = instance.query_with_retry("SELECT date FROM test.view", check_callback = lambda query_result: query_result == expected)

    assert result == expected


def test_nats_bad_args(nats_cluster):
    assert(
        "You must specify `nats_subjects` setting"
        in
        instance.query_and_get_error(
            """
            CREATE TABLE test.drop (key UInt64, value UInt64)
                ENGINE = NATS
                SETTINGS nats_url = 'nats1:4444',
                        nats_secure = true,
                        nats_format = 'JSONEachRow';
            """))


def test_nats_drop_mv(nats_cluster):
    instance.query(
        """
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = 'mv',
                     nats_format = 'JSONEachRow';
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        """
    )
    nats_helpers.wait_for_table_is_ready(instance, "test.nats")

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.nats;
        """
    )
    nats_helpers.wait_for_mv_attached_to_table(instance, "test.nats")
    
    messages = []
    for i in range(20):
        messages.append(json.dumps({"key": i, "value": i}))
    asyncio.run(publish_messages(nats_cluster, "mv", messages))

    nats_helpers.wait_query_result(instance, "SELECT count() FROM test.view", 20)

    instance.query("DROP VIEW test.consumer")
    nats_helpers.wait_for_table_is_ready(instance, "test.nats")

    messages = []
    for i in range(100, 200):
        messages.append(json.dumps({"key": i, "value": i}))
    asyncio.run(publish_messages(nats_cluster, "mv", messages))

    time.sleep (1)

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.nats;
        """
    )
    nats_helpers.wait_for_mv_attached_to_table(instance, "test.nats")

    messages = []
    for i in range(20, 40):
        messages.append(json.dumps({"key": i, "value": i}))
    asyncio.run(publish_messages(nats_cluster, "mv", messages))

    nats_helpers.wait_query_result(instance, "SELECT count() FROM test.view", 40)

    instance.query("DROP VIEW test.consumer")
    nats_helpers.wait_for_table_is_ready(instance, "test.nats")

    messages = []
    for i in range(200, 400):
        messages.append(json.dumps({"key": i, "value": i}))
    asyncio.run(publish_messages(nats_cluster, "mv", messages))

    time.sleep (1)

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.nats;
        """
    )
    nats_helpers.wait_for_mv_attached_to_table(instance, "test.nats")

    messages = []
    for i in range(40, 50):
        messages.append(json.dumps({"key": i, "value": i}))
    asyncio.run(publish_messages(nats_cluster, "mv", messages))
    nats_helpers.check_query_result(instance, "SELECT * FROM test.view ORDER BY key")

    instance.query("DROP VIEW test.consumer")
    nats_helpers.wait_for_table_is_ready(instance, "test.nats")

    messages = []
    for i in range(400, 500):
        messages.append(json.dumps({"key": i, "value": i}))
    asyncio.run(publish_messages(nats_cluster, "mv", messages))
    nats_helpers.check_query_result(instance, "SELECT * FROM test.view ORDER BY key")


def test_nats_predefined_configuration(nats_cluster):
    instance.query(
        """
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS(nats1);
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        """
    )
    nats_helpers.wait_for_table_is_ready(instance, "test.nats")

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.nats;
        """
    )
    nats_helpers.wait_for_mv_attached_to_table(instance, "test.nats")

    asyncio.run(
        publish_messages(
            nats_cluster, "named", [json.dumps({"key": 1, "value": 2})]
        )
    )
    result = instance.query_with_retry(
        "SELECT * FROM test.view ORDER BY key", 
        ignore_error = True,
        check_callback = lambda query_result: query_result == "1\t2\n")
    
    assert result == "1\t2\n"


def test_format_with_prefix_and_suffix(nats_cluster):
    instance.query(
        """
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = 'custom',
                     nats_format = 'CustomSeparated';
    """
    )
    nats_helpers.wait_for_table_is_ready(instance, "test.nats")

    insert_messages = []

    async def sub_to_nats():
        nc = await nats_connect_ssl(nats_cluster)
        sub = await nc.subscribe("custom")
        await sub.unsubscribe(2)
        async for msg in sub.messages:
            insert_messages.append(msg.data.decode())

        await sub.drain()
        await nc.drain()

    def run_sub():
        asyncio.run(sub_to_nats())

    thread = threading.Thread(target=run_sub)
    thread.start()
    time.sleep(1)

    instance.query(
        "INSERT INTO test.nats select number*10 as key, number*100 as value from numbers(2) settings format_custom_result_before_delimiter='<prefix>\n', format_custom_result_after_delimiter='<suffix>\n'"
    )

    thread.join()

    assert (
        "".join(insert_messages)
        == "<prefix>\n0\t0\n<suffix>\n<prefix>\n10\t100\n<suffix>\n"
    )


def test_max_rows_per_message(nats_cluster):
    instance.query(
        """
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = 'custom1',
                     nats_format = 'CustomSeparated',
                     nats_max_rows_per_message = 3,
                     format_custom_result_before_delimiter = '<prefix>\n',
                     format_custom_result_after_delimiter = '<suffix>\n';
        """
    )
    nats_helpers.wait_for_table_is_ready(instance, "test.nats")

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.view Engine=Log AS
        SELECT key, value FROM test.nats;
        """
    )
    nats_helpers.wait_for_mv_attached_to_table(instance, "test.nats")

    num_rows = 5

    insert_messages = []

    async def sub_to_nats():
        nc = await nats_connect_ssl(nats_cluster)
        sub = await nc.subscribe("custom1")
        await sub.unsubscribe(2)
        async for msg in sub.messages:
            insert_messages.append(msg.data.decode())

        await sub.drain()
        await nc.drain()

    def run_sub():
        asyncio.run(sub_to_nats())

    thread = threading.Thread(target=run_sub)
    thread.start()
    time.sleep(1)

    instance.query(
        f"INSERT INTO test.nats select number*10 as key, number*100 as value from numbers({num_rows}) settings format_custom_result_before_delimiter='<prefix>\n', format_custom_result_after_delimiter='<suffix>\n'"
    )

    thread.join()

    assert (
        "".join(insert_messages)
        == "<prefix>\n0\t0\n10\t100\n20\t200\n<suffix>\n<prefix>\n30\t300\n40\t400\n<suffix>\n"
    )

    rows = instance.query_with_retry(
        "SELECT count() FROM test.view",
        retry_count = 100,
        check_callback = lambda query_result: int(query_result) == num_rows)
    assert int(rows) == num_rows

    result = instance.query("SELECT * FROM test.view ORDER BY key")
    assert result == "0\t0\n10\t100\n20\t200\n30\t300\n40\t400\n"


def test_row_based_formats(nats_cluster):
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
            DROP TABLE IF EXISTS test.nats;
               
            CREATE TABLE test.nats (key UInt64, value UInt64)
                ENGINE = NATS
                SETTINGS nats_url = 'nats1:4444',
                         nats_subjects = '{format_name}',
                         nats_format = '{format_name}';      
            """
        )
        nats_helpers.wait_for_table_is_ready(instance, "test.nats")

        instance.query(
            """
            CREATE MATERIALIZED VIEW test.view Engine=Log AS
            SELECT key, value FROM test.nats;
            """
        )
        nats_helpers.wait_for_mv_attached_to_table(instance, "test.nats")

        insert_messages = 0

        async def sub_to_nats():
            nc = await nats_connect_ssl(nats_cluster)
            sub = await nc.subscribe(format_name)
            await sub.unsubscribe(2)
            async for msg in sub.messages:
                nonlocal insert_messages
                insert_messages += 1

            await sub.drain()
            await nc.drain()

        def run_sub():
            asyncio.run(sub_to_nats())

        thread = threading.Thread(target=run_sub)
        thread.start()
        time.sleep(1)

        instance.query(
            f"INSERT INTO test.nats select number*10 as key, number*100 as value from numbers({num_rows})"
        )

        thread.join()

        assert insert_messages == 2

        rows = instance.query_with_retry(
            "SELECT count() FROM test.view",
            retry_count = 100,
            check_callback = lambda query_result: int(query_result) == num_rows)
        assert int(rows) == num_rows

        expected = ""
        for i in range(num_rows):
            expected += str(i * 10) + "\t" + str(i * 100) + "\n"

        result = instance.query("SELECT * FROM test.view ORDER BY key")
        assert result == expected


def test_block_based_formats_1(nats_cluster):
    instance.query(
        """
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = 'PrettySpace',
                     nats_format = 'PrettySpace';
    """
    )

    insert_messages = []

    async def sub_to_nats():
        nc = await nats_connect_ssl(nats_cluster)
        sub = await nc.subscribe("PrettySpace")
        await sub.unsubscribe(3)
        async for msg in sub.messages:
            insert_messages.append(msg.data.decode())

        await sub.drain()
        await nc.drain()

    def run_sub():
        asyncio.run(sub_to_nats())

    thread = threading.Thread(target=run_sub)
    thread.start()
    time.sleep(1)

    instance.query_with_retry(
        "INSERT INTO test.nats SELECT number * 10 as key, number * 100 as value FROM numbers(5) settings max_block_size=2, optimize_trivial_insert_select=0;",
        retry_count = 100)
    thread.join()

    data = []
    for message in insert_messages:
        splitted = message.split("\n")

        assert len(splitted) >= 3
        assert splitted[0] == "    key   value"
        assert splitted[1] == ""
        assert splitted[-1] == ""

        for line in splitted[2:-1]:
            elements = line.split()
            assert len(elements) >= 3
            data += [[elements[1], elements[2]]]

    assert data == [
        ["0", "0"],
        ["10", "100"],
        ["20", "200"],
        ["30", "300"],
        ["40", "400"],
    ]


def test_block_based_formats_2(nats_cluster):
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
            DROP TABLE IF EXISTS test.nats;
               
            CREATE TABLE test.nats (key UInt64, value UInt64)
                ENGINE = NATS
                SETTINGS nats_url = 'nats1:4444',
                         nats_subjects = '{format_name}',
                         nats_format = '{format_name}';      
            """
        )
        nats_helpers.wait_for_table_is_ready(instance, "test.nats")

        instance.query(
            """
            CREATE MATERIALIZED VIEW test.view Engine=Log AS
            SELECT key, value FROM test.nats;
            """
        )
        nats_helpers.wait_for_mv_attached_to_table(instance, "test.nats")

        insert_messages = 0

        async def sub_to_nats():
            nc = await nats_connect_ssl(nats_cluster)
            sub = await nc.subscribe(format_name)
            await sub.unsubscribe(9)
            async for msg in sub.messages:
                nonlocal insert_messages
                insert_messages += 1

            await sub.drain()
            await nc.drain()

        def run_sub():
            asyncio.run(sub_to_nats())

        thread = threading.Thread(target=run_sub)
        thread.start()
        time.sleep(1)

        instance.query(
            f"INSERT INTO test.nats SELECT number * 10 as key, number * 100 as value FROM numbers({num_rows}) settings max_block_size=12, optimize_trivial_insert_select=0;"
        )

        thread.join()

        assert insert_messages == 9

        rows = instance.query_with_retry(
            "SELECT count() FROM test.view",
            retry_count = 100,
            check_callback = lambda query_result: int(query_result) == num_rows)
        assert int(rows) == num_rows

        result = instance.query("SELECT * FROM test.view ORDER by key")
        expected = ""
        for i in range(num_rows):
            expected += str(i * 10) + "\t" + str(i * 100) + "\n"
        assert result == expected


if __name__ == "__main__":
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()


def test_hiding_credentials(nats_cluster):
    table_name = 'test_hiding_credentials'
    instance.query(
        f"""
        DROP TABLE IF EXISTS test.{table_name};
        CREATE TABLE test.{table_name} (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = '{table_name}',
                     nats_format = 'TSV',
                     nats_username = '{nats_user}',
                     nats_password = '{nats_pass}',
                     nats_credential_file = '',
                     nats_row_delimiter = '\\n';
        """
    )

    instance.query("SYSTEM FLUSH LOGS")
    message = instance.query(f"SELECT message FROM system.text_log WHERE message ILIKE '%CREATE TABLE test.{table_name}%'")
    assert "nats_password = \\'[HIDDEN]\\'" in  message
    assert "nats_credential_file = \\'[HIDDEN]\\'" in  message
