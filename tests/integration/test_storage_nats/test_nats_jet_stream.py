import asyncio
import json
import logging
import os.path as p
import random
import threading
import time
import nats

import pytest
from google.protobuf.internal.encoder import _VarintBytes

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import nats_user, nats_pass
from helpers.test_tools import TSV

from . import common as nats_helpers
from . import nats_pb2

from nats.js import api

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

async def publish_messages(cluster_inst, stream, subject, messages=(), bytes=None):
    nc = await nats_helpers.nats_connect_ssl(cluster_inst)
    logging.debug("NATS connection status: " + str(nc.is_connected))

    for message in messages:
        await nc.jetstream().publish(subject, message.encode(), stream=stream)
    if bytes is not None:
        await nc.jetstream().publish(subject, bytes, stream=stream)
    await nc.flush()
    logging.debug("Finished publishing to " + subject)

    await nc.close()

async def receive_messages(cluster_inst, stream_name, consumer_name, subject, decode_data=True):
    nc = await nats_helpers.nats_connect_ssl(cluster_inst)
    js = nc.jetstream()

    result = []

    sub = await js.pull_subscribe(stream=stream_name, durable=consumer_name, subject=subject)

    try:
        while True:
            msgs = await sub.fetch(1)
            for msg in msgs:
                result.append(msg.data.decode() if decode_data else msg.data)
                await msg.ack()
    except nats.errors.TimeoutError:
        pass

    await sub.unsubscribe()

    await nc.drain()
    await nc.close()

    return result


async def add_stream(cluster_inst, stream_name, stream_subjects):
    nc = await nats_helpers.nats_connect_ssl(cluster_inst)
    logging.debug("NATS connection status: " + str(nc.is_connected))

    # Create JetStream context.
    js = nc.jetstream()

    stream_info = await js.add_stream(name=stream_name, subjects=stream_subjects)
    logging.debug("added NATS jet stream: " + str(stream_info))

    await nc.close()

async def get_stream_info(cluster_inst, stream_name) -> api.StreamInfo:
    nc = await nats_helpers.nats_connect_ssl(cluster_inst)
    logging.debug("NATS connection status: " + str(nc.is_connected))

    # Create JetStream context.
    js = nc.jetstream()

    stream_info = await js.stream_info(stream_name)
    logging.debug("recived NATS jet stream info: " + str(stream_info))

    await nc.close()
    return stream_info

async def delete_stream(cluster_inst, stream_name):
    nc = await nats_helpers.nats_connect_ssl(cluster_inst)
    logging.debug("NATS connection status: " + str(nc.is_connected))

    # Create JetStream context.
    js = nc.jetstream()

    # Persist messages on 'foo's subject.
    await js.delete_stream(name=stream_name)

    await nc.close()


async def add_durable_consumer(cluster_inst, stream_name, consumer_name):
    nc = await nats_helpers.nats_connect_ssl(cluster_inst)
    logging.debug("NATS connection status: " + str(nc.is_connected))

    # Create JetStream context.
    js = nc.jetstream()

    consumer_config = api.ConsumerConfig(name=consumer_name, durable_name=consumer_name)

    # Persist messages on 'foo's subject.
    consumer_info = await js.add_consumer(stream=stream_name, config=consumer_config)
    logging.debug("added durable NATS jet stream consumer: " + str(consumer_info))

    await nc.close()

async def delete_durable_consumer(cluster_inst, stream_name, consumer_name):
    nc = await nats_helpers.nats_connect_ssl(cluster_inst)
    logging.debug("NATS connection status: " + str(nc.is_connected))

    # Create JetStream context.
    js = nc.jetstream()

    # Persist messages on 'foo's subject.
    await js.delete_consumer(stream_name, consumer_name)

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

    asyncio.run(add_stream(cluster, "test_stream", ["test_subject", "right_insert1" ,"right_insert2"]))

    yield  # run test

    asyncio.run(delete_stream(cluster, "test_stream"))

    instance.query("DROP DATABASE test")


# Tests

def test_nats_select_empty(nats_cluster):

    asyncio.run(add_durable_consumer(cluster, "test_stream", "test_consumer"))

    instance.query(
        f"""
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_stream = 'test_stream',
                     nats_consumer_name = 'test_consumer',
                     nats_subjects = 'test_subject',
                     nats_format = 'TSV',
                     nats_row_delimiter = '\\n';
        """
    )

    assert int(instance.query("SELECT count() FROM test.nats")) == 0


def test_nats_select(nats_cluster):

    asyncio.run(add_durable_consumer(cluster, "test_stream", "test_consumer"))

    instance.query(
        f"""
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_stream = 'test_stream',
                     nats_consumer_name = 'test_consumer',
                     nats_subjects = 'test_subject',
                     nats_format = 'JSONEachRow',
                     nats_row_delimiter = '\\n';
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.nats;
        """
    )

    messages = []
    for i in range(50):
        messages.append(json.dumps({"key": i, "value": i}))
    asyncio.run(publish_messages(nats_cluster, "test_stream", "test_subject", messages))

    nats_helpers.check_query_result(instance, "SELECT * FROM test.view ORDER BY key")


def test_nats_json_without_delimiter(nats_cluster):

    asyncio.run(add_durable_consumer(cluster, "test_stream", "test_consumer"))

    instance.query(
        f"""
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_stream = 'test_stream',
                     nats_consumer_name = 'test_consumer',
                     nats_subjects = 'test_subject',
                     nats_format = 'JSONEachRow';
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.nats;
        """
    )

    messages = ""
    for i in range(25):
        messages += json.dumps({"key": i, "value": i}) + "\n"

    all_messages = [messages]
    asyncio.run(publish_messages(nats_cluster, "test_stream", "test_subject", all_messages))

    messages = ""
    for i in range(25, 50):
        messages += json.dumps({"key": i, "value": i}) + "\n"
    all_messages = [messages]
    asyncio.run(publish_messages(nats_cluster, "test_stream", "test_subject", all_messages))

    nats_helpers.check_query_result(instance, "SELECT * FROM test.view ORDER BY key")


def test_nats_csv_with_delimiter(nats_cluster):

    asyncio.run(add_durable_consumer(cluster, "test_stream", "test_consumer"))

    instance.query(
        f"""
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_stream = 'test_stream',
                     nats_consumer_name = 'test_consumer',
                     nats_subjects = 'test_subject',
                     nats_format = 'CSV',
                     nats_row_delimiter = '\\n';
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.nats;
        """
    )

    messages = []
    for i in range(50):
        messages.append("{i}, {i}".format(i=i))

    asyncio.run(publish_messages(nats_cluster, "test_stream", "test_subject", messages))

    nats_helpers.check_query_result(instance, "SELECT * FROM test.view ORDER BY key")


def test_nats_tsv_with_delimiter(nats_cluster):

    asyncio.run(add_durable_consumer(cluster, "test_stream", "test_consumer"))

    instance.query(
        f"""
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_stream = 'test_stream',
                     nats_consumer_name = 'test_consumer',
                     nats_subjects = 'test_subject',
                     nats_format = 'TSV',
                     nats_row_delimiter = '\\n';
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.nats;
        """
    )

    messages = []
    for i in range(50):
        messages.append("{i}\t{i}".format(i=i))

    asyncio.run(publish_messages(nats_cluster, "test_stream", "test_subject", messages))

    nats_helpers.check_query_result(instance, "SELECT * FROM test.view ORDER BY key")


def test_nats_macros(nats_cluster):

    asyncio.run(add_durable_consumer(cluster, "test_stream", "test_consumer"))

    instance.query(
        f"""
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = '{{nats_url}}',
                     nats_stream = '{{nats_stream}}',
                     nats_consumer_name = '{{nats_consumer_name}}',
                     nats_subjects = '{{nats_subjects}}',
                     nats_format = '{{nats_format}}';
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.nats;
        """
    )

    message = ""
    for i in range(50):
        message += json.dumps({"key": i, "value": i}) + "\n"
    asyncio.run(publish_messages(nats_cluster, "test_stream", "test_subject", [message]))

    nats_helpers.check_query_result(instance, "SELECT * FROM test.view ORDER BY key")


def test_nats_materialized_view(nats_cluster):

    asyncio.run(add_durable_consumer(cluster, "test_stream", "test_consumer"))

    instance.query(
        f"""
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_stream = 'test_stream',
                     nats_consumer_name = 'test_consumer',
                     nats_subjects = 'test_subject',
                     nats_format = 'JSONEachRow',
                     nats_row_delimiter = '\\n';
        CREATE TABLE test.view1 (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE TABLE test.view2 (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer1 TO test.view1 AS
            SELECT * FROM test.nats;
        CREATE MATERIALIZED VIEW test.consumer2 TO test.view2 AS
            SELECT * FROM test.nats;
        """
    )

    messages = []
    for i in range(50):
        messages.append(json.dumps({"key": i, "value": i}))
    asyncio.run(publish_messages(nats_cluster, "test_stream", "test_subject", messages))

    nats_helpers.check_query_result(instance, "SELECT * FROM test.view1 ORDER BY key")
    nats_helpers.check_query_result(instance, "SELECT * FROM test.view2 ORDER BY key")


def test_nats_materialized_view_with_subquery(nats_cluster):

    asyncio.run(add_durable_consumer(cluster, "test_stream", "test_consumer"))

    instance.query(
        f"""
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_stream = 'test_stream',
                     nats_consumer_name = 'test_consumer',
                     nats_subjects = 'test_subject',
                     nats_format = 'JSONEachRow',
                     nats_row_delimiter = '\\n';
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM (SELECT * FROM test.nats);
        """
    )

    messages = []
    for i in range(50):
        messages.append(json.dumps({"key": i, "value": i}))
    asyncio.run(publish_messages(nats_cluster, "test_stream", "test_subject", messages))

    nats_helpers.check_query_result(instance, "SELECT * FROM test.view ORDER BY key")


def test_nats_protobuf(nats_cluster):

    asyncio.run(add_durable_consumer(cluster, "test_stream", "test_consumer"))

    instance.query(
        f"""
        CREATE TABLE test.nats (key UInt64, value String)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_stream = 'test_stream',
                     nats_consumer_name = 'test_consumer',
                     nats_subjects = 'test_subject',
                     nats_format = 'Protobuf',
                     nats_schema = 'nats.proto:ProtoKeyValue';
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.nats;
        """
    )

    def produce_messages(range):
        data = b""
        for i in range:
            msg = nats_pb2.ProtoKeyValue()
            msg.key = i
            msg.value = str(i)
            serialized_msg = msg.SerializeToString()
            data = data + _VarintBytes(len(serialized_msg)) + serialized_msg
        asyncio.run(publish_messages(nats_cluster, "test_stream", "test_subject", bytes=data))

    produce_messages(range(0, 20))
    produce_messages(range(20, 21))
    produce_messages(range(21, 50))

    nats_helpers.check_query_result(instance, "SELECT * FROM test.view ORDER BY key")


def test_nats_big_message(nats_cluster):

    asyncio.run(add_durable_consumer(cluster, "test_stream", "test_consumer"))

    # Create batches of messages of size ~100Kb
    nats_messages = 100
    batch_messages = 1000
    messages = [
        json.dumps({"key": i, "value": "x" * 100}) * batch_messages
        for i in range(nats_messages)
    ]

    instance.query(
        f"""
        CREATE TABLE test.nats (key UInt64, value String)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_stream = 'test_stream',
                     nats_consumer_name = 'test_consumer',
                     nats_subjects = 'test_subject',
                     nats_format = 'JSONEachRow';
        CREATE TABLE test.view (key UInt64, value String)
            ENGINE = MergeTree
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.nats;
        """
    )

    asyncio.run(publish_messages(nats_cluster, "test_stream", "test_subject", messages))

    nats_helpers.wait_query_result(instance, "SELECT count() FROM test.view", batch_messages * nats_messages)

def test_nats_mv_combo(nats_cluster):

    asyncio.run(add_durable_consumer(cluster, "test_stream", "test_consumer"))

    NUM_MV = 5
    NUM_CONSUMERS = 4

    instance.query(
        f"""
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_stream = 'test_stream',
                     nats_consumer_name = 'test_consumer',
                     nats_subjects = 'test_subject',
                     nats_num_consumers = {NUM_CONSUMERS},
                     nats_format = 'JSONEachRow',
                     nats_row_delimiter = '\\n';
        """
    )

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

    i = [0]
    messages_num = 10000

    def produce():
        messages = []
        for _ in range(messages_num):
            messages.append(json.dumps({"key": i[0], "value": i[0]}))
            i[0] += 1
        asyncio.run(publish_messages(nats_cluster, "test_stream", "test_subject", messages))

    threads = []
    threads_num = 20

    for _ in range(threads_num):
        threads.append(threading.Thread(target=produce))
    for thread in threads:
        time.sleep(random.uniform(0, 1))
        thread.start()

    for thread in threads:
        thread.join()

    time_limit_sec = 300
    deadline = time.monotonic() + time_limit_sec

    expected_result = messages_num * threads_num * NUM_MV

    while time.monotonic() < deadline:
        result = 0
        for mv_id in range(NUM_MV):
            result += int(
                instance.query("SELECT count() FROM test.combo_{0}".format(mv_id))
            )
        if int(result) == expected_result:
            break
        time.sleep(1)

    if int(result) == expected_result:
        return

    stream_info = asyncio.run(get_stream_info(nats_cluster, "test_stream"))
    assert (stream_info.state.messages == messages_num * threads_num
    ), "NATS server lost some messages: {}".format(stream_info.state.messages)

    assert (
        int(result) == expected_result
    ), "Clickhouse server lost some messages: {}".format(result)


def test_nats_insert(nats_cluster):

    asyncio.run(add_durable_consumer(cluster, "test_stream", "test_consumer"))

    instance.query(
        f"""
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_stream = 'test_stream',
                     nats_subjects = 'test_subject',
                     nats_format = 'TSV',
                     nats_row_delimiter = '\\n';
        """
    )

    values = []
    for i in range(50):
        values.append("({i}, {i})".format(i=i))
    values = ",".join(values)

    instance.query_with_retry("INSERT INTO test.nats VALUES {}".format(values))

    insert_messages = asyncio.run(receive_messages(nats_cluster, "test_stream", "test_consumer", "test_subject"))

    result = "\n".join(insert_messages)
    nats_helpers.check_result(result, True)


def test_fetching_messages_without_mv(nats_cluster):

    asyncio.run(add_durable_consumer(cluster, "test_stream", "test_consumer"))

    instance.query(
        f"""
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_stream = 'test_stream',
                     nats_subjects = 'test_subject',
                     nats_format = 'TSV',
                     nats_row_delimiter = '\\n';
        """
    )

    values = []
    for i in range(50):
        values.append("({i}, {i})".format(i=i))
    values = ",".join(values)

    instance.query_with_retry("INSERT INTO test.nats VALUES {}".format(values))

    insert_messages = asyncio.run(receive_messages(nats_cluster, "test_stream", "test_consumer", "test_subject"))
    result = "\n".join(insert_messages)
    nats_helpers.check_result(result, True)

def test_nats_many_subjects_insert_wrong(nats_cluster):

    instance.query(
        f"""
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_stream = 'test_stream',
                     nats_subjects = 'insert1,insert2.>,insert3.*.foo',
                     nats_format = 'TSV',
                     nats_row_delimiter = '\\n';
        """
    )

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

    asyncio.run(add_durable_consumer(cluster, "test_stream", "test_consumer"))

    instance.query(
        f"""
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_stream = 'test_stream',
                     nats_subjects = 'right_insert1,right_insert2',
                     nats_format = 'TSV',
                     nats_row_delimiter = '\\n';
        """
    )

    values = []
    for i in range(50):
        values.append("({i}, {i})".format(i=i))
    values = ",".join(values)

    instance.query_with_retry("INSERT INTO test.nats SETTINGS stream_like_engine_insert_queue='right_insert1' VALUES {}".format(values))

    insert_messages = asyncio.run(receive_messages(nats_cluster, "test_stream", "test_consumer", "right_insert1"))
    result = "\n".join(insert_messages)
    nats_helpers.check_result(result, True)


def test_nats_many_inserts(nats_cluster):

    asyncio.run(add_durable_consumer(cluster, "test_stream", "test_consumer"))

    instance.query(
        f"""
        CREATE TABLE test.nats_many (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_stream = 'test_stream',
                     nats_subjects = 'test_subject',
                     nats_format = 'TSV',
                     nats_row_delimiter = '\\n';
        CREATE TABLE test.nats_consume (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_stream = 'test_stream',
                     nats_consumer_name = 'test_consumer',
                     nats_subjects = 'test_subject',
                     nats_format = 'TSV',
                     nats_row_delimiter = '\\n';
        CREATE TABLE test.view_many (key UInt64, value UInt64)
            ENGINE = MergeTree
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer_many TO test.view_many AS
            SELECT * FROM test.nats_consume;
        """
    )

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
        check_callback = lambda query_result: int(query_result) >= messages_num * threads_num)

    assert (
        int(result) == messages_num * threads_num
    ), "ClickHouse lost some messages or got duplicated ones. Total count: {}".format(
        result
    )


def test_nats_overloaded_insert(nats_cluster):

    asyncio.run(add_durable_consumer(cluster, "test_stream", "test_consumer"))

    instance.query(
        f"""
        CREATE TABLE test.nats_consume (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_stream = 'test_stream',
                     nats_consumer_name = 'test_consumer',
                     nats_subjects = 'test_subject',
                     nats_num_consumers = 5,
                     nats_max_block_size = 10000,
                     nats_format = 'TSV',
                     nats_row_delimiter = '\\n';
        CREATE TABLE test.nats_overload (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_stream = 'test_stream',
                     nats_subjects = 'test_subject',
                     nats_format = 'TSV',
                     nats_row_delimiter = '\\n';
        CREATE TABLE test.view_overload (key UInt64, value UInt64)
            ENGINE = MergeTree
            ORDER BY key
            SETTINGS old_parts_lifetime=5, cleanup_delay_period=2, cleanup_delay_period_random_add=3,
            cleanup_thread_preferred_points_per_iteration=0;
        CREATE MATERIALIZED VIEW test.consumer_overload TO test.view_overload AS
            SELECT * FROM test.nats_consume;
        """
    )

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

    if int(result) != messages_num * threads_num:
        repeated_msgs = TSV(
            instance.query(
                f"SELECT key, count() AS count FROM test.view_overload GROUP BY key HAVING count != {threads_num} ORDER BY count DESC"
            )
        )

        assert (
            False
        ), f"ClickHouse lost some messages or got duplicated ones. Total count: {result}, problematic messages: {repeated_msgs}"


def test_nats_virtual_column(nats_cluster):

    asyncio.run(add_durable_consumer(cluster, "test_stream", "test_consumer"))

    instance.query(
        f"""
        CREATE TABLE test.nats_virtuals (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_stream = 'test_stream',
                     nats_consumer_name = 'test_consumer',
                     nats_subjects = 'test_subject',
                     nats_format = 'JSONEachRow';
        CREATE MATERIALIZED VIEW test.view Engine=Log AS
            SELECT value, key, _subject FROM test.nats_virtuals;
        """
    )

    message_num = 10
    i = 0
    messages = []
    for _ in range(message_num):
        messages.append(json.dumps({"key": i, "value": i}))
        i += 1

    asyncio.run(publish_messages(nats_cluster, "test_stream", "test_subject", messages))
    nats_helpers.wait_query_result(instance, "SELECT count() FROM test.view", message_num)

    result = instance.query(
        """
        SELECT key, value, _subject
        FROM test.view ORDER BY key
        """
    )

    expected = """\
0	0	test_subject
1	1	test_subject
2	2	test_subject
3	3	test_subject
4	4	test_subject
5	5	test_subject
6	6	test_subject
7	7	test_subject
8	8	test_subject
9	9	test_subject
"""

    assert TSV(result) == TSV(expected)


def test_nats_virtual_column_with_materialized_view(nats_cluster):

    asyncio.run(add_durable_consumer(cluster, "test_stream", "test_consumer"))

    instance.query(
        f"""
        CREATE TABLE test.nats_virtuals_mv (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_stream = 'test_stream',
                     nats_consumer_name = 'test_consumer',
                     nats_subjects = 'test_subject',
                     nats_format = 'JSONEachRow';
        CREATE TABLE test.view (key UInt64, value UInt64, subject String) ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT *, _subject as subject
            FROM test.nats_virtuals_mv;
        """
    )

    message_num = 10
    i = 0
    messages = []
    for _ in range(message_num):
        messages.append(json.dumps({"key": i, "value": i}))
        i += 1

    asyncio.run(publish_messages(nats_cluster, "test_stream", "test_subject", messages))
    nats_helpers.wait_query_result(instance, "SELECT count() FROM test.view", message_num)

    result = instance.query("SELECT key, value, subject FROM test.view ORDER BY key")
    expected = """\
0	0	test_subject
1	1	test_subject
2	2	test_subject
3	3	test_subject
4	4	test_subject
5	5	test_subject
6	6	test_subject
7	7	test_subject
8	8	test_subject
9	9	test_subject
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
        asyncio.run(add_durable_consumer(cluster, "test_stream", f"test_consumer_{table_id}"))

        logging.debug(f"Setting up table {table_id}")
        instance.query(
            f"""
            CREATE TABLE test.many_consumers_{table_id} (key UInt64, value UInt64)
                ENGINE = NATS
                SETTINGS nats_url = 'nats1:4444',
                         nats_stream = 'test_stream',
                         nats_consumer_name = 'test_consumer_{table_id}',
                         nats_subjects = 'test_subject',
                         nats_num_consumers = 2,
                         nats_queue_group = 'many_consumers',
                         nats_format = 'JSONEachRow',
                         nats_row_delimiter = '\\n';
            CREATE MATERIALIZED VIEW test.many_consumers_{table_id}_mv TO test.destination AS
                SELECT key, value FROM test.many_consumers_{table_id};
            """
        )

    i = [0]
    messages_num = 1000

    def produce():
        messages = []
        for _ in range(messages_num):
            messages.append(json.dumps({"key": i[0], "value": i[0]}))
            i[0] += 1
        asyncio.run(publish_messages(nats_cluster, "test_stream", "test_subject", messages))

    threads = []
    threads_num = 20

    for _ in range(threads_num):
        threads.append(threading.Thread(target=produce))
    for thread in threads:
        time.sleep(random.uniform(0, 1))
        thread.start()
    for thread in threads:
        thread.join()

    nats_helpers.wait_query_result(instance, "SELECT count() FROM test.destination", messages_num * threads_num * num_tables)


def test_nats_restore_failed_connection_without_losses_on_write(nats_cluster):

    asyncio.run(add_durable_consumer(cluster, "test_stream", "test_consumer"))

    instance.query(
        f"""
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree
            ORDER BY key;
        CREATE TABLE test.consume (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_stream = 'test_stream',
                     nats_consumer_name = 'test_consumer',
                     nats_subjects = 'test_subject',
                     nats_format = 'JSONEachRow',
                     nats_num_consumers = 2,
                     nats_row_delimiter = '\\n';
        CREATE TABLE test.producer_reconnect (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_stream = 'test_stream',
                     nats_subjects = 'test_subject',
                     nats_format = 'JSONEachRow',
                     nats_row_delimiter = '\\n';
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.consume;
        """
    )

    messages_num = 100000
    values = []
    for i in range(messages_num):
        values.append("({i}, {i})".format(i=i))
    values = ",".join(values)

    instance.query_with_retry("INSERT INTO test.producer_reconnect VALUES {}".format(values))

    result = instance.query_with_retry("SELECT count() FROM test.view", sleep_time = 0.1, check_callback = lambda num_rows: int(num_rows) != 0)
    assert(int(result) != 0)

    nats_helpers.kill_nats(nats_cluster)
    time.sleep(4)
    nats_helpers.revive_nats(nats_cluster)

    result = instance.query_with_retry(
        "SELECT count(DISTINCT key) FROM test.view",
        retry_count = 300,
        sleep_time = 1,
        check_callback = lambda num_rows: int(num_rows) == messages_num)
    assert int(result) == messages_num, "ClickHouse lost some messages: {}".format(result)


def test_nats_no_connection_at_startup_1(nats_cluster):

    asyncio.run(add_durable_consumer(cluster, "test_stream", "test_consumer"))

    assert(
        "Cannot connect to Nats"
        in
        instance.query_and_get_error(
            f"""
            CREATE TABLE test.cs (key UInt64, value UInt64)
                ENGINE = NATS
                SETTINGS nats_url = 'invalid_nats_url:4444',
                        nats_stream = 'test_stream',
                        nats_consumer_name = 'test_consumer',
                        nats_subjects = 'test_subject',
                        nats_format = 'JSONEachRow',
                        nats_num_consumers = '5',
                        nats_row_delimiter = '\\n';
            """
    ))
    assert "Table `cs` doesn't exist" in instance.query_and_get_error("SHOW TABLE test.cs;")


def test_nats_no_connection_at_startup_2(nats_cluster):

    asyncio.run(add_durable_consumer(cluster, "test_stream", "test_consumer"))

    instance.query(
        f"""
        CREATE TABLE test.cs (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_stream = 'test_stream',
                     nats_consumer_name = 'test_consumer',
                     nats_subjects = 'test_subject',
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

    asyncio.run(add_durable_consumer(cluster, "test_stream", "test_consumer"))

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.cs;
        """
    )

    messages_num = 1000
    messages = []
    for i in range(messages_num):
        messages.append(json.dumps({"key": i, "value": i}))
    asyncio.run(publish_messages(nats_cluster, "test_stream", "test_subject", messages))

    result = instance.query_with_retry(
        "SELECT count() FROM test.view",
        retry_count = 20,
        sleep_time = 1,
        check_callback = lambda num_rows: int(num_rows) == messages_num)
    assert int(result) == messages_num, "ClickHouse lost some messages: {}".format(result)


def test_nats_format_factory_settings(nats_cluster):

    asyncio.run(add_durable_consumer(cluster, "test_stream", "test_consumer"))

    instance.query(
        f"""
        CREATE TABLE test.format_settings (
            id String, date DateTime
        ) ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_stream = 'test_stream',
                     nats_consumer_name = 'test_consumer',
                     nats_subjects = 'test_subject',
                     nats_format = 'JSONEachRow',
                     date_time_input_format = 'best_effort';
        CREATE TABLE test.view (id String, date DateTime) ENGINE = MergeTree ORDER BY id;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.format_settings;
        """
    )

    message = json.dumps(
        {"id": "format_settings_test", "date": "2021-01-19T14:42:33.1829214Z"}
    )
    expected = instance.query(
        """SELECT parseDateTimeBestEffort(CAST('2021-01-19T14:42:33.1829214Z', 'String'))"""
    )
    asyncio.run(publish_messages(nats_cluster, "test_stream", "test_subject", [message]))

    result = instance.query_with_retry("SELECT date FROM test.view", check_callback = lambda result: result == expected)

    assert result == expected

def test_nats_bad_args(nats_cluster):
    asyncio.run(add_durable_consumer(cluster, "test_stream", "test_consumer"))

    instance.query(
        f"""
        CREATE TABLE test.producer_table (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_stream = 'test_stream',
                     nats_subjects = 'test_subject',
                     nats_format = 'JSONEachRow';
        """
    )
    assert(
        "To read from NATS jet stream, you must specify `nats_consumer_name` setting"
        in
        instance.query_and_get_error("SELECT * FROM test.producer_table"))

    assert(
        "To use NATS jet stream, you must specify `nats_stream` setting"
        in
        instance.query_and_get_error(
            f"""
            CREATE TABLE test.drop (key UInt64, value UInt64)
                ENGINE = NATS
                SETTINGS nats_url = 'nats1:4444',
                        nats_consumer_name = 'test_consumer',
                        nats_subjects = 'test_subject',
                        nats_format = 'JSONEachRow';
            """))


def test_nats_drop_mv(nats_cluster):

    asyncio.run(add_durable_consumer(cluster, "test_stream", "test_consumer"))

    instance.query(
        f"""
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_stream = 'test_stream',
                     nats_consumer_name = 'test_consumer',
                     nats_subjects = 'test_subject',
                     nats_format = 'JSONEachRow';
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.nats;
        """
    )

    messages = []
    for i in range(20):
        messages.append(json.dumps({"key": i, "value": i}))
    asyncio.run(publish_messages(nats_cluster, "test_stream", "test_subject", messages))

    nats_helpers.wait_query_result(instance, "SELECT count() FROM test.view", 20)

    instance.query("DROP VIEW test.consumer")
    nats_helpers.wait_for_table_is_ready(instance, "test.nats")

    messages = []
    for i in range(20, 40):
        messages.append(json.dumps({"key": i, "value": i}))
    asyncio.run(publish_messages(nats_cluster, "test_stream", "test_subject", messages))

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.nats;
        """
    )
    nats_helpers.wait_query_result(instance, "SELECT count() FROM test.view", 40)

    instance.query("DROP VIEW test.consumer")

    messages = []
    for i in range(40, 50):
        messages.append(json.dumps({"key": i, "value": i}))
    asyncio.run(publish_messages(nats_cluster, "test_stream", "test_subject", messages))

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.nats;
        """
    )
    nats_helpers.check_query_result(instance, "SELECT * FROM test.view ORDER BY key")


def test_nats_predefined_configuration(nats_cluster):

    asyncio.run(add_durable_consumer(cluster, "test_stream", "test_consumer"))

    instance.query(
        f"""
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS(nats_pull_consumer);
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.nats;
        """
    )

    asyncio.run(publish_messages(nats_cluster, "test_stream", "test_subject", [json.dumps({"key": 1, "value": 2})]))

    result = instance.query_with_retry(
        "SELECT * FROM test.view ORDER BY key",
        ignore_error = True,
        check_callback = lambda query_result: query_result == "1\t2\n")

    assert result == "1\t2\n"


def test_format_with_prefix_and_suffix(nats_cluster):

    asyncio.run(add_durable_consumer(cluster, "test_stream", "test_consumer"))

    instance.query(
        f"""
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_stream = 'test_stream',
                     nats_consumer_name = 'test_consumer',
                     nats_subjects = 'test_subject',
                     nats_format = 'CustomSeparated';
        """
    )

    instance.query(
        "INSERT INTO test.nats select number*10 as key, number*100 as value from numbers(2) settings format_custom_result_before_delimiter='<prefix>\n', format_custom_result_after_delimiter='<suffix>\n'"
    )

    insert_messages = asyncio.run(receive_messages(nats_cluster, "test_stream", "test_consumer", "test_subject"))
    assert (
        "".join(insert_messages)
        == "<prefix>\n0\t0\n<suffix>\n<prefix>\n10\t100\n<suffix>\n"
    )


def test_max_rows_per_message(nats_cluster):

    asyncio.run(add_durable_consumer(cluster, "test_stream", "table_consumer"))
    asyncio.run(add_durable_consumer(cluster, "test_stream", "external_consumer"))

    instance.query(
        f"""
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_stream = 'test_stream',
                     nats_consumer_name = 'table_consumer',
                     nats_subjects = 'test_subject',
                     nats_format = 'CustomSeparated',
                     nats_max_rows_per_message = 3,
                     format_custom_result_before_delimiter = '<prefix>\n',
                     format_custom_result_after_delimiter = '<suffix>\n';
        CREATE MATERIALIZED VIEW test.view Engine=Log AS
            SELECT key, value FROM test.nats;
        """
    )

    num_rows = 5
    instance.query(
        f"INSERT INTO test.nats select number*10 as key, number*100 as value from numbers({num_rows}) settings format_custom_result_before_delimiter='<prefix>\n', format_custom_result_after_delimiter='<suffix>\n'"
    )

    insert_messages = asyncio.run(receive_messages(nats_cluster, "test_stream", "external_consumer", "test_subject"))
    assert (
        "".join(insert_messages)
        == "<prefix>\n0\t0\n10\t100\n20\t200\n<suffix>\n<prefix>\n30\t300\n40\t400\n<suffix>\n"
    )

    result = instance.query_with_retry("SELECT count() FROM test.view", retry_count = 100, check_callback = lambda result: int(result) == num_rows)
    assert int(result) == num_rows

    result = instance.query("SELECT * FROM test.view ORDER BY key")
    assert result == "0\t0\n10\t100\n20\t200\n30\t300\n40\t400\n"


def test_row_based_formats(nats_cluster):

    asyncio.run(add_durable_consumer(cluster, "test_stream", "table_consumer"))
    asyncio.run(add_durable_consumer(cluster, "test_stream", "external_consumer"))

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
                         nats_stream = 'test_stream',
                         nats_consumer_name = 'table_consumer',
                         nats_subjects = 'test_subject',
                         nats_format = '{format_name}';
            CREATE MATERIALIZED VIEW test.view Engine=Log AS
                SELECT key, value FROM test.nats;
            """
        )

        instance.query(
            f"INSERT INTO test.nats select number*10 as key, number*100 as value from numbers({num_rows})"
        )

        insert_messages = asyncio.run(receive_messages(nats_cluster, "test_stream", "external_consumer", 'test_subject', decode_data=False))
        assert len(insert_messages) == num_rows

        rows = instance.query_with_retry("SELECT count() FROM test.view", retry_count = 100, check_callback = lambda result: int(result) == num_rows)
        assert int(rows) == num_rows

        expected = ""
        for i in range(num_rows):
            expected += str(i * 10) + "\t" + str(i * 100) + "\n"

        result = instance.query("SELECT * FROM test.view ORDER BY key")
        assert result == expected


def test_block_based_formats_1(nats_cluster):

    asyncio.run(add_durable_consumer(cluster, "test_stream", "test_consumer"))

    instance.query(
        f"""
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_stream = 'test_stream',
                     nats_consumer_name = 'test_consumer',
                     nats_subjects = 'test_subject',
                     nats_format = 'PrettySpace';
        """
    )

    instance.query_with_retry(
        "INSERT INTO test.nats SELECT number * 10 as key, number * 100 as value FROM numbers(5) settings max_block_size=2, optimize_trivial_insert_select=0;",
        retry_count=100)

    data = []
    for message in asyncio.run(receive_messages(nats_cluster, "test_stream", "test_consumer", "test_subject")):
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

    asyncio.run(add_durable_consumer(cluster, "test_stream", "table_consumer"))
    asyncio.run(add_durable_consumer(cluster, "test_stream", "external_consumer"))

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
                         nats_stream = 'test_stream',
                         nats_consumer_name = 'table_consumer',
                         nats_subjects = 'test_subject',
                         nats_format = '{format_name}';
            CREATE MATERIALIZED VIEW test.view Engine=Log AS
                SELECT key, value FROM test.nats;
            """
        )

        instance.query(
            f"INSERT INTO test.nats SELECT number * 10 as key, number * 100 as value FROM numbers({num_rows}) settings max_block_size=12, optimize_trivial_insert_select=0;"
        )

        insert_messages = asyncio.run(receive_messages(nats_cluster, "test_stream", "external_consumer", "test_subject", decode_data=False))
        assert len(insert_messages) == 9

        rows = instance.query_with_retry("SELECT count() FROM test.view", retry_count = 100, check_callback = lambda result: int(result) == num_rows)
        assert int(rows) == num_rows

        result = instance.query("SELECT * FROM test.view ORDER by key")
        expected = ""
        for i in range(num_rows):
            expected += str(i * 10) + "\t" + str(i * 100) + "\n"
        assert result == expected


def test_hiding_credentials(nats_cluster):

    asyncio.run(add_durable_consumer(cluster, "test_stream", "test_consumer"))

    table_name = 'test_hiding_credentials'
    instance.query(
        f"""
        DROP TABLE IF EXISTS test.{table_name};
        CREATE TABLE test.{table_name} (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_stream = 'test_stream',
                     nats_consumer_name = 'test_consumer',
                     nats_subjects = 'test_subject',
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


if __name__ == "__main__":
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
