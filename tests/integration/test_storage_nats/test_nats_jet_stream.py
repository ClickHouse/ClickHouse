import asyncio
import json
import logging
import random
import threading
import time
import nats

import pytest
from google.protobuf.internal.encoder import _VarintBytes

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
        "configs/disable_insertion.xml",
    ],
    user_configs=["configs/users.xml"],
    with_nats=True,
    clickhouse_path_dir="clickhouse_path",
    stay_alive=True,
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


async def add_durable_consumer(cluster_inst, stream_name, consumer_name, ack_wait_sec = None):
    nc = await nats_helpers.nats_connect_ssl(cluster_inst)
    logging.debug("NATS connection status: " + str(nc.is_connected))

    # Create JetStream context.
    js = nc.jetstream()

    consumer_config = api.ConsumerConfig(name=consumer_name, durable_name=consumer_name)
    if ack_wait_sec is not None:
        consumer_config.ack_wait = ack_wait_sec

    # Persist messages on 'foo's subject.
    consumer_info = await js.add_consumer(stream=stream_name, config=consumer_config)
    logging.debug("added durable NATS jet stream consumer: " + str(consumer_info))

    await nc.close()


async def get_consumer_info(cluster_inst, stream_name, consumer_name):
    nc = await nats_helpers.nats_connect_ssl(cluster_inst)
    consumer_info = await nc.jetstream().consumer_info(stream_name, consumer_name)
    await nc.close()
    return consumer_info

async def delete_durable_consumer(cluster_inst, stream_name, consumer_name):
    nc = await nats_helpers.nats_connect_ssl(cluster_inst)
    logging.debug("NATS connection status: " + str(nc.is_connected))

    # Create JetStream context.
    js = nc.jetstream()

    # Persist messages on 'foo's subject.
    await js.delete_consumer(stream_name, consumer_name)

    await nc.close()


async def get_num_ack_pending(cluster_inst, stream_name, consumer_name):
    nc = await nats_helpers.nats_connect_ssl(cluster_inst)
    js = nc.jetstream()

    consumer_info = await js.consumer_info(stream_name, consumer_name)

    await nc.close()
    return consumer_info.num_ack_pending


async def get_delivered_consumer_seq(cluster_inst, stream_name, consumer_name):
    nc = await nats_helpers.nats_connect_ssl(cluster_inst)
    js = nc.jetstream()

    consumer_info = await js.consumer_info(stream_name, consumer_name)

    await nc.close()
    return consumer_info.delivered.consumer_seq


async def get_num_waiting(cluster_inst, stream_name, consumer_name):
    nc = await nats_helpers.nats_connect_ssl(cluster_inst)
    js = nc.jetstream()

    consumer_info = await js.consumer_info(stream_name, consumer_name)

    await nc.close()
    return consumer_info.num_waiting


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
        """
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
        """
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


def test_disable_insertion_and_mutation_disables_streaming(nats_cluster):
    asyncio.run(add_durable_consumer(cluster, "test_stream", "test_consumer"))

    try:
        instance.replace_in_config(
            "/etc/clickhouse-server/config.d/disable_insertion.xml",
            "<disable_insertion_and_mutation>0</disable_insertion_and_mutation>",
            "<disable_insertion_and_mutation>1</disable_insertion_and_mutation>",
        )
        # `disable_insertion_and_mutation` is a startup-only server setting.
        instance.restart_clickhouse()

        assert (
            "true"
            == instance.query(
                "SELECT getServerSetting('disable_insertion_and_mutation')"
            ).strip()
        )

        instance.query(
            """
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

        error_patterns = [
            "Insert queries are prohibited",
            "Message queue insertion is disabled",
            "Failed to process data",
        ]
        error_counts = {
            pattern: int(instance.count_in_log(pattern)) for pattern in error_patterns
        }

        messages = [json.dumps({"key": i, "value": i}) for i in range(10)]
        asyncio.run(
            publish_messages(
                nats_cluster, "test_stream", "test_subject", messages
            )
        )
        instance.query(
            "INSERT INTO test.nats FORMAT JSONEachRow"
            ' {"key": 999, "value": 999}'
        )

        time.sleep(10)
        assert 0 == int(instance.query("SELECT count() FROM test.view"))
        assert 0 == asyncio.run(
            get_consumer_info(nats_cluster, "test_stream", "test_consumer")
        ).num_ack_pending
        for pattern, count in error_counts.items():
            assert count == int(instance.count_in_log(pattern))

        instance.replace_in_config(
            "/etc/clickhouse-server/config.d/disable_insertion.xml",
            "<disable_insertion_and_mutation>1</disable_insertion_and_mutation>",
            "<disable_insertion_and_mutation>0</disable_insertion_and_mutation>",
        )
        instance.restart_clickhouse()

        assert 11 == int(
            instance.query_with_retry(
                "SELECT count() FROM test.view",
                check_callback=lambda result: int(result) == 11,
                retry_count=100,
            )
        )
    finally:
        instance.replace_in_config(
            "/etc/clickhouse-server/config.d/disable_insertion.xml",
            "<disable_insertion_and_mutation>1</disable_insertion_and_mutation>",
            "<disable_insertion_and_mutation>0</disable_insertion_and_mutation>",
        )
        instance.restart_clickhouse()


def test_nats_json_without_delimiter(nats_cluster):

    asyncio.run(add_durable_consumer(cluster, "test_stream", "test_consumer"))

    instance.query(
        """
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
        """
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
        """
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
        """
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = '{nats_url}',
                     nats_stream = '{nats_stream}',
                     nats_consumer_name = '{nats_consumer_name}',
                     nats_subjects = '{nats_subjects}',
                     nats_format = '{nats_format}';
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
        """
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
        """
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
        """
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
        """
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
    ), "ClickHouse server lost some messages: {}".format(result)


def test_nats_insert(nats_cluster):

    asyncio.run(add_durable_consumer(cluster, "test_stream", "test_consumer"))

    instance.query(
        """
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
        """
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
        """
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
        "This NATS engine reads from multiple subjects. You must specify `stream_like_engine_insert_queue` to choose the subject to write to"
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
        """
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
        """
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
        """
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

        instance.query_with_retry(
            "INSERT INTO test.nats_overload VALUES {}".format(values),
            settings={"receive_timeout": 600},
        )

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
        """
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
        """
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
        """
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


RESUBSCRIBE_LOG_LINE = "A subscription stopped consuming from the NATS server, resubscribing"


def _setup_restart_table(subject, consumer_name):
    asyncio.run(add_durable_consumer(cluster, "test_stream", consumer_name))

    instance.query(
        """
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree
            ORDER BY key;
        CREATE TABLE test.consume (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_stream = 'test_stream',
                     nats_consumer_name = '{consumer_name}',
                     nats_subjects = '{subject}',
                     nats_format = 'JSONEachRow',
                     nats_row_delimiter = '\\n';
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.consume;
        """.format(subject=subject, consumer_name=consumer_name)
    )
    nats_helpers.wait_for_streaming_started(instance, "test.consume")


def _publish_and_expect(subject, keys, total_expected):
    """Publish `keys` and wait until the view holds `total_expected` distinct keys."""
    messages = [json.dumps({"key": key, "value": key}) for key in keys]
    asyncio.run(publish_messages(cluster, "test_stream", subject, messages))

    result = instance.query_with_retry(
        "SELECT count(DISTINCT key) FROM test.view",
        retry_count = 60,
        sleep_time = 1,
        check_callback = lambda num_rows: int(num_rows) == total_expected)
    assert int(result) == total_expected, "consumption did not resume, view holds {} of {} keys".format(
        result, total_expected)


def _wait_for_parked_pull_request(consumer_name = "test_consumer", time_limit_sec = 60):
    # Waits until the consumer has a pull request parked server side. `num_waiting` counts exactly
    # those, so this reads the precondition off the broker rather than inferring it, and observes the
    # parked request without publishing anything.
    deadline = time.monotonic() + time_limit_sec
    num_waiting = 0
    while time.monotonic() < deadline:
        num_waiting = asyncio.run(get_num_waiting(cluster, "test_stream", consumer_name))
        if num_waiting >= 1:
            return
        time.sleep(0.2)

    raise AssertionError(
        "no pull request is parked for consumer {}: num_waiting is {}".format(consumer_name, num_waiting))


def _restart_nats(nats_cluster, kill = nats_helpers.kill_nats):
    # Restarts the broker with a pull request parked, which is the state that used to leave the table
    # subscribed and permanently silent.
    #
    # Recovery keys on two things, and a restart can land on either: a graceful shutdown answers the
    # outstanding pull request, which closes the subscription client side, while a restart landing in
    # the milliseconds between a re-subscribe and its request being parked leaves the client holding a
    # subscription it has no status for. The reconnect itself is what reports the second one, so no
    # restart needs to be excluded here, and which one a restart lands on need not be known.
    _wait_for_parked_pull_request()

    kill(nats_cluster)
    time.sleep(4)
    nats_helpers.revive_nats(nats_cluster)


def test_nats_jet_stream_resumes_consuming_after_broker_restart(nats_cluster):
    # An asynchronous JetStream pull request is renewed only when a message is delivered, and a
    # reconnect resends the `SUB` line but not the outstanding request, so a broker restart with
    # nothing in flight used to leave the table subscribed and permanently silent. Draining the
    # backlog first is what puts the pull chain in that idle state deterministically.
    _setup_restart_table("test_subject", "test_consumer")

    total_expected = 10
    _publish_and_expect("test_subject", range(0, 10), total_expected)

    _restart_nats(nats_cluster)

    total_expected += 10
    _publish_and_expect("test_subject", range(100, 110), total_expected)


def test_nats_jet_stream_resumes_consuming_after_broker_hard_kill(nats_cluster):
    # A hard kill answers nothing, so no subscription is ever reported as closed: the client keeps
    # handles that still look healthy while the request they wait on died with the broker. Only the
    # reconnect reports this, and it is the same state a restart reaches when it lands between a
    # re-subscribe and its request being parked - the window that made a graceful restart flaky.
    _setup_restart_table("test_subject", "test_consumer")

    total_expected = 10
    _publish_and_expect("test_subject", range(0, 10), total_expected)

    _restart_nats(nats_cluster, kill = nats_helpers.hard_kill_nats)

    total_expected += 10
    _publish_and_expect("test_subject", range(100, 110), total_expected)


IN_SOURCE_RESUBSCRIBE_LOG_LINE = (
    "A subscription stopped consuming from the NATS server, resubscribing within a running query"
)


def test_nats_jet_stream_direct_select_resumes_after_broker_hard_kill(nats_cluster):
    # Unlike the materialized-view path, a direct SELECT owns its consumer until the query ends.
    # Keep that query waiting on a parked pull request, then hard-kill the broker: a reconnect
    # restores the NATS `SUB`, but not the JetStream pull request. The source must therefore
    # re-subscribe itself before the message published after the reconnect can arrive.
    asyncio.run(add_durable_consumer(cluster, "test_stream", "test_consumer"))

    instance.query(
        """
        CREATE TABLE test.consume (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_stream = 'test_stream',
                     nats_consumer_name = 'test_consumer',
                     nats_subjects = 'test_subject',
                     nats_format = 'JSONEachRow',
                     nats_row_delimiter = '\\n';
        """
    )

    # The query has to still be waiting when the message is published, and the whole broker restart
    # happens inside that wait, so the limit is generous rather than tight: it is a deadline for the
    # restart, not a measurement. The source returns as soon as it has the single row it is asked
    # for, so nothing here is spent waiting when the recovery works.
    select = instance.get_query_request(
        "SELECT key FROM test.consume SETTINGS stream_like_engine_allow_direct_select = 1, rabbitmq_max_wait_ms = 120000",
        timeout=180,
    )
    _wait_for_parked_pull_request()
    _restart_nats(nats_cluster, kill = nats_helpers.hard_kill_nats)

    asyncio.run(publish_messages(nats_cluster, "test_stream", "test_subject", [json.dumps({"key": 42, "value": 42})]))
    assert TSV(select.get_answer()) == TSV("42")


def test_nats_jet_stream_direct_select_resumes_after_broker_graceful_restart(nats_cluster):
    # The recovery inside a running query keys on two things, and a graceful restart is the one
    # that reaches the other: the broker answers the parked pull request on its way down, and the
    # client closes the subscription in response, so the query is holding a subscription that is
    # invalid long before the connection comes back. The hard kill above never produces that
    # state; it leaves handles that look healthy until the reconnect count says otherwise. So
    # this is a separate contract on the direct-`SELECT` path, where the query performs the
    # `unsubscribe` and `subscribe` itself rather than through the retry loop of the background
    # task: a subscription that turns out to be closed while the broker is still unreachable must
    # not fail the query, it must be replaced once the broker is back.
    asyncio.run(add_durable_consumer(cluster, "test_stream", "test_consumer"))

    instance.query(
        """
        CREATE TABLE test.consume (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_stream = 'test_stream',
                     nats_consumer_name = 'test_consumer',
                     nats_subjects = 'test_subject',
                     nats_format = 'JSONEachRow',
                     nats_row_delimiter = '\\n';
        """
    )

    # Anchored before the query starts, so a recovery counts however early in the query it runs.
    anchor = nats_helpers.log_line_count(instance)

    select = instance.get_query_request(
        "SELECT key FROM test.consume SETTINGS stream_like_engine_allow_direct_select = 1, rabbitmq_max_wait_ms = 120000",
        timeout=180,
    )
    _wait_for_parked_pull_request()
    _restart_nats(nats_cluster, kill = nats_helpers.kill_nats)

    asyncio.run(publish_messages(nats_cluster, "test_stream", "test_subject", [json.dumps({"key": 42, "value": 42})]))
    assert TSV(select.get_answer()) == TSV("42")

    # The row alone would also be explained by a recovery the background task performed on a
    # consumer the query had already handed back, so require the query to have done it itself.
    assert nats_helpers.count_in_log_after(instance, IN_SOURCE_RESUBSCRIBE_LOG_LINE, anchor) > 0, (
        "the query did not recover its subscription itself")


def test_nats_jet_stream_streaming_drains_local_backlog_after_in_source_recovery(nats_cluster):
    # When a reconnect lands while a streaming source is inside its flush interval, the source
    # itself recovers the subscription. The consumer must then stay subscribed when that source is
    # destroyed: rows beyond the first output block are already delivered to the local queue, and
    # unsubscribing would make the next cycle drop them locally, stalling the view until the server
    # redelivers them after the ACK deadline.
    #
    # The ACK deadline is set far beyond the waits below, so server redelivery cannot make up for a
    # locally dropped backlog: the final assertion holds only if the recovered consumer stayed
    # subscribed and the following streaming cycles drained the local queue.
    asyncio.run(add_durable_consumer(cluster, "test_stream", "test_consumer", ack_wait_sec = 600))

    # Anchored before the table exists, so the first streaming cycle counts however quickly it
    # starts. A cycle here spans a whole 60 second flush interval, longer than the wait itself, so
    # a first cycle the wait fails to recognize has no successor to fall back on.
    created = nats_helpers.log_line_count(instance)
    instance.query(
        """
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
                     nats_row_delimiter = '\\n',
                     nats_max_block_size = 5,
                     nats_flush_interval_ms = 60000,
                     nats_wait_for_flush_interval = 1;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.consume;
        """
    )
    nats_helpers.wait_for_streaming_started(instance, "test.consume", anchor = created)

    # A streaming cycle spans the whole 60 second flush interval, so a reconnect all but always
    # lands mid-cycle, where the source performs the recovery itself. Only that in-source recovery
    # exercises the code path under test, so wait for its log line; in the rare case the reconnect
    # hits the gap between cycles and the background task recovers the subscription instead, the
    # broker is healthy again and the restart can simply be retried.
    anchor = nats_helpers.log_line_count(instance)
    for _ in range(3):
        _restart_nats(nats_cluster, kill = nats_helpers.hard_kill_nats)

        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if nats_helpers.count_in_log_after(instance, IN_SOURCE_RESUBSCRIBE_LOG_LINE, anchor) > 0:
                break
            time.sleep(0.2)
        else:
            continue
        break
    else:
        raise AssertionError("no streaming source performed an in-source recovery")

    # More rows than one output block: the first block ends the recovering source's cycle, and the
    # rest waits in the local queue for the cycles after it.
    _publish_and_expect("test_subject", range(0, 25), 25)


def _wait_for_ack_pending(expected, consumer_name = "test_consumer", time_limit_sec = 60):
    # Waits until the broker counts exactly `expected` messages as delivered and awaiting an
    # acknowledgement, which is how a message the streaming cycle is holding reads from outside.
    deadline = time.monotonic() + time_limit_sec
    num_ack_pending = 0
    while time.monotonic() < deadline:
        num_ack_pending = asyncio.run(get_num_ack_pending(cluster, "test_stream", consumer_name))
        if num_ack_pending == expected:
            return
        time.sleep(0.2)

    raise AssertionError(
        "consumer {} holds {} unacknowledged messages, expected {}".format(
            consumer_name, num_ack_pending, expected))


# The recovery keys on two things, and the two restarts below reach one each: a hard kill answers
# nothing, so only the reconnect count reports it, while a graceful shutdown answers the parked pull
# request and the client closes the subscription in response. What the recovery does with the
# messages the consumer holds - and with the ones `nats_skip_broken_messages` passed over - has to
# be the same on both, and the closed-subscription case is the one where it acknowledges or hands
# back messages through a subscription the client has already marked closed.
BROKER_RESTARTS = [
    pytest.param(nats_helpers.hard_kill_nats, id = "hard_kill"),
    pytest.param(nats_helpers.kill_nats, id = "graceful_restart"),
]


@pytest.mark.parametrize("kill", BROKER_RESTARTS)
def test_nats_jet_stream_skipped_broken_message_is_not_redelivered_after_reconnect(nats_cluster, kill):
    # A cycle that has emitted no rows is not a cycle that has consumed nothing:
    # `nats_skip_broken_messages` makes a message that yields no rows an ordinary outcome, and
    # `consume` takes the message before it is parsed. Reconnect recovery hands back to the broker
    # everything the consumer has consumed but not acknowledged, so recovering on a zero-row cycle
    # would undo the skip - the malformed message is delivered again and parsed again, and a
    # reconnect in front of the first good row can keep the table reprocessing the same bad input.
    #
    # The broker's own delivery counter is the oracle: the consumer sequence counts every delivery,
    # redeliveries included, so it stays at one per message exactly when nothing was handed back.
    # The ACK deadline is far beyond every wait below, so a redelivery cannot come from anywhere
    # else, and the run holds the flush interval open so the reconnect lands inside a cycle.
    asyncio.run(add_durable_consumer(cluster, "test_stream", "test_consumer", ack_wait_sec = 600))

    # Anchored before the table exists, so the first streaming cycle counts however quickly it
    # starts - the cycles here are longer than the wait itself.
    created = nats_helpers.log_line_count(instance)
    instance.query(
        """
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
                     nats_row_delimiter = '\\n',
                     nats_skip_broken_messages = 1000,
                     nats_flush_interval_ms = 30000,
                     nats_wait_for_flush_interval = 1;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.consume;
        """
    )
    nats_helpers.wait_for_streaming_started(instance, "test.consume", anchor = created)

    # Values the format cannot parse into the column types. The cycle skips the message and keeps
    # waiting for the rest of the flush interval, which is the state under test: an unacknowledged
    # message held by a source that has emitted nothing.
    asyncio.run(publish_messages(
        cluster, "test_stream", "test_subject", [json.dumps({"key": "not a number", "value": "neither"})]))
    _wait_for_ack_pending(1)

    _restart_nats(nats_cluster, kill = kill)

    # The restart lands inside the same cycle, while the skipped message is still held: the wait
    # above read that state off the broker, and the flush interval is far longer than the restart
    # takes. Nothing is asserted about the state afterwards - the recovery acknowledges a skipped
    # message as soon as it runs, so the count seen here depends on how quickly the client
    # reconnects. What the skip is worth is measured by the delivery counter at the end.

    # A stale subscription consumes nothing, so the view holding this row also means the recovery
    # did happen - deferred to a cycle that holds nothing rather than skipped altogether. It waits
    # out the rest of the current cycle and then a whole cycle of its own.
    messages = [json.dumps({"key": 42, "value": 42})]
    asyncio.run(publish_messages(cluster, "test_stream", "test_subject", messages))
    result = instance.query_with_retry(
        "SELECT count() FROM test.view",
        retry_count = 120,
        sleep_time = 1,
        check_callback = lambda num_rows: int(num_rows) == 1)
    assert int(result) == 1, "consumption did not resume, view holds {} rows".format(result)

    consumer_seq = asyncio.run(get_delivered_consumer_seq(cluster, "test_stream", "test_consumer"))
    assert consumer_seq == 2, (
        "the skipped message was handed back to the broker and delivered again: "
        "{} deliveries for two messages".format(consumer_seq))


@pytest.mark.parametrize("kill", BROKER_RESTARTS)
def test_nats_jet_stream_direct_select_resumes_after_skipped_broken_message(nats_cluster, kill):
    # A direct `SELECT` owns its consumer for the whole query, so the query itself has to notice a
    # reconnect and replace the stale pull subscription. A message it passed over because of
    # `nats_skip_broken_messages` must not stand in the way of that: nothing is waiting for it to
    # become a row, so the recovery hands it back to the broker instead of holding the resubscribe
    # up, and the rows published after the reconnect reach this query rather than leaving it to sit
    # on a stale subscription until `rabbitmq_max_wait_ms` runs out. With `nats_commit_on_select`
    # the redelivery is skipped again and acknowledged where the query commits what it read, so the
    # skip costs one extra delivery and nothing is left outstanding.
    #
    # The ACK deadline is far beyond every wait below, so a redelivery can only come from the
    # recovery handing the message back, which makes the broker's delivery counter the oracle.
    asyncio.run(add_durable_consumer(cluster, "test_stream", "test_consumer", ack_wait_sec = 600))

    instance.query(
        """
        CREATE TABLE test.consume (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_stream = 'test_stream',
                     nats_consumer_name = 'test_consumer',
                     nats_subjects = 'test_subject',
                     nats_format = 'JSONEachRow',
                     nats_row_delimiter = '\\n',
                     nats_skip_broken_messages = 1000,
                     nats_commit_on_select = 1;
        """
    )

    # Values the format cannot parse into the column types. The query consumes this message, emits
    # nothing for it and keeps waiting, which is the state under test: an unacknowledged message
    # held by a query that has produced no rows.
    asyncio.run(publish_messages(
        cluster, "test_stream", "test_subject", [json.dumps({"key": "not a number", "value": "neither"})]))

    # The whole broker restart happens inside this wait, so the limit is a deadline for the restart
    # rather than a measurement; a query that recovers its subscription returns as soon as it has
    # the row published below.
    select = instance.get_query_request(
        "SELECT key FROM test.consume SETTINGS stream_like_engine_allow_direct_select = 1, rabbitmq_max_wait_ms = 120000",
        timeout = 180,
    )
    _wait_for_ack_pending(1)

    _restart_nats(nats_cluster, kill = kill)

    asyncio.run(publish_messages(cluster, "test_stream", "test_subject", [json.dumps({"key": 42, "value": 42})]))
    assert TSV(select.get_answer()) == TSV("42")

    # Three deliveries for two messages: the recovery handed the skipped message back instead of
    # committing it on behalf of a query that had returned nothing yet, and the redelivery was
    # skipped again straight away. Two would mean it was consumed before the query committed
    # anything, which is what a cancelled query must not leave behind.
    deadline = time.monotonic() + 60
    consumer_seq = 0
    while time.monotonic() < deadline:
        consumer_seq = asyncio.run(get_delivered_consumer_seq(cluster, "test_stream", "test_consumer"))
        if consumer_seq >= 3:
            break
        time.sleep(0.2)

    assert consumer_seq >= 3, (
        "the recovery acknowledged the skipped message before the query committed anything: "
        "{} deliveries for two messages".format(consumer_seq))

    # The query did commit, so the row it returned is consumed by the time it returns, and so is the
    # skipped message where its redelivery reached the query before that row did. It does not have
    # to: a direct `SELECT` returns as soon as it has a row, and a graceful shutdown can lose the
    # redelivery on the wire (the broker counts it as delivered, the client never sees it), leaving
    # it outstanding until the ACK deadline with nothing left to pull it. What must hold is that the
    # only thing the broker may still count as outstanding is that skipped message, never the row.
    _wait_for_nothing_but_the_first_message_outstanding()


def _wait_for_nothing_but_the_first_message_outstanding(consumer_name = "test_consumer", time_limit_sec = 60):
    # With two messages in the stream, an acknowledgement floor of zero together with a single
    # outstanding message means the first one is outstanding and the second acknowledged.
    deadline = time.monotonic() + time_limit_sec
    info = None
    while time.monotonic() < deadline:
        info = asyncio.run(get_consumer_info(cluster, "test_stream", consumer_name))
        if info.num_ack_pending == 0:
            return
        if info.num_ack_pending == 1 and info.ack_floor.stream_seq == 0:
            return
        time.sleep(0.2)

    raise AssertionError(
        "consumer {} holds {} unacknowledged messages with acknowledgement floor {}".format(
            consumer_name, info.num_ack_pending, info.ack_floor.stream_seq))


def test_nats_jet_stream_direct_select_does_not_commit_a_skipped_message_when_cancelled(nats_cluster):
    # `nats_commit_on_select` commits only what the read returns, and reconnect recovery runs long
    # before that commit point. A query that skipped a malformed message, replaced its subscription
    # after a reconnect and was then cancelled without returning a single row must leave that
    # message to the next reader, exactly like the messages it had read but not returned.
    #
    # The ACK deadline is far beyond every wait below, so a redelivery can only come from the
    # recovery handing the message back, and nothing but this query can acknowledge one.
    asyncio.run(add_durable_consumer(cluster, "test_stream", "test_consumer", ack_wait_sec = 600))

    instance.query(
        """
        CREATE TABLE test.consume (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_stream = 'test_stream',
                     nats_consumer_name = 'test_consumer',
                     nats_subjects = 'test_subject',
                     nats_format = 'JSONEachRow',
                     nats_row_delimiter = '\\n',
                     nats_skip_broken_messages = 1000,
                     nats_commit_on_select = 1;
        """
    )

    asyncio.run(publish_messages(
        cluster, "test_stream", "test_subject", [json.dumps({"key": "not a number", "value": "neither"})]))

    # Nothing is published for this query to return, so it waits out the limit unless it is
    # cancelled first, which is what happens below.
    select = instance.get_query_request(
        "SELECT key FROM test.consume SETTINGS stream_like_engine_allow_direct_select = 1, rabbitmq_max_wait_ms = 120000",
        timeout = 180,
    )
    _wait_for_ack_pending(1)

    _restart_nats(nats_cluster, kill = nats_helpers.hard_kill_nats)

    # A pull request parked after the restart can only come from this query: it holds the consumer
    # for as long as it runs, so the recovery has happened by the time the broker counts one, and
    # whatever the recovery did with the skipped message it has done by then.
    _wait_for_parked_pull_request()

    instance.query("SYSTEM CANCEL test.consume")
    assert TSV(select.get_answer()) == TSV("")

    # The cancelled read returned nothing, so it committed nothing: the malformed message went back
    # to the broker at the resubscribe, was delivered a second time and is waiting to be read again.
    # Acknowledging it in the recovery would leave one delivery and nothing pending, putting a
    # message out of reach of every later query without a single row having been returned for it.
    deadline = time.monotonic() + 60
    consumer_seq = 0
    while time.monotonic() < deadline:
        consumer_seq = asyncio.run(get_delivered_consumer_seq(cluster, "test_stream", "test_consumer"))
        if consumer_seq >= 2:
            break
        time.sleep(0.2)

    assert consumer_seq >= 2, (
        "the cancelled query consumed the message it skipped: "
        "{} deliveries for one message".format(consumer_seq))

    _wait_for_ack_pending(1)


def test_nats_jet_stream_direct_select_does_not_consume_skipped_broken_message(nats_cluster):
    # The same reconnect recovery with `nats_commit_on_select` left at its default `0`, where a
    # direct read must consume nothing at all: the query never commits, so the message it passed
    # over because of `nats_skip_broken_messages` has to go back to the broker with everything else
    # the recovery hands back, and the next query gets to see it again. Acknowledging it there would
    # consume a message on behalf of an uncommitted read and put it out of reach for good.
    #
    # The ACK deadline is far beyond every wait below, so a redelivery can only come from the
    # recovery handing the message back, which makes the broker's delivery counter the oracle.
    asyncio.run(add_durable_consumer(cluster, "test_stream", "test_consumer", ack_wait_sec = 600))

    instance.query(
        """
        CREATE TABLE test.consume (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_stream = 'test_stream',
                     nats_consumer_name = 'test_consumer',
                     nats_subjects = 'test_subject',
                     nats_format = 'JSONEachRow',
                     nats_row_delimiter = '\\n',
                     nats_skip_broken_messages = 1000;
        """
    )

    asyncio.run(publish_messages(
        cluster, "test_stream", "test_subject", [json.dumps({"key": "not a number", "value": "neither"})]))

    select = instance.get_query_request(
        "SELECT key FROM test.consume SETTINGS stream_like_engine_allow_direct_select = 1, rabbitmq_max_wait_ms = 120000",
        timeout = 180,
    )
    _wait_for_ack_pending(1)

    _restart_nats(nats_cluster, kill = nats_helpers.hard_kill_nats)

    asyncio.run(publish_messages(cluster, "test_stream", "test_subject", [json.dumps({"key": 42, "value": 42})]))
    assert TSV(select.get_answer()) == TSV("42")

    # Three deliveries for two messages: the skipped one was handed back and delivered again. Two
    # would mean the uncommitted query acknowledged it.
    deadline = time.monotonic() + 60
    consumer_seq = 0
    while time.monotonic() < deadline:
        consumer_seq = asyncio.run(get_delivered_consumer_seq(cluster, "test_stream", "test_consumer"))
        if consumer_seq >= 3:
            break
        time.sleep(0.2)

    assert consumer_seq >= 3, (
        "an uncommitted direct SELECT acknowledged the message it skipped: "
        "{} deliveries for two messages".format(consumer_seq))


def test_nats_jet_stream_keeps_buffered_backlog_across_broker_restart(nats_cluster):
    # Reconnect recovery re-subscribes the consumer, and the local queue of messages the broker had
    # already delivered used to go with the stale subscription. Those rows are in this server's
    # hands: dropping them leaves the view short until JetStream redelivers them, a whole ACK
    # deadline later. The deadline here is far beyond every wait below, so redelivery cannot cover
    # for a dropped backlog - the final count holds only if the buffered messages survived the
    # recovery.
    total_expected = 3000
    asyncio.run(add_durable_consumer(cluster, "test_stream", "test_consumer", ack_wait_sec = 600))

    # Published before the table exists, so the whole backlog is waiting when it subscribes: the
    # client pulls it into the local queue far faster than the streaming cycles insert it, which is
    # what leaves a large buffered backlog for the restart below to land on.
    messages = [json.dumps({"key": key, "value": key}) for key in range(total_expected)]
    asyncio.run(publish_messages(cluster, "test_stream", "test_subject", messages))

    instance.query(
        """
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
                     nats_row_delimiter = '\\n',
                     nats_max_block_size = 5;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.consume;
        """
    )

    # Blocks of five rows make every streaming cycle insert a handful at a time, so the view
    # crossing this mark means consuming is well under way while most of the backlog is still
    # buffered locally.
    consumed = instance.query_with_retry(
        "SELECT count() FROM test.view",
        retry_count = 600,
        sleep_time = 0.1,
        check_callback = lambda num_rows: int(num_rows) >= 50)
    assert int(consumed) >= 50, "streaming did not start, the view holds {} rows".format(consumed)
    assert int(consumed) < total_expected, "the whole backlog was consumed before the restart"

    nats_helpers.hard_kill_nats(nats_cluster)
    time.sleep(4)
    nats_helpers.revive_nats(nats_cluster)

    result = instance.query_with_retry(
        "SELECT count(DISTINCT key) FROM test.view",
        retry_count = 120,
        sleep_time = 1,
        check_callback = lambda num_rows: int(num_rows) == total_expected)
    assert int(result) == total_expected, "the buffered backlog was dropped, view holds {} of {} keys".format(
        result, total_expected)


UNSUBSCRIBED_LOG_LINE = "Consumer .* unsubscribed"


def test_nats_jet_stream_returns_buffered_backlog_to_the_broker_when_unsubscribing(nats_cluster):
    # A table that keeps consuming can have its subscription replaced under it - by reconnect
    # recovery, by a `SYSTEM STOP`, or by the last materialized view going away - and has to part
    # with the messages the broker had already delivered into the local queue, because a `natsMsg`
    # cannot outlive the subscription it arrived on. Those are not lost rows from the broker's
    # point of view: it counts them as delivered and awaiting an acknowledgement, so destroying
    # them locally makes them unreachable until the ACK deadline. Handing them back with
    # `natsMsg_Nak` while the subscription is still alive is what keeps them reachable, and it is
    # what closes the window where a message arrives in the moment between a recovery deciding the
    # queue is empty and that queue being finished.
    #
    # Dropping the last view reaches that state deterministically: nothing pops the local queue
    # from the moment the view is gone, so it is certainly holding a large backlog when the
    # consumer is unsubscribed. The ACK deadline is far beyond every wait below, so server-side
    # redelivery cannot make up for a backlog that was destroyed instead of returned.
    total_expected = 3000
    asyncio.run(add_durable_consumer(cluster, "test_stream", "test_consumer", ack_wait_sec = 600))

    # Published before the table exists, so the whole backlog is waiting when it subscribes: the
    # client pulls it into the local queue far faster than the streaming cycles insert it.
    messages = [json.dumps({"key": key, "value": key}) for key in range(total_expected)]
    asyncio.run(publish_messages(cluster, "test_stream", "test_subject", messages))

    instance.query(
        """
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
                     nats_row_delimiter = '\\n',
                     nats_max_block_size = 5;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.consume;
        """
    )

    # Blocks of five rows make every streaming cycle insert a handful at a time, so the view
    # crossing this mark means consuming is well under way while most of the backlog is still
    # buffered locally.
    consumed = instance.query_with_retry(
        "SELECT count() FROM test.view",
        retry_count = 600,
        sleep_time = 0.1,
        check_callback = lambda num_rows: int(num_rows) >= 50)
    assert int(consumed) >= 50, "streaming did not start, the view holds {} rows".format(consumed)
    assert int(consumed) < total_expected, "the whole backlog was consumed before the view was dropped"

    anchor = nats_helpers.log_line_count(instance)
    assert anchor > 0, "log offset anchor is not a line number: {}".format(anchor)

    instance.query("DROP VIEW test.consumer")

    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if nats_helpers.count_in_log_after(instance, UNSUBSCRIBED_LOG_LINE, anchor) > 0:
            break
        time.sleep(0.2)
    else:
        raise AssertionError("the consumer stayed subscribed after the last view was dropped")

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.consume;
        """
    )

    result = instance.query_with_retry(
        "SELECT count(DISTINCT key) FROM test.view",
        retry_count = 120,
        sleep_time = 1,
        check_callback = lambda num_rows: int(num_rows) == total_expected)
    assert int(result) == total_expected, (
        "the buffered backlog was destroyed instead of returned to the broker, view holds {} of {} keys".format(
            result, total_expected))


def test_nats_jet_stream_does_not_acknowledge_messages_discarded_by_a_view_dropped_mid_cycle(nats_cluster):
    # A streaming cycle checks that views are attached and then has its insert pipeline built, which
    # looks them up again. A `DROP VIEW` landing between the two leaves the pipeline with nowhere to
    # insert into: it discards what the sources consume, and the cycle used to acknowledge those
    # messages all the same, so the broker counted as consumed a block of rows that reached no
    # table. The gap is a millisecond wide, so a failpoint holds a cycle open exactly there.
    #
    # The ACK deadline is far beyond every wait below, so the rows can only reach the view attached
    # afterwards if the cycle handed them back rather than acknowledging them.
    total_expected = 10
    asyncio.run(add_durable_consumer(cluster, "test_stream", "test_consumer", ack_wait_sec = 600))

    instance.query(
        """
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
                     nats_row_delimiter = '\\n';
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.consume;
        """
    )
    nats_helpers.wait_for_streaming_started(instance, "test.consume")

    # Streaming is under way, so the next cycle stops right before it builds the insert pipeline,
    # having already found the view attached.
    instance.query("SYSTEM ENABLE FAILPOINT nats_pause_before_building_insert_pipeline")
    instance.query("SYSTEM WAIT FAILPOINT nats_pause_before_building_insert_pipeline PAUSE")

    # Delivered into the consumer's local queue while the cycle is held, so the cycle has something
    # to consume once it goes on. The broker counts them as delivered and awaiting acknowledgement.
    messages = [json.dumps({"key": key, "value": key}) for key in range(total_expected)]
    asyncio.run(publish_messages(cluster, "test_stream", "test_subject", messages))
    _wait_for_ack_pending(total_expected)

    anchor = nats_helpers.log_line_count(instance)
    assert anchor > 0, "log offset anchor is not a line number: {}".format(anchor)

    instance.query("DROP VIEW test.consumer")
    instance.query("SYSTEM DISABLE FAILPOINT nats_pause_before_building_insert_pipeline")

    # The released cycle finds no view to stream to, and the task unsubscribes its consumers once it
    # notices the last view is gone, handing back whatever they hold. Waiting for that makes the
    # count below a statement about what the released cycle did with the messages, not about a
    # view recreated before it ran.
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if nats_helpers.count_in_log_after(instance, UNSUBSCRIBED_LOG_LINE, anchor) > 0:
            break
        time.sleep(0.2)
    else:
        raise AssertionError("the consumer stayed subscribed after the last view was dropped")

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.consume;
        """
    )

    result = instance.query_with_retry(
        "SELECT count(DISTINCT key) FROM test.view",
        retry_count = 60,
        sleep_time = 1,
        check_callback = lambda num_rows: int(num_rows) == total_expected)
    assert int(result) == total_expected, (
        "the cycle that had no view to stream to acknowledged the messages it discarded, "
        "view holds {} of {} keys".format(result, total_expected))


NOTHING_TO_STREAM_TO_LOG_LINE = "nothing to stream to"


def test_nats_jet_stream_does_not_acknowledge_messages_discarded_by_a_view_detached_mid_cycle(nats_cluster):
    # The detach twin of the test above. Unlike `DROP VIEW`, a plain `DETACH TABLE` keeps the view
    # registered as a dependency of the `NATS` table, so a cycle that decides whether it has anywhere
    # to stream to by the dependency metadata alone would go on to consume into the discarding
    # pipeline and acknowledge. The view stays a dependency, so the consumer stays subscribed and
    # keeps the held messages locally: they can only reach the re-attached view if the held cycle
    # left them alone.
    total_expected = 10
    asyncio.run(add_durable_consumer(cluster, "test_stream", "test_consumer", ack_wait_sec = 600))

    instance.query(
        """
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
                     nats_row_delimiter = '\\n';
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.consume;
        """
    )
    nats_helpers.wait_for_streaming_started(instance, "test.consume")

    instance.query("SYSTEM ENABLE FAILPOINT nats_pause_before_building_insert_pipeline")
    instance.query("SYSTEM WAIT FAILPOINT nats_pause_before_building_insert_pipeline PAUSE")

    messages = [json.dumps({"key": key, "value": key}) for key in range(total_expected)]
    asyncio.run(publish_messages(cluster, "test_stream", "test_subject", messages))
    _wait_for_ack_pending(total_expected)

    anchor = nats_helpers.log_line_count(instance)
    assert anchor > 0, "log offset anchor is not a line number: {}".format(anchor)

    instance.query("DETACH TABLE test.consumer")
    instance.query("SYSTEM DISABLE FAILPOINT nats_pause_before_building_insert_pipeline")

    # The released cycle must notice that the view is gone. Waiting for that makes the count below
    # a statement about what the released cycle did with the messages, not about a view attached
    # again before it ran.
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if nats_helpers.count_in_log_after(instance, NOTHING_TO_STREAM_TO_LOG_LINE, anchor) > 0:
            break
        time.sleep(0.2)
    else:
        raise AssertionError("the released cycle did not report that the view was gone")

    instance.query("ATTACH TABLE test.consumer")

    result = instance.query_with_retry(
        "SELECT count(DISTINCT key) FROM test.view",
        retry_count = 60,
        sleep_time = 1,
        check_callback = lambda num_rows: int(num_rows) == total_expected)
    assert int(result) == total_expected, (
        "the cycle that had no view to stream to acknowledged the messages it discarded, "
        "view holds {} of {} keys".format(result, total_expected))


def _wait_for_ack_floor(expected, consumer_name = "test_consumer", time_limit_sec = 60):
    # Waits until the broker has every message up to stream sequence `expected` acknowledged, which
    # is how a stream consumed and committed in full reads from outside.
    deadline = time.monotonic() + time_limit_sec
    ack_floor = 0
    while time.monotonic() < deadline:
        info = asyncio.run(get_consumer_info(cluster, "test_stream", consumer_name))
        ack_floor = info.ack_floor.stream_seq
        if ack_floor == expected:
            return
        time.sleep(0.2)

    raise AssertionError(
        "consumer {} has acknowledged messages up to stream sequence {}, expected {}".format(
            consumer_name, ack_floor, expected))


def test_nats_jet_stream_streams_to_a_materialized_view_with_a_null_target(nats_cluster):
    # A materialized view whose target is `Null` is a legitimate way to consume a stream for its
    # side effects (or to drop it on the floor on purpose): the insert pipeline it produces ends in
    # the same discarding sink as the pipeline of a table whose last view was dropped mid-cycle. The
    # cycle must tell the two apart by the dependency metadata and keep streaming - and, with
    # JetStream, acknowledging - rather than treating the view as absent and never consuming.
    total_expected = 10
    asyncio.run(add_durable_consumer(cluster, "test_stream", "test_consumer", ack_wait_sec = 600))

    instance.query(
        """
        CREATE TABLE test.consume (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_stream = 'test_stream',
                     nats_consumer_name = 'test_consumer',
                     nats_subjects = 'test_subject',
                     nats_format = 'JSONEachRow',
                     nats_row_delimiter = '\\n';
        CREATE MATERIALIZED VIEW test.consumer ENGINE = Null AS
            SELECT * FROM test.consume;
        """
    )
    nats_helpers.wait_for_streaming_started(instance, "test.consume")

    messages = [json.dumps({"key": key, "value": key}) for key in range(total_expected)]
    asyncio.run(publish_messages(cluster, "test_stream", "test_subject", messages))

    # Consumed and committed: the broker has every message acknowledged. A table that mistook the
    # `Null` target for a dropped view would never build a source, and the floor would stay at zero.
    _wait_for_ack_floor(total_expected)


def test_nats_jet_stream_streams_to_a_fan_out_with_a_null_target(nats_cluster):
    # The same with a view that has somewhere to insert into next to the `Null` one: every row
    # reaches the real target, and the discarding sink of the other branch does not stop the cycle.
    total_expected = 10
    asyncio.run(add_durable_consumer(cluster, "test_stream", "test_consumer", ack_wait_sec = 600))

    instance.query(
        """
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
                     nats_row_delimiter = '\\n';
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.consume;
        CREATE MATERIALIZED VIEW test.discarding_consumer ENGINE = Null AS
            SELECT * FROM test.consume;
        """
    )
    nats_helpers.wait_for_streaming_started(instance, "test.consume")

    messages = [json.dumps({"key": key, "value": key}) for key in range(total_expected)]
    asyncio.run(publish_messages(cluster, "test_stream", "test_subject", messages))

    result = instance.query_with_retry(
        "SELECT count(DISTINCT key) FROM test.view",
        retry_count = 60,
        sleep_time = 1,
        check_callback = lambda num_rows: int(num_rows) == total_expected)
    assert int(result) == total_expected, "view holds {} of {} keys".format(result, total_expected)

    _wait_for_ack_floor(total_expected)


def test_nats_jet_stream_hands_back_the_backlog_of_a_consumer_a_direct_select_left_subscribed(nats_cluster):
    # Once the last materialized view is gone the streaming task unsubscribes the consumers, but it
    # only reaches the ones in the pool at that moment. A direct `SELECT` issued right after the
    # `DROP VIEW` takes the consumer out of the pool still subscribed and hands it back that way,
    # and nothing unsubscribes it afterwards: everything published while no view is attached lands
    # in its local queue. Attaching a view again must part with that backlog the way dropping a
    # view does - by handing it back to the broker while the subscription it arrived on is still
    # alive - rather than by clearing the queue under a live subscription, which the client can
    # keep appending to and which the broker counts as delivered until the ACK deadline. The
    # deadline here is far beyond every wait below, so the final count holds only if the backlog
    # was returned.
    asyncio.run(add_durable_consumer(cluster, "test_stream", "test_consumer", ack_wait_sec = 600))

    # A short flush interval keeps the streaming cycles short, so the `SELECT` below gets the
    # consumer as soon as the cycle that is holding it ends.
    instance.query(
        """
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
                     nats_row_delimiter = '\\n',
                     nats_flush_interval_ms = 1000;
        """
    )
    nats_helpers.wait_for_table_is_ready(instance, "test.consume")

    create_view = """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.consume;
        """
    instance.query(create_view)
    nats_helpers.wait_for_mv_attached_to_table(instance, "test.consume")
    _publish_and_expect("test_subject", range(20), 20)

    # The streaming task notices the dropped view on its next run, up to half a second after the
    # `DROP VIEW`, and unsubscribes whatever consumer is in the pool at that moment. The `SELECT`
    # has to take the consumer out of the pool before that run: both statements go over a single
    # client connection, so the query is planned a few milliseconds after the drop completes, not
    # after a second round trip into the container, which is slower than the task on a sanitizer
    # build. The query then holds the consumer well past that run, and the task, which is not
    # rescheduled once it finds no view, never reaches it again. Whether the query really found
    # the consumer subscribed shows in the log: a consumer the query had to subscribe itself is
    # unsubscribed again when the query ends, and one it found subscribed is not.
    for _ in range(5):
        anchor = nats_helpers.log_line_count(instance)
        instance.query(
            """
            DROP VIEW test.consumer;
            SELECT * FROM test.consume SETTINGS rabbitmq_max_wait_ms = 5000;
            """
        )
        time.sleep(1)
        if nats_helpers.count_in_log_after(instance, UNSUBSCRIBED_LOG_LINE, anchor) == 0:
            break
        instance.query(create_view)
        nats_helpers.wait_for_mv_attached_to_table(instance, "test.consume")
    else:
        raise AssertionError("no direct SELECT got hold of a consumer that was still subscribed")

    # The subscription the query left behind keeps a pull request parked, so what is published now
    # is delivered into the local queue of a table that is not streaming. The broker counting all
    # of it as awaiting an acknowledgement is what proves it got there.
    _wait_for_parked_pull_request()
    detached = 100
    messages = [json.dumps({"key": key, "value": key}) for key in range(100, 100 + detached)]
    asyncio.run(publish_messages(cluster, "test_stream", "test_subject", messages))
    _wait_for_ack_pending(detached)

    instance.query(create_view)
    nats_helpers.wait_for_mv_attached_to_table(instance, "test.consume")

    # Both what was buffered while detached and what arrives afterwards have to reach the view: a
    # backlog cleared locally instead of returned stays unreachable for the whole ACK deadline.
    _publish_and_expect("test_subject", range(20, 40), 20 + detached + 20)


def test_nats_jet_stream_resumes_consuming_after_two_broker_restarts(nats_cluster):
    # A one-shot recovery would pass the single-restart test above, so require it to work twice.
    _setup_restart_table("test_subject", "test_consumer")

    total_expected = 10
    _publish_and_expect("test_subject", range(0, 10), total_expected)

    for round_index in range(2):
        first_key = 100 + round_index * 100
        _restart_nats(nats_cluster)

        total_expected += 10
        _publish_and_expect("test_subject", range(first_key, first_key + 10), total_expected)


def test_nats_jet_stream_does_not_resubscribe_while_healthy(nats_cluster):
    # The recovery keys on a subscription the client has closed, so a healthy consumer must never
    # trigger it: an ordinary reconnect does not close a subscription, and firing per streaming
    # cycle would tear down and rebuild the subscription every few hundred milliseconds.
    _setup_restart_table("test_subject", "test_consumer")

    # The count is anchored to an absolute log offset, so zero means zero: no such line was written
    # after this point, however much else the instance logged meanwhile.
    anchor = nats_helpers.log_line_count(instance)
    assert anchor > 0, "log offset anchor is not a line number: {}".format(anchor)

    for round_index in range(3):
        first_key = round_index * 10
        _publish_and_expect("test_subject", range(first_key, first_key + 10), first_key + 10)

    time.sleep(5)

    assert nats_helpers.log_line_count(instance) >= anchor, "the log rotated, the offset is stale"
    resubscribes = nats_helpers.count_in_log_after(instance, RESUBSCRIBE_LOG_LINE, anchor)
    assert resubscribes == 0, "resubscribed {} times without a broker restart".format(resubscribes)


def test_nats_jet_stream_settles_after_one_broker_restart(nats_cluster):
    # After recovering, the table must stop resubscribing. A detector that keeps reporting a dead
    # subscription would fire on every streaming cycle instead, so bound the count over a quiet
    # period rather than only checking that consumption resumed.
    _setup_restart_table("test_subject", "test_consumer")

    total_expected = 10
    _publish_and_expect("test_subject", range(0, 10), total_expected)

    recovery_anchor = nats_helpers.log_line_count(instance)
    _restart_nats(nats_cluster)

    total_expected += 10
    _publish_and_expect("test_subject", range(100, 110), total_expected)

    # Positive control for the zero-count assertions below and in the healthy-consumer test: the
    # restart above must have recovered through this exact line, so if the production message ever
    # changes those assertions start failing here instead of silently passing on a literal nothing
    # emits any more.
    assert nats_helpers.count_in_log_after(instance, RESUBSCRIBE_LOG_LINE, recovery_anchor) > 0, (
        "consuming resumed without recovering through {!r}, so a zero count proves nothing".format(
            RESUBSCRIBE_LOG_LINE))

    # A closure queued while the broker was still coming up can arrive shortly after consuming
    # resumed, so let that pass before measuring. What is asserted is that recovery then STAYS
    # quiet: a detector that keeps reporting a dead subscription fires on every streaming cycle and
    # so keeps adding lines here.
    time.sleep(5)

    # The count is anchored to an absolute log offset, so zero means zero: no such line was written
    # after this point, however much else the instance logged meanwhile.
    anchor = nats_helpers.log_line_count(instance)
    assert anchor > 0, "log offset anchor is not a line number: {}".format(anchor)

    time.sleep(10)

    # The streaming task reschedules about twice a second, so a detector stuck reporting a dead
    # subscription would add tens of lines over this window.
    assert nats_helpers.log_line_count(instance) >= anchor, "the log rotated, the offset is stale"
    resubscribes = nats_helpers.count_in_log_after(instance, RESUBSCRIBE_LOG_LINE, anchor)
    assert resubscribes == 0, "kept resubscribing after recovery: {} more lines".format(resubscribes)


def test_nats_jet_stream_resumes_consuming_multiple_subjects_after_broker_restart(nats_cluster):
    # One pull subscription per subject, and both have to resume: recovery keys on any of them
    # being closed and then re-subscribes the consumer as a whole.
    _setup_restart_table("test_subject,right_insert1", "test_consumer")

    total_expected = 10
    _publish_and_expect("test_subject", range(0, 10), total_expected)

    _restart_nats(nats_cluster)

    # Both subjects are asserted separately, so recovering only the one that reported closed would
    # leave the other silent and fail here.
    total_expected += 10
    _publish_and_expect("test_subject", range(100, 110), total_expected)

    total_expected += 10
    _publish_and_expect("right_insert1", range(200, 210), total_expected)


def test_nats_no_connection_at_startup_1(nats_cluster):

    asyncio.run(add_durable_consumer(cluster, "test_stream", "test_consumer"))

    assert(
        "Cannot connect to Nats"
        in
        instance.query_and_get_error(
            """
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
        """
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
        """
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
        """
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
            """
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
        """
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
        """
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
        """
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
        """
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
        """
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
