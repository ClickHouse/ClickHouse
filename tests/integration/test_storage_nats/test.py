import asyncio
import json
import logging
import math
import os.path as p
import random
import subprocess
import threading
import time
from random import randrange

import pytest
from google.protobuf.internal.encoder import _VarintBytes

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster, check_nats_is_available, nats_connect_ssl
from helpers.test_tools import TSV

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


def wait_nats_to_start(nats_port, ssl_ctx=None, timeout=180):
    start = time.time()
    while time.time() - start < timeout:
        try:
            if asyncio.run(check_nats_is_available(nats_port, ssl_ctx=ssl_ctx)):
                logging.debug("NATS is available")
                return
            time.sleep(0.5)
        except Exception as ex:
            logging.debug("Can't connect to NATS " + str(ex))
            time.sleep(0.5)

def nats_check_query_result(query, time_limit_sec = 60):
    query_result = ""
    deadline = time.monotonic() + time_limit_sec

    while time.monotonic() < deadline:
        query_result = instance.query(query, ignore_error=True        )
        if nats_check_result(query_result):
            break

    nats_check_result(query_result, True)

def nats_check_result(query_result, check=False, ref_file="test_nats_json.reference"):
    fpath = p.join(p.dirname(__file__), ref_file)
    with open(fpath) as reference:
        if check:
            assert TSV(query_result) == TSV(reference)
        else:
            return TSV(query_result) == TSV(reference)


def kill_nats(nats_id):
    p = subprocess.Popen(("docker", "stop", nats_id), stdout=subprocess.PIPE)
    p.communicate()
    return p.returncode == 0


def revive_nats(nats_id, nats_port):
    p = subprocess.Popen(("docker", "start", nats_id), stdout=subprocess.PIPE)
    p.communicate()
    wait_nats_to_start(nats_port)


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

async def nats_produce_messages(cluster_inst, subject, messages=(), bytes=None):
    nc = await nats_connect_ssl(
        cluster_inst.nats_port,
        user="click",
        password="house",
        ssl_ctx=cluster_inst.nats_ssl_context,
    )
    logging.debug("NATS connection status: " + str(nc.is_connected))

    for message in messages:
        await nc.publish(subject, message.encode())
    if bytes is not None:
        await nc.publish(subject, bytes)
    await nc.flush()
    logging.debug("Finished publishing to " + subject)

    await nc.close()
    return messages

def wait_query_result(instance, query, wait_query_result, sleep_timeout = 0.5, time_limit_sec = 60):
    deadline = time.monotonic() + time_limit_sec
    
    query_result = 0
    while time.monotonic() < deadline:
        query_result = int(instance.query(query))
        if query_result == wait_query_result:
            break
        
        time.sleep(1)
    
    assert query_result == wait_query_result


def wait_for_table_is_ready(instance, table_name, sleep_timeout = 0.5, time_limit_sec = 60):
    deadline = time.monotonic() + time_limit_sec
    while (not check_table_is_ready(instance, table_name)) and time.monotonic() < deadline:
        time.sleep(sleep_timeout)

    assert(check_table_is_ready(instance, table_name))

# waiting for subscription to nats subjects (after subscription direct selection is not available and completed with an error)
def wait_for_mv_attached_to_table(instance, table_name, sleep_timeout = 0.5, time_limit_sec = 60):
    deadline = time.monotonic() + time_limit_sec
    while check_table_is_ready(instance, table_name) and time.monotonic() < deadline:
        time.sleep(sleep_timeout)
    
    assert(not check_table_is_ready(instance, table_name))

def check_table_is_ready(instance, table_name):
    try:
        instance.query("SELECT * FROM {}".format(table_name))
        return True
    except Exception:
        return False


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
    wait_for_table_is_ready(instance, "test.nats")

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.nats;
        """
    )
    wait_for_mv_attached_to_table(instance, "test.nats")

    messages = []
    for i in range(50):
        messages.append(json.dumps({"key": i, "value": i}))
    asyncio.run(nats_produce_messages(nats_cluster, "select", messages))

    nats_check_query_result("SELECT * FROM test.view ORDER BY key")


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
    wait_for_table_is_ready(instance, "test.nats")

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.nats;
        """
    )
    wait_for_mv_attached_to_table(instance, "test.nats")
    
    messages = ""
    for i in range(25):
        messages += json.dumps({"key": i, "value": i}) + "\n"

    all_messages = [messages]
    asyncio.run(nats_produce_messages(nats_cluster, "json", all_messages))

    messages = ""
    for i in range(25, 50):
        messages += json.dumps({"key": i, "value": i}) + "\n"
    all_messages = [messages]
    asyncio.run(nats_produce_messages(nats_cluster, "json", all_messages))

    nats_check_query_result("SELECT * FROM test.view ORDER BY key")


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
    wait_for_table_is_ready(instance, "test.nats")

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.nats;
        """
    )
    wait_for_mv_attached_to_table(instance, "test.nats")

    messages = []
    for i in range(50):
        messages.append("{i}, {i}".format(i=i))

    asyncio.run(nats_produce_messages(nats_cluster, "csv", messages))

    time.sleep(1)

    result = ""
    time_limit_sec = 60
    deadline = time.monotonic() + time_limit_sec

    while time.monotonic() < deadline:
        result = instance.query(
            "SELECT * FROM test.view ORDER BY key", ignore_error=True
        )
        if nats_check_result(result):
            break

    nats_check_result(result, True)


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
    wait_for_table_is_ready(instance, "test.nats")

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.nats;
        """
    )
    wait_for_mv_attached_to_table(instance, "test.nats")

    messages = []
    for i in range(50):
        messages.append("{i}\t{i}".format(i=i))

    asyncio.run(nats_produce_messages(nats_cluster, "tsv", messages))

    nats_check_query_result("SELECT * FROM test.view ORDER BY key")

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
    wait_for_table_is_ready(instance, "test.nats")

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.nats;
        """
    )
    wait_for_mv_attached_to_table(instance, "test.nats")
    
    message = ""
    for i in range(50):
        message += json.dumps({"key": i, "value": i}) + "\n"
    asyncio.run(nats_produce_messages(nats_cluster, "macro", [message]))

    nats_check_query_result("SELECT * FROM test.view ORDER BY key")


def test_nats_materialized_view(nats_cluster):
    instance.query(
        """
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = 'mv',
                     nats_format = 'JSONEachRow',
                     nats_row_delimiter = '\\n';
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE TABLE test.view2 (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        """
    )
    wait_for_table_is_ready(instance, "test.nats")

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.nats;
        CREATE MATERIALIZED VIEW test.consumer2 TO test.view2 AS
            SELECT * FROM test.nats group by (key, value);
        """
    )
    wait_for_mv_attached_to_table(instance, "test.nats")
    
    messages = []
    for i in range(50):
        messages.append(json.dumps({"key": i, "value": i}))

    asyncio.run(nats_produce_messages(nats_cluster, "mv", messages))

    nats_check_result("SELECT * FROM test.view ORDER BY key")
    nats_check_result("SELECT * FROM test.view2 ORDER BY key")


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
    wait_for_table_is_ready(instance, "test.nats")

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM (SELECT * FROM test.nats);
        """
    )
    wait_for_mv_attached_to_table(instance, "test.nats")
    
    messages = []
    for i in range(50):
        messages.append(json.dumps({"key": i, "value": i}))
    asyncio.run(nats_produce_messages(nats_cluster, "mvsq", messages))

    nats_check_query_result("SELECT * FROM test.view ORDER BY key")


def test_nats_many_materialized_views(nats_cluster):
    instance.query(
        """
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = 'mmv',
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
    wait_for_table_is_ready(instance, "test.nats")

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer1 TO test.view1 AS
            SELECT * FROM test.nats;
        CREATE MATERIALIZED VIEW test.consumer2 TO test.view2 AS
            SELECT * FROM test.nats;
        """
    )
    wait_for_mv_attached_to_table(instance, "test.nats")
    
    messages = []
    for i in range(50):
        messages.append(json.dumps({"key": i, "value": i}))
    asyncio.run(nats_produce_messages(nats_cluster, "mmv", messages))

    time_limit_sec = 60
    deadline = time.monotonic() + time_limit_sec

    while time.monotonic() < deadline:
        result1 = instance.query("SELECT * FROM test.view1 ORDER BY key")
        result2 = instance.query("SELECT * FROM test.view2 ORDER BY key")
        if nats_check_result(result1) and nats_check_result(result2):
            break

    nats_check_result(result1, True)
    nats_check_result(result2, True)


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
    wait_for_table_is_ready(instance, "test.nats")

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.nats;
        """
    )
    wait_for_mv_attached_to_table(instance, "test.nats")

    data = b""
    for i in range(0, 20):
        msg = nats_pb2.ProtoKeyValue()
        msg.key = i
        msg.value = str(i)
        serialized_msg = msg.SerializeToString()
        data = data + _VarintBytes(len(serialized_msg)) + serialized_msg
    asyncio.run(nats_produce_messages(nats_cluster, "pb", bytes=data))
    data = b""
    for i in range(20, 21):
        msg = nats_pb2.ProtoKeyValue()
        msg.key = i
        msg.value = str(i)
        serialized_msg = msg.SerializeToString()
        data = data + _VarintBytes(len(serialized_msg)) + serialized_msg
    asyncio.run(nats_produce_messages(nats_cluster, "pb", bytes=data))
    data = b""
    for i in range(21, 50):
        msg = nats_pb2.ProtoKeyValue()
        msg.key = i
        msg.value = str(i)
        serialized_msg = msg.SerializeToString()
        data = data + _VarintBytes(len(serialized_msg)) + serialized_msg
    asyncio.run(nats_produce_messages(nats_cluster, "pb", bytes=data))

    nats_check_query_result("SELECT * FROM test.view ORDER BY key")


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
    wait_for_table_is_ready(instance, "test.nats")

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.nats;
        """
    )
    wait_for_mv_attached_to_table(instance, "test.nats")

    asyncio.run(nats_produce_messages(nats_cluster, "big", messages))

    while True:
        result = instance.query("SELECT count() FROM test.view")
        if int(result) == batch_messages * nats_messages:
            break

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
    wait_for_table_is_ready(instance, "test.nats")

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
    wait_for_mv_attached_to_table(instance, "test.nats")

    i = [0]
    messages_num = 10000

    def produce():
        messages = []
        for _ in range(messages_num):
            messages.append(json.dumps({"key": i[0], "value": i[0]}))
            i[0] += 1
        asyncio.run(nats_produce_messages(nats_cluster, "combo", messages))

    threads = []
    threads_num = 20

    for _ in range(threads_num):
        threads.append(threading.Thread(target=produce))
    for thread in threads:
        time.sleep(random.uniform(0, 1))
        thread.start()

    while True:
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
    wait_for_table_is_ready(instance, "test.nats")

    values = []
    for i in range(50):
        values.append("({i}, {i})".format(i=i))
    values = ",".join(values)

    insert_messages = []

    async def sub_to_nats():
        nc = await nats_connect_ssl(
            nats_cluster.nats_port,
            user="click",
            password="house",
            ssl_ctx=nats_cluster.nats_ssl_context,
        )
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

    while True:
        try:
            instance.query("INSERT INTO test.nats VALUES {}".format(values))
            break
        except QueryRuntimeException as e:
            if "Local: Timed out." in str(e):
                continue
            else:
                raise
    thread.join()

    result = "\n".join(insert_messages)
    nats_check_result(result, True)


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
    wait_for_table_is_ready(instance, "test.nats")

    values = []
    for i in range(50):
        values.append("({i}, {i})".format(i=i))
    values = ",".join(values)

    insert_messages = []

    async def sub_to_nats():
        nc = await nats_connect_ssl(
            nats_cluster.nats_port,
            user="click",
            password="house",
            ssl_ctx=nats_cluster.nats_ssl_context,
        )
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

    while True:
        try:
            instance.query("INSERT INTO test.nats VALUES {}".format(values))
            break
        except QueryRuntimeException as e:
            if "Local: Timed out." in str(e):
                continue
            else:
                raise
    thread.join()

    result = "\n".join(insert_messages)
    nats_check_result(result, True)


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
    wait_for_table_is_ready(instance, "test.nats")

    values = []
    for i in range(50):
        values.append("({i}, {i})".format(i=i))
    values = ",".join(values)

    # no subject specified
    instance.query_and_get_error("INSERT INTO test.nats VALUES {}".format(values))

    # can't insert into wildcard subjects
    instance.query_and_get_error(
        "INSERT INTO test.nats SETTINGS stream_like_engine_insert_queue='insert2.>' VALUES {}".format(
            values
        )
    )
    instance.query_and_get_error(
        "INSERT INTO test.nats SETTINGS stream_like_engine_insert_queue='insert3.*.foo' VALUES {}".format(
            values
        )
    )

    # specified subject is not among engine's subjects
    instance.query_and_get_error(
        "INSERT INTO test.nats SETTINGS stream_like_engine_insert_queue='insert4' VALUES {}".format(
            values
        )
    )
    instance.query_and_get_error(
        "INSERT INTO test.nats SETTINGS stream_like_engine_insert_queue='insert3.foo.baz' VALUES {}".format(
            values
        )
    )
    instance.query_and_get_error(
        "INSERT INTO test.nats SETTINGS stream_like_engine_insert_queue='foo.insert2' VALUES {}".format(
            values
        )
    )


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
    wait_for_table_is_ready(instance, "test.nats")

    values = []
    for i in range(50):
        values.append("({i}, {i})".format(i=i))
    values = ",".join(values)

    insert_messages = []

    async def sub_to_nats():
        nc = await nats_connect_ssl(
            nats_cluster.nats_port,
            user="click",
            password="house",
            ssl_ctx=nats_cluster.nats_ssl_context,
        )
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

    while True:
        try:
            instance.query(
                "INSERT INTO test.nats SETTINGS stream_like_engine_insert_queue='right_insert1' VALUES {}".format(
                    values
                )
            )
            break
        except QueryRuntimeException as e:
            if "Local: Timed out." in str(e):
                continue
            else:
                raise
    thread.join()

    result = "\n".join(insert_messages)
    nats_check_result(result, True)


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
    wait_for_table_is_ready(instance, "test.nats_consume")
    wait_for_table_is_ready(instance, "test.nats_many")

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer_many TO test.view_many AS
            SELECT * FROM test.nats_consume;
        """
    )
    wait_for_mv_attached_to_table(instance, "test.nats_consume")

    messages_num = 10000
    values = []
    for i in range(messages_num):
        values.append("({i}, {i})".format(i=i))
    values = ",".join(values)

    def insert():
        while True:
            try:
                instance.query("INSERT INTO test.nats_many VALUES {}".format(values))
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

    for thread in threads:
        thread.join()

    time_limit_sec = 300
    deadline = time.monotonic() + time_limit_sec

    while time.monotonic() < deadline:
        result = instance.query("SELECT count() FROM test.view_many")
        logging.debug(result, messages_num * threads_num)
        if int(result) >= messages_num * threads_num:
            break
        time.sleep(1)

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
    wait_for_table_is_ready(instance, "test.nats_consume")
    wait_for_table_is_ready(instance, "test.nats_overload")

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer_overload TO test.view_overload AS
            SELECT * FROM test.nats_consume;
        """
    )
    wait_for_mv_attached_to_table(instance, "test.nats_consume")

    messages_num = 100000

    def insert():
        values = []
        for i in range(messages_num):
            values.append("({i}, {i})".format(i=i))
        values = ",".join(values)

        while True:
            try:
                instance.query(
                    "INSERT INTO test.nats_overload VALUES {}".format(values)
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

    time_limit_sec = 300
    deadline = time.monotonic() + time_limit_sec

    while time.monotonic() < deadline:
        result = instance.query("SELECT count() FROM test.view_overload")
        time.sleep(1)
        if int(result) >= messages_num * threads_num:
            break

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
    wait_for_table_is_ready(instance, "test.nats_virtuals")

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.view Engine=Log AS
        SELECT value, key, _subject FROM test.nats_virtuals;
        """
    )
    wait_for_mv_attached_to_table(instance, "test.nats_virtuals")
    
    message_num = 10
    i = 0
    messages = []
    for _ in range(message_num):
        messages.append(json.dumps({"key": i, "value": i}))
        i += 1

    asyncio.run(nats_produce_messages(nats_cluster, "virtuals", messages))

    while True:
        result = instance.query("SELECT count() FROM test.view")
        time.sleep(1)
        if int(result) == message_num:
            break

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
    wait_for_table_is_ready(instance, "test.nats_virtuals_mv")

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
        SELECT *, _subject as subject
        FROM test.nats_virtuals_mv;
        """
    )
    wait_for_mv_attached_to_table(instance, "test.nats_virtuals_mv")
    
    message_num = 10
    i = 0
    messages = []
    for _ in range(message_num):
        messages.append(json.dumps({"key": i, "value": i}))
        i += 1

    asyncio.run(nats_produce_messages(nats_cluster, "virtuals_mv", messages))

    while True:
        result = instance.query("SELECT count() FROM test.view")
        time.sleep(1)
        if int(result) == message_num:
            break

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

    instance.query(
        """
        DROP TABLE test.consumer;
        DROP TABLE test.view;
        DROP TABLE test.nats_virtuals_mv
    """
    )

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
        wait_for_table_is_ready(instance, "test.many_consumers_{}".format(table_id))

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
        wait_for_mv_attached_to_table(instance, "test.many_consumers_{0}".format(table_id))

    i = [0]
    messages_num = 1000

    def produce():
        messages = []
        for _ in range(messages_num):
            messages.append(json.dumps({"key": i[0], "value": i[0]}))
            i[0] += 1
        asyncio.run(nats_produce_messages(nats_cluster, "many_consumers", messages))

    threads = []
    threads_num = 20

    for _ in range(threads_num):
        threads.append(threading.Thread(target=produce))
    for thread in threads:
        time.sleep(random.uniform(0, 1))
        thread.start()

    result1 = ""
    while True:
        result1 = instance.query("SELECT count() FROM test.destination")
        time.sleep(1)
        if int(result1) == messages_num * threads_num:
            break

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
    
    wait_for_table_is_ready(instance, "test.consume")
    wait_for_table_is_ready(instance, "test.producer_reconnect")

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.consume;
        """
    )
    wait_for_mv_attached_to_table(instance, "test.consume")

    messages_num = 100000
    values = []
    for i in range(messages_num):
        values.append("({i}, {i})".format(i=i))
    values = ",".join(values)

    while True:
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

    while int(instance.query("SELECT count() FROM test.view")) == 0:
        time.sleep(0.1)

    kill_nats(nats_cluster.nats_docker_id)
    time.sleep(4)
    revive_nats(nats_cluster.nats_docker_id, nats_cluster.nats_port)

    while True:
        result = instance.query("SELECT count(DISTINCT key) FROM test.view")
        time.sleep(1)
        if int(result) == messages_num:
            break

    assert int(result) == messages_num, "ClickHouse lost some messages: {}".format(
        result
    )


def test_nats_no_connection_at_startup_1(nats_cluster):
    with nats_cluster.pause_container("nats1"):
        instance.query_and_get_error(
            """
            CREATE TABLE test.cs (key UInt64, value UInt64)
                ENGINE = NATS
                SETTINGS nats_url = 'nats1:4444',
                        nats_subjects = 'cs',
                        nats_format = 'JSONEachRow',
                        nats_num_consumers = '5',
                        nats_row_delimiter = '\\n';
        """
        )
        instance.query_and_get_error(
            """
            SHOW TABLE test.cs;
        """
        )


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
        instance.query("ATTACH TABLE test.cs")

    wait_for_table_is_ready(instance, "test.cs")

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.cs;
        """
    )
    wait_for_mv_attached_to_table(instance, "test.cs")

    messages_num = 1000
    messages = []
    for i in range(messages_num):
        messages.append(json.dumps({"key": i, "value": i}))
    asyncio.run(nats_produce_messages(nats_cluster, "cs", messages))

    for _ in range(20):
        result = instance.query("SELECT count() FROM test.view")
        time.sleep(1)
        if int(result) == messages_num:
            break

    assert int(result) == messages_num, "ClickHouse lost some messages: {}".format(
        result
    )


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
    wait_for_table_is_ready(instance, "test.format_settings")

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.format_settings;
        """
    )
    wait_for_mv_attached_to_table(instance, "test.format_settings")


    message = json.dumps(
        {"id": "format_settings_test", "date": "2021-01-19T14:42:33.1829214Z"}
    )
    expected = instance.query(
        """SELECT parseDateTimeBestEffort(CAST('2021-01-19T14:42:33.1829214Z', 'String'))"""
    )

    asyncio.run(nats_produce_messages(nats_cluster, "format_settings", [message]))
    while True:
        result = instance.query("SELECT date FROM test.view")
        if result == expected:
            break

    assert result == expected


def test_nats_bad_args(nats_cluster):
    instance.query_and_get_error(
        """
        CREATE TABLE test.drop (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_secure = true,
                     nats_format = 'JSONEachRow';
        """
    )


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
    wait_for_table_is_ready(instance, "test.nats")

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.nats;
        """
    )
    wait_for_mv_attached_to_table(instance, "test.nats")
    
    messages = []
    for i in range(20):
        messages.append(json.dumps({"key": i, "value": i}))
    asyncio.run(nats_produce_messages(nats_cluster, "mv", messages))

    wait_query_result(instance, "SELECT count() FROM test.view", 20)

    instance.query("DROP VIEW test.consumer")
    wait_for_table_is_ready(instance, "test.nats")

    messages = []
    for i in range(100, 200):
        messages.append(json.dumps({"key": i, "value": i}))
    asyncio.run(nats_produce_messages(nats_cluster, "mv", messages))

    time.sleep (1)

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.nats;
        """
    )
    wait_for_mv_attached_to_table(instance, "test.nats")

    messages = []
    for i in range(20, 40):
        messages.append(json.dumps({"key": i, "value": i}))
    asyncio.run(nats_produce_messages(nats_cluster, "mv", messages))

    wait_query_result(instance, "SELECT count() FROM test.view", 40)

    instance.query("DROP VIEW test.consumer")
    wait_for_table_is_ready(instance, "test.nats")

    messages = []
    for i in range(200, 400):
        messages.append(json.dumps({"key": i, "value": i}))
    asyncio.run(nats_produce_messages(nats_cluster, "mv", messages))

    time.sleep (1)

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.nats;
        """
    )
    wait_for_mv_attached_to_table(instance, "test.nats")

    messages = []
    for i in range(40, 50):
        messages.append(json.dumps({"key": i, "value": i}))
    asyncio.run(nats_produce_messages(nats_cluster, "mv", messages))
    nats_check_query_result("SELECT * FROM test.view ORDER BY key")

    instance.query("DROP VIEW test.consumer")
    wait_for_table_is_ready(instance, "test.nats")

    messages = []
    for i in range(400, 500):
        messages.append(json.dumps({"key": i, "value": i}))
    asyncio.run(nats_produce_messages(nats_cluster, "mv", messages))
    nats_check_query_result("SELECT * FROM test.view ORDER BY key")


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
    wait_for_table_is_ready(instance, "test.nats")

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.nats;
        """
    )
    wait_for_mv_attached_to_table(instance, "test.nats")

    asyncio.run(
        nats_produce_messages(
            nats_cluster, "named", [json.dumps({"key": 1, "value": 2})]
        )
    )
    while True:
        result = instance.query(
            "SELECT * FROM test.view ORDER BY key", ignore_error=True
        )
        if result == "1\t2\n":
            break


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
    wait_for_table_is_ready(instance, "test.nats")

    insert_messages = []

    async def sub_to_nats():
        nc = await nats_connect_ssl(
            nats_cluster.nats_port,
            user="click",
            password="house",
            ssl_ctx=nats_cluster.nats_ssl_context,
        )
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
    wait_for_table_is_ready(instance, "test.nats")

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.view Engine=Log AS
        SELECT key, value FROM test.nats;
        """
    )
    wait_for_mv_attached_to_table(instance, "test.nats")

    num_rows = 5

    insert_messages = []

    async def sub_to_nats():
        nc = await nats_connect_ssl(
            nats_cluster.nats_port,
            user="click",
            password="house",
            ssl_ctx=nats_cluster.nats_ssl_context,
        )
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

    attempt = 0
    rows = 0
    while attempt < 100:
        rows = int(instance.query("SELECT count() FROM test.view"))
        if rows == num_rows:
            break
        attempt += 1

    assert rows == num_rows

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
        wait_for_table_is_ready(instance, "test.nats")

        instance.query(
            """
            CREATE MATERIALIZED VIEW test.view Engine=Log AS
            SELECT key, value FROM test.nats;
            """
        )
        wait_for_mv_attached_to_table(instance, "test.nats")

        insert_messages = 0

        async def sub_to_nats():
            nc = await nats_connect_ssl(
                nats_cluster.nats_port,
                user="click",
                password="house",
                ssl_ctx=nats_cluster.nats_ssl_context,
            )
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

        attempt = 0
        rows = 0
        while attempt < 100:
            rows = int(instance.query("SELECT count() FROM test.view"))
            if rows == num_rows:
                break
            attempt += 1

        assert rows == num_rows

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
        nc = await nats_connect_ssl(
            nats_cluster.nats_port,
            user="click",
            password="house",
            ssl_ctx=nats_cluster.nats_ssl_context,
        )
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

    attempt = 0
    while attempt < 100:
        try:
            instance.query(
                "INSERT INTO test.nats SELECT number * 10 as key, number * 100 as value FROM numbers(5) settings max_block_size=2, optimize_trivial_insert_select=0;"
            )
            break
        except Exception:
            logging.debug("Table test.nats is not yet ready")
            time.sleep(0.5)
            attempt += 1
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
        wait_for_table_is_ready(instance, "test.nats")

        instance.query(
            """
            CREATE MATERIALIZED VIEW test.view Engine=Log AS
            SELECT key, value FROM test.nats;
            """
        )
        wait_for_mv_attached_to_table(instance, "test.nats")

        insert_messages = 0

        async def sub_to_nats():
            nc = await nats_connect_ssl(
                nats_cluster.nats_port,
                user="click",
                password="house",
                ssl_ctx=nats_cluster.nats_ssl_context,
            )
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

        attempt = 0
        rows = 0
        while attempt < 100:
            rows = int(instance.query("SELECT count() FROM test.view"))
            if rows == num_rows:
                break
            attempt += 1

        assert rows == num_rows

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
                     nats_username = 'click',
                     nats_password = 'house',
                     nats_credential_file = '',
                     nats_row_delimiter = '\\n';
        """
    )

    instance.query("SYSTEM FLUSH LOGS")
    message = instance.query(f"SELECT message FROM system.text_log WHERE message ILIKE '%CREATE TABLE test.{table_name}%'")
    assert "nats_password = \\'[HIDDEN]\\'" in  message
    assert "nats_credential_file = \\'[HIDDEN]\\'" in  message
