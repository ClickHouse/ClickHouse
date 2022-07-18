import json
import os.path as p
import random
import subprocess
import threading
import logging
import time
from random import randrange
import math

import asyncio
import nats
import pytest
from google.protobuf.internal.encoder import _VarintBytes
from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster, check_nats_is_available
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


def wait_nats_to_start(nats_ip, timeout=180):
    start = time.time()
    while time.time() - start < timeout:
        try:
            if asyncio.run(check_nats_is_available(nats_ip)):
                logging.debug("NATS is available")
                return
            time.sleep(0.5)
        except Exception as ex:
            logging.debug("Can't connect to NATS " + str(ex))
            time.sleep(0.5)


def nats_check_result(result, check=False, ref_file="test_nats_json.reference"):
    fpath = p.join(p.dirname(__file__), ref_file)
    with open(fpath) as reference:
        if check:
            assert TSV(result) == TSV(reference)
        else:
            return TSV(result) == TSV(reference)


def kill_nats(nats_id):
    p = subprocess.Popen(("docker", "stop", nats_id), stdout=subprocess.PIPE)
    p.communicate()
    return p.returncode == 0


def revive_nats(nats_id, nats_ip):
    p = subprocess.Popen(("docker", "start", nats_id), stdout=subprocess.PIPE)
    p.communicate()
    wait_nats_to_start(nats_ip)


# Fixtures


@pytest.fixture(scope="module")
def nats_cluster():
    try:
        cluster.start()
        logging.debug("nats_id is {}".format(instance.cluster.nats_docker_id))
        instance.query("CREATE DATABASE test")

        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def nats_setup_teardown():
    print("NATS is available - running test")
    yield  # run test
    instance.query("DROP DATABASE test NO DELAY")
    instance.query("CREATE DATABASE test")


# Tests


async def nats_produce_messages(ip, subject, messages=(), bytes=None):
    nc = await nats.connect("{}:4444".format(ip), user="click", password="house")
    logging.debug("NATS connection status: " + str(nc.is_connected))

    for message in messages:
        await nc.publish(subject, message.encode())
    if bytes is not None:
        await nc.publish(subject, bytes)
    logging.debug("Finished publising to " + subject)

    await nc.close()
    return messages


def check_table_is_ready(instance, table_name):
    try:
        instance.query("SELECT * FROM {}".format(table_name))
        return True
    except Exception:
        return False


def test_nats_select(nats_cluster):
    instance.query(
        """
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = 'select',
                     nats_format = 'JSONEachRow',
                     nats_row_delimiter = '\\n';
        """
    )
    while not check_table_is_ready(instance, "test.nats"):
        logging.debug("Table test.nats is not yet ready")
        time.sleep(0.5)

    messages = []
    for i in range(50):
        messages.append(json.dumps({"key": i, "value": i}))
    asyncio.run(nats_produce_messages(nats_cluster.nats_ip, "select", messages))

    # The order of messages in select * from test.nats is not guaranteed, so sleep to collect everything in one select
    time.sleep(1)

    result = ""
    while True:
        result += instance.query(
            "SELECT * FROM test.nats ORDER BY key", ignore_error=True
        )
        if nats_check_result(result):
            break

    nats_check_result(result, True)


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


def test_nats_json_without_delimiter(nats_cluster):
    instance.query(
        """
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = 'json',
                     nats_format = 'JSONEachRow';
        """
    )
    while not check_table_is_ready(instance, "test.nats"):
        logging.debug("Table test.nats is not yet ready")
        time.sleep(0.5)

    messages = ""
    for i in range(25):
        messages += json.dumps({"key": i, "value": i}) + "\n"

    all_messages = [messages]
    asyncio.run(nats_produce_messages(nats_cluster.nats_ip, "json", all_messages))

    messages = ""
    for i in range(25, 50):
        messages += json.dumps({"key": i, "value": i}) + "\n"
    all_messages = [messages]
    asyncio.run(nats_produce_messages(nats_cluster.nats_ip, "json", all_messages))

    time.sleep(1)

    result = ""
    time_limit_sec = 60
    deadline = time.monotonic() + time_limit_sec

    while time.monotonic() < deadline:
        result += instance.query(
            "SELECT * FROM test.nats ORDER BY key", ignore_error=True
        )
        if nats_check_result(result):
            break

    nats_check_result(result, True)


def test_nats_csv_with_delimiter(nats_cluster):
    instance.query(
        """
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = 'csv',
                     nats_format = 'CSV',
                     nats_row_delimiter = '\\n';
        """
    )
    while not check_table_is_ready(instance, "test.nats"):
        logging.debug("Table test.nats is not yet ready")
        time.sleep(0.5)

    messages = []
    for i in range(50):
        messages.append("{i}, {i}".format(i=i))

    asyncio.run(nats_produce_messages(nats_cluster.nats_ip, "csv", messages))

    time.sleep(1)

    result = ""
    for _ in range(60):
        result += instance.query(
            "SELECT * FROM test.nats ORDER BY key", ignore_error=True
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
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.nats;
        """
    )
    while not check_table_is_ready(instance, "test.nats"):
        logging.debug("Table test.nats is not yet ready")
        time.sleep(0.5)

    messages = []
    for i in range(50):
        messages.append("{i}\t{i}".format(i=i))

    asyncio.run(nats_produce_messages(nats_cluster.nats_ip, "tsv", messages))

    result = ""
    for _ in range(60):
        result = instance.query("SELECT * FROM test.view ORDER BY key")
        if nats_check_result(result):
            break

    nats_check_result(result, True)


#


def test_nats_macros(nats_cluster):
    instance.query(
        """
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = '{nats_url}',
                     nats_subjects = '{nats_subjects}',
                     nats_format = '{nats_format}'
        """
    )
    while not check_table_is_ready(instance, "test.nats"):
        logging.debug("Table test.nats is not yet ready")
        time.sleep(0.5)

    message = ""
    for i in range(50):
        message += json.dumps({"key": i, "value": i}) + "\n"
    asyncio.run(nats_produce_messages(nats_cluster.nats_ip, "macro", [message]))

    time.sleep(1)

    result = ""
    for _ in range(60):
        result += instance.query(
            "SELECT * FROM test.nats ORDER BY key", ignore_error=True
        )
        if nats_check_result(result):
            break

    nats_check_result(result, True)


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
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.nats;

        CREATE TABLE test.view2 (key UInt64, value UInt64)
            ENGINE = MergeTree()
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer2 TO test.view2 AS
            SELECT * FROM test.nats group by (key, value);
    """
    )
    while not check_table_is_ready(instance, "test.nats"):
        logging.debug("Table test.nats is not yet ready")
        time.sleep(0.5)

    messages = []
    for i in range(50):
        messages.append(json.dumps({"key": i, "value": i}))

    asyncio.run(nats_produce_messages(nats_cluster.nats_ip, "mv", messages))

    time_limit_sec = 60
    deadline = time.monotonic() + time_limit_sec

    while time.monotonic() < deadline:
        result = instance.query("SELECT * FROM test.view ORDER BY key")
        if nats_check_result(result):
            break

    nats_check_result(result, True)

    deadline = time.monotonic() + time_limit_sec

    while time.monotonic() < deadline:
        result = instance.query("SELECT * FROM test.view2 ORDER BY key")
        if nats_check_result(result):
            break

    nats_check_result(result, True)


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
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM (SELECT * FROM test.nats);
    """
    )
    while not check_table_is_ready(instance, "test.nats"):
        logging.debug("Table test.nats is not yet ready")
        time.sleep(0.5)

    messages = []
    for i in range(50):
        messages.append(json.dumps({"key": i, "value": i}))
    asyncio.run(nats_produce_messages(nats_cluster.nats_ip, "mvsq", messages))

    time_limit_sec = 60
    deadline = time.monotonic() + time_limit_sec

    while time.monotonic() < deadline:
        result = instance.query("SELECT * FROM test.view ORDER BY key")
        if nats_check_result(result):
            break

    nats_check_result(result, True)


def test_nats_many_materialized_views(nats_cluster):
    instance.query(
        """
        DROP TABLE IF EXISTS test.view1;
        DROP TABLE IF EXISTS test.view2;
        DROP TABLE IF EXISTS test.consumer1;
        DROP TABLE IF EXISTS test.consumer2;
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
        CREATE MATERIALIZED VIEW test.consumer1 TO test.view1 AS
            SELECT * FROM test.nats;
        CREATE MATERIALIZED VIEW test.consumer2 TO test.view2 AS
            SELECT * FROM test.nats;
    """
    )
    while not check_table_is_ready(instance, "test.nats"):
        logging.debug("Table test.nats is not yet ready")
        time.sleep(0.5)

    messages = []
    for i in range(50):
        messages.append(json.dumps({"key": i, "value": i}))
    asyncio.run(nats_produce_messages(nats_cluster.nats_ip, "mmv", messages))

    time_limit_sec = 60
    deadline = time.monotonic() + time_limit_sec

    while time.monotonic() < deadline:
        result1 = instance.query("SELECT * FROM test.view1 ORDER BY key")
        result2 = instance.query("SELECT * FROM test.view2 ORDER BY key")
        if nats_check_result(result1) and nats_check_result(result2):
            break

    instance.query(
        """
        DROP TABLE test.consumer1;
        DROP TABLE test.consumer2;
        DROP TABLE test.view1;
        DROP TABLE test.view2;
    """
    )

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
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.nats;
        """
    )
    while not check_table_is_ready(instance, "test.nats"):
        logging.debug("Table test.nats is not yet ready")
        time.sleep(0.5)

    data = b""
    for i in range(0, 20):
        msg = nats_pb2.ProtoKeyValue()
        msg.key = i
        msg.value = str(i)
        serialized_msg = msg.SerializeToString()
        data = data + _VarintBytes(len(serialized_msg)) + serialized_msg
    asyncio.run(nats_produce_messages(nats_cluster.nats_ip, "pb", bytes=data))
    data = b""
    for i in range(20, 21):
        msg = nats_pb2.ProtoKeyValue()
        msg.key = i
        msg.value = str(i)
        serialized_msg = msg.SerializeToString()
        data = data + _VarintBytes(len(serialized_msg)) + serialized_msg
    asyncio.run(nats_produce_messages(nats_cluster.nats_ip, "pb", bytes=data))
    data = b""
    for i in range(21, 50):
        msg = nats_pb2.ProtoKeyValue()
        msg.key = i
        msg.value = str(i)
        serialized_msg = msg.SerializeToString()
        data = data + _VarintBytes(len(serialized_msg)) + serialized_msg
    asyncio.run(nats_produce_messages(nats_cluster.nats_ip, "pb", bytes=data))

    result = ""
    time_limit_sec = 60
    deadline = time.monotonic() + time_limit_sec

    while time.monotonic() < deadline:
        result = instance.query("SELECT * FROM test.view ORDER BY key")
        if nats_check_result(result):
            break

    nats_check_result(result, True)


def test_nats_big_message(nats_cluster):
    # Create batchs of messages of size ~100Kb
    nats_messages = 1000
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
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.nats;
    """
    )
    while not check_table_is_ready(instance, "test.nats"):
        logging.debug("Table test.nats is not yet ready")
        time.sleep(0.5)

    asyncio.run(nats_produce_messages(nats_cluster.nats_ip, "big", messages))

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
    while not check_table_is_ready(instance, "test.nats"):
        logging.debug("Table test.nats is not yet ready")
        time.sleep(0.5)

    for mv_id in range(NUM_MV):
        instance.query(
            """
            DROP TABLE IF EXISTS test.combo_{0};
            DROP TABLE IF EXISTS test.combo_{0}_mv;
            CREATE TABLE test.combo_{0} (key UInt64, value UInt64)
                ENGINE = MergeTree()
                ORDER BY key;
            CREATE MATERIALIZED VIEW test.combo_{0}_mv TO test.combo_{0} AS
                SELECT * FROM test.nats;
        """.format(
                mv_id
            )
        )

    time.sleep(2)

    i = [0]
    messages_num = 10000

    def produce():
        messages = []
        for _ in range(messages_num):
            messages.append(json.dumps({"key": i[0], "value": i[0]}))
            i[0] += 1
        asyncio.run(nats_produce_messages(nats_cluster.nats_ip, "combo", messages))

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
    while not check_table_is_ready(instance, "test.nats"):
        logging.debug("Table test.nats is not yet ready")
        time.sleep(0.5)

    values = []
    for i in range(50):
        values.append("({i}, {i})".format(i=i))
    values = ",".join(values)

    insert_messages = []

    async def sub_to_nats():
        nc = await nats.connect(
            "{}:4444".format(nats_cluster.nats_ip), user="click", password="house"
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
    while not check_table_is_ready(instance, "test.nats"):
        logging.debug("Table test.nats is not yet ready")
        time.sleep(0.5)

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
    while not check_table_is_ready(instance, "test.nats"):
        logging.debug("Table test.nats is not yet ready")
        time.sleep(0.5)

    values = []
    for i in range(50):
        values.append("({i}, {i})".format(i=i))
    values = ",".join(values)

    insert_messages = []

    async def sub_to_nats():
        nc = await nats.connect(
            "{}:4444".format(nats_cluster.nats_ip), user="click", password="house"
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
        DROP TABLE IF EXISTS test.nats_many;
        DROP TABLE IF EXISTS test.nats_consume;
        DROP TABLE IF EXISTS test.view_many;
        DROP TABLE IF EXISTS test.consumer_many;
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
        CREATE MATERIALIZED VIEW test.consumer_many TO test.view_many AS
            SELECT * FROM test.nats_consume;
    """
    )
    while not check_table_is_ready(instance, "test.nats_consume"):
        logging.debug("Table test.nats_consume is not yet ready")
        time.sleep(0.5)

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
        print(result, messages_num * threads_num)
        if int(result) >= messages_num * threads_num:
            break
        time.sleep(1)

    instance.query(
        """
        DROP TABLE test.nats_consume;
        DROP TABLE test.nats_many;
        DROP TABLE test.consumer_many;
        DROP TABLE test.view_many;
    """
    )

    assert (
        int(result) == messages_num * threads_num
    ), "ClickHouse lost some messages or got duplicated ones. Total count: {}".format(
        result
    )


def test_nats_overloaded_insert(nats_cluster):
    instance.query(
        """
        DROP TABLE IF EXISTS test.view_overload;
        DROP TABLE IF EXISTS test.consumer_overload;
        DROP TABLE IF EXISTS test.nats_consume;
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
            SETTINGS old_parts_lifetime=5, cleanup_delay_period=2, cleanup_delay_period_random_add=3;
        CREATE MATERIALIZED VIEW test.consumer_overload TO test.view_overload AS
            SELECT * FROM test.nats_consume;
    """
    )
    while not check_table_is_ready(instance, "test.nats_consume"):
        logging.debug("Table test.nats_consume is not yet ready")
        time.sleep(0.5)

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

    instance.query(
        """
        DROP TABLE test.consumer_overload;
        DROP TABLE test.view_overload;
        DROP TABLE test.nats_consume;
        DROP TABLE test.nats_overload;
    """
    )

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
        CREATE MATERIALIZED VIEW test.view Engine=Log AS
        SELECT value, key, _subject FROM test.nats_virtuals;
    """
    )
    while not check_table_is_ready(instance, "test.nats_virtuals"):
        logging.debug("Table test.nats_virtuals is not yet ready")
        time.sleep(0.5)

    message_num = 10
    i = 0
    messages = []
    for _ in range(message_num):
        messages.append(json.dumps({"key": i, "value": i}))
        i += 1

    asyncio.run(nats_produce_messages(nats_cluster.nats_ip, "virtuals", messages))

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

    instance.query(
        """
        DROP TABLE test.nats_virtuals;
        DROP TABLE test.view;
    """
    )

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
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
        SELECT *, _subject as subject
        FROM test.nats_virtuals_mv;
    """
    )
    while not check_table_is_ready(instance, "test.nats_virtuals_mv"):
        logging.debug("Table test.nats_virtuals_mv is not yet ready")
        time.sleep(0.5)

    message_num = 10
    i = 0
    messages = []
    for _ in range(message_num):
        messages.append(json.dumps({"key": i, "value": i}))
        i += 1

    asyncio.run(nats_produce_messages(nats_cluster.nats_ip, "virtuals_mv", messages))

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
        DROP TABLE IF EXISTS test.destination;
        CREATE TABLE test.destination(key UInt64, value UInt64)
        ENGINE = MergeTree()
        ORDER BY key;
    """
    )

    num_tables = 4
    for table_id in range(num_tables):
        print(("Setting up table {}".format(table_id)))
        instance.query(
            """
            DROP TABLE IF EXISTS test.many_consumers_{0};
            DROP TABLE IF EXISTS test.many_consumers_{0}_mv;
            CREATE TABLE test.many_consumers_{0} (key UInt64, value UInt64)
                ENGINE = NATS
                SETTINGS nats_url = 'nats1:4444',
                         nats_subjects = 'many_consumers',
                         nats_num_consumers = 2,
                         nats_queue_group = 'many_consumers',
                         nats_format = 'JSONEachRow',
                         nats_row_delimiter = '\\n';
            CREATE MATERIALIZED VIEW test.many_consumers_{0}_mv TO test.destination AS
            SELECT key, value FROM test.many_consumers_{0};
        """.format(
                table_id
            )
        )
        while not check_table_is_ready(
            instance, "test.many_consumers_{}".format(table_id)
        ):
            logging.debug(
                "Table test.many_consumers_{} is not yet ready".format(table_id)
            )
            time.sleep(0.5)

    i = [0]
    messages_num = 1000

    def produce():
        messages = []
        for _ in range(messages_num):
            messages.append(json.dumps({"key": i[0], "value": i[0]}))
            i[0] += 1
        asyncio.run(
            nats_produce_messages(nats_cluster.nats_ip, "many_consumers", messages)
        )

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
        DROP TABLE IF EXISTS test.consume;
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
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.consume;
        DROP TABLE IF EXISTS test.producer_reconnect;
        CREATE TABLE test.producer_reconnect (key UInt64, value UInt64)
            ENGINE = NATS
            SETTINGS nats_url = 'nats1:4444',
                     nats_subjects = 'producer_reconnect',
                     nats_format = 'JSONEachRow',
                     nats_row_delimiter = '\\n';
    """
    )
    while not check_table_is_ready(instance, "test.consume"):
        logging.debug("Table test.consume is not yet ready")
        time.sleep(0.5)

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
    revive_nats(nats_cluster.nats_docker_id, nats_cluster.nats_ip)

    while True:
        result = instance.query("SELECT count(DISTINCT key) FROM test.view")
        time.sleep(1)
        if int(result) == messages_num:
            break

    instance.query(
        """
        DROP TABLE test.consume;
        DROP TABLE test.producer_reconnect;
    """
    )

    assert int(result) == messages_num, "ClickHouse lost some messages: {}".format(
        result
    )


def test_nats_no_connection_at_startup_1(nats_cluster):
    # no connection when table is initialized
    nats_cluster.pause_container("nats1")
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
    nats_cluster.unpause_container("nats1")


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

    instance.query("DETACH TABLE test.cs")
    nats_cluster.pause_container("nats1")
    instance.query("ATTACH TABLE test.cs")
    nats_cluster.unpause_container("nats1")
    while not check_table_is_ready(instance, "test.cs"):
        logging.debug("Table test.cs is not yet ready")
        time.sleep(0.5)

    messages_num = 1000
    messages = []
    for i in range(messages_num):
        messages.append(json.dumps({"key": i, "value": i}))
    asyncio.run(nats_produce_messages(nats_cluster.nats_ip, "cs", messages))

    for _ in range(20):
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
        """
    )
    while not check_table_is_ready(instance, "test.format_settings"):
        logging.debug("Table test.format_settings is not yet ready")
        time.sleep(0.5)

    message = json.dumps(
        {"id": "format_settings_test", "date": "2021-01-19T14:42:33.1829214Z"}
    )
    expected = instance.query(
        """SELECT parseDateTimeBestEffort(CAST('2021-01-19T14:42:33.1829214Z', 'String'))"""
    )

    asyncio.run(
        nats_produce_messages(nats_cluster.nats_ip, "format_settings", [message])
    )

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

    asyncio.run(
        nats_produce_messages(nats_cluster.nats_ip, "format_settings", [message])
    )
    while True:
        result = instance.query("SELECT date FROM test.view")
        if result == expected:
            break

    instance.query(
        """
        DROP TABLE test.consumer;
        DROP TABLE test.format_settings;
    """
    )

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
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.nats;
    """
    )
    while not check_table_is_ready(instance, "test.nats"):
        logging.debug("Table test.nats is not yet ready")
        time.sleep(0.5)

    messages = []
    for i in range(20):
        messages.append(json.dumps({"key": i, "value": i}))
    asyncio.run(nats_produce_messages(nats_cluster.nats_ip, "mv", messages))

    instance.query("DROP VIEW test.consumer")
    messages = []
    for i in range(20, 40):
        messages.append(json.dumps({"key": i, "value": i}))
    asyncio.run(nats_produce_messages(nats_cluster.nats_ip, "mv", messages))

    instance.query(
        """
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.nats;
    """
    )
    messages = []
    for i in range(40, 50):
        messages.append(json.dumps({"key": i, "value": i}))
    asyncio.run(nats_produce_messages(nats_cluster.nats_ip, "mv", messages))

    while True:
        result = instance.query("SELECT * FROM test.view ORDER BY key")
        if nats_check_result(result):
            break

    nats_check_result(result, True)

    instance.query("DROP VIEW test.consumer")
    messages = []
    for i in range(50, 60):
        messages.append(json.dumps({"key": i, "value": i}))
    asyncio.run(nats_produce_messages(nats_cluster.nats_ip, "mv", messages))

    count = 0
    while True:
        count = int(instance.query("SELECT count() FROM test.nats"))
        if count:
            break

    assert count > 0


def test_nats_predefined_configuration(nats_cluster):
    instance.query(
        """
        CREATE TABLE test.nats (key UInt64, value UInt64)
            ENGINE = NATS(nats1) """
    )
    while not check_table_is_ready(instance, "test.nats"):
        logging.debug("Table test.nats is not yet ready")
        time.sleep(0.5)

    asyncio.run(
        nats_produce_messages(
            nats_cluster.nats_ip, "named", [json.dumps({"key": 1, "value": 2})]
        )
    )
    while True:
        result = instance.query(
            "SELECT * FROM test.nats ORDER BY key", ignore_error=True
        )
        if result == "1\t2\n":
            break


if __name__ == "__main__":
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
