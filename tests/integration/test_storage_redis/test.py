## sudo -H pip install redis
import json
import os
import struct
import sys

import pytest
import redis

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV, wait_condition

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance("node", with_redis=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_redis_connection(db_id=0):
    client = redis.Redis(
        host="localhost", port=cluster.redis_port, password="clickhouse", db=db_id
    )
    return client


def get_address_for_ch():
    return cluster.redis_host + ":6379"


def drop_table(table):
    node.query(f"DROP TABLE IF EXISTS {table} SYNC")


# see SerializationString.serializeBinary
def serialize_binary_for_string(x):
    var_uint_max = (1 << 63) - 1
    buf = bytearray()
    # write length
    length = len(x)
    # length = (length << 1) ^ (length >> 63)
    if length > var_uint_max:
        raise ValueError("Value too large for varint encoding")
    for i in range(9):
        byte = length & 0x7F
        if length > 0x7F:
            byte |= 0x80
        buf += bytes([byte])
        length >>= 7
        if not length:
            break
    # write data
    buf += x.encode("utf-8")
    return bytes(buf)


# see SerializationNumber.serializeBinary
def serialize_binary_for_uint32(x):
    buf = bytearray()
    packed_num = struct.pack("I", x)
    buf += packed_num
    if sys.byteorder != "little":
        buf.reverse()
    return bytes(buf)


def test_simple_select(started_cluster):
    client = get_redis_connection()
    address = get_address_for_ch()

    # clean all
    client.flushall()
    drop_table("test_simple_select")

    data = {}
    for i in range(100):
        packed = serialize_binary_for_string(str(i))
        data[packed] = packed

    client.mset(data)
    client.close()

    # create table
    node.query(
        f"""
        CREATE TABLE test_simple_select(
            k String, 
            v String
        ) Engine=Redis('{address}', 0, 'clickhouse') PRIMARY KEY (k)
        """
    )

    response = TSV.toMat(
        node.query("SELECT k, v FROM test_simple_select WHERE k='0' FORMAT TSV")
    )
    assert len(response) == 1
    assert response[0] == ["0", "0"]

    response = TSV.toMat(
        node.query("SELECT * FROM test_simple_select ORDER BY k FORMAT TSV")
    )
    assert len(response) == 100
    assert response[0] == ["0", "0"]


def test_select_int(started_cluster):
    client = get_redis_connection()
    address = get_address_for_ch()

    # clean all
    client.flushall()
    drop_table("test_select_int")

    data = {}
    for i in range(100):
        packed = serialize_binary_for_uint32(i)
        data[packed] = packed

    client.mset(data)
    client.close()

    # create table
    node.query(
        f"""
        CREATE TABLE test_select_int(
            k UInt32, 
            v UInt32
        ) Engine=Redis('{address}', 0, 'clickhouse') PRIMARY KEY (k)
        """
    )

    response = TSV.toMat(
        node.query("SELECT k, v FROM test_select_int WHERE k=0 FORMAT TSV")
    )
    assert len(response) == 1
    assert response[0] == ["0", "0"]

    response = TSV.toMat(
        node.query("SELECT * FROM test_select_int ORDER BY k FORMAT TSV")
    )
    assert len(response) == 100
    assert response[0] == ["0", "0"]


def test_create_table(started_cluster):
    address = get_address_for_ch()

    # simple creation
    drop_table("test_create_table")
    node.query(
        f"""
        CREATE TABLE test_create_table(
            k String,
            v UInt32
        ) Engine=Redis('{address}') PRIMARY KEY (k)
        """
    )

    # simple creation with full engine args
    drop_table("test_create_table")
    node.query(
        f"""
        CREATE TABLE test_create_table(
            k String,
            v UInt32
        ) Engine=Redis('{address}', 0, 'clickhouse', 10) PRIMARY KEY (k)
        """
    )

    drop_table("test_create_table")
    node.query(
        f"""
        CREATE TABLE test_create_table(
            k String,
            f String,
            v UInt32
        ) Engine=Redis('{address}', 0, 'clickhouse', 10) PRIMARY KEY (k)
        """
    )

    drop_table("test_create_table")
    with pytest.raises(QueryRuntimeException):
        node.query(
            f"""
            CREATE TABLE test_create_table(
                k String,
                f String,
                v UInt32
            ) Engine=Redis('{address}', 0, 'clickhouse', 10) PRIMARY KEY ()
            """
        )

    drop_table("test_create_table")
    with pytest.raises(QueryRuntimeException):
        node.query(
            f"""
            CREATE TABLE test_create_table(
                k String,
                f String,
                v UInt32
            ) Engine=Redis('{address}', 0, 'clickhouse', 10)
            """
        )


def test_simple_insert(started_cluster):
    client = get_redis_connection()
    address = get_address_for_ch()

    # clean all
    client.flushall()
    drop_table("test_simple_insert")

    node.query(
        f"""
        CREATE TABLE test_simple_insert(
            k UInt32, 
            m DateTime,
            n String
        ) Engine=Redis('{address}', 0, 'clickhouse') PRIMARY KEY (k)
        """
    )

    node.query(
        """
        INSERT INTO test_simple_insert Values 
        (1, '2023-06-01 00:00:00', 'lili'), (2, '2023-06-02 00:00:00', 'lucy')
        """
    )

    response = node.query("SELECT COUNT(*) FROM test_simple_insert FORMAT Values")
    assert response == "(2)"

    response = TSV.toMat(
        node.query("SELECT k, m, n FROM test_simple_insert WHERE k=1 FORMAT TSV")
    )
    assert len(response) == 1
    assert response[0] == ["1", "2023-06-01 00:00:00", "lili"]

    response = TSV.toMat(
        node.query(
            "SELECT k, m, n FROM test_simple_insert WHERE m='2023-06-01 00:00:00' FORMAT TSV"
        )
    )
    assert len(response) == 1
    assert response[0] == ["1", "2023-06-01 00:00:00", "lili"]

    response = TSV.toMat(
        node.query("SELECT k, m, n FROM test_simple_insert WHERE n='lili' FORMAT TSV")
    )
    assert len(response) == 1
    assert response[0] == ["1", "2023-06-01 00:00:00", "lili"]


def test_update(started_cluster):
    client = get_redis_connection()
    address = get_address_for_ch()
    # clean all
    client.flushall()
    drop_table("test_update")

    node.query(
        f"""
        CREATE TABLE test_update(
            k UInt32, 
            m DateTime,
            n String
        ) Engine=Redis('{address}', 0, 'clickhouse') PRIMARY KEY (k)
        """
    )

    node.query(
        """
        INSERT INTO test_update Values 
        (1, '2023-06-01 00:00:00', 'lili'), (2, '2023-06-02 00:00:00', 'lucy')
        """
    )

    response = node.query(
        """
        ALTER TABLE test_update UPDATE m='2023-06-03 00:00:00' WHERE k=1
        """
    )

    print("update response: ", response)

    response = TSV.toMat(
        node.query("SELECT k, m, n FROM test_update WHERE k=1 FORMAT TSV")
    )
    assert len(response) == 1
    assert response[0] == ["1", "2023-06-03 00:00:00", "lili"]

    # can not update key
    with pytest.raises(QueryRuntimeException):
        node.query(
            """
            ALTER TABLE test_update UPDATE k=2 WHERE k=1
            """
        )


def test_delete(started_cluster):
    client = get_redis_connection()
    address = get_address_for_ch()

    # clean all
    client.flushall()
    drop_table("test_delete")

    node.query(
        f"""
        CREATE TABLE test_delete(
            k UInt32, 
            m DateTime,
            n String
        ) Engine=Redis('{address}', 0, 'clickhouse') PRIMARY KEY (k)
        """
    )

    node.query(
        """
        INSERT INTO test_delete Values 
        (1, '2023-06-01 00:00:00', 'lili'), (2, '2023-06-02 00:00:00', 'lucy')
        """
    )

    response = node.query(
        """
        ALTER TABLE test_delete DELETE WHERE k=1
        """
    )

    print("delete response: ", response)

    response = TSV.toMat(node.query("SELECT k, m, n FROM test_delete FORMAT TSV"))
    assert len(response) == 1
    assert response[0] == ["2", "2023-06-02 00:00:00", "lucy"]

    response = node.query(
        """
        ALTER TABLE test_delete DELETE WHERE m='2023-06-02 00:00:00'
        """
    )

    response = TSV.toMat(node.query("SELECT k, m, n FROM test_delete FORMAT TSV"))
    assert len(response) == 0


def test_truncate(started_cluster):
    client = get_redis_connection()
    address = get_address_for_ch()
    # clean all
    client.flushall()
    drop_table("test_truncate")

    node.query(
        f"""
        CREATE TABLE test_truncate(
            k UInt32, 
            m DateTime,
            n String
        ) Engine=Redis('{address}', 0, 'clickhouse') PRIMARY KEY (k)
        """
    )

    node.query(
        """
        INSERT INTO test_truncate Values 
        (1, '2023-06-01 00:00:00', 'lili'), (2, '2023-06-02 00:00:00', 'lucy')
        """
    )

    response = node.query(
        """
        TRUNCATE TABLE test_truncate
        """
    )

    print("truncate table response: ", response)

    response = TSV.toMat(node.query("SELECT COUNT(*) FROM test_truncate FORMAT TSV"))
    assert len(response) == 1
    assert response[0] == ["0"]


def test_hiding_credentials(started_cluster):
    address = get_address_for_ch()
    table_name = "test_hiding_credentials"
    node.query(
        f"""
        DROP TABLE IF EXISTS {table_name};
        CREATE TABLE {table_name} (k String, v String) Engine=Redis('{address}', 0, "password") PRIMARY KEY (k)
        """
    )
    node.query("SYSTEM FLUSH LOGS")
    message = node.query(f"SELECT message FROM system.text_log WHERE message ILIKE '%CREATE TABLE {table_name}%'")
    assert "password" not in message
    assert f"Redis(\\'{address}\\', 0, \\'[HIDDEN]\\')" in message


def test_direct_join(started_cluster):
    address = get_address_for_ch()

    # clean all
    drop_table("test_direct_join")
    drop_table("test_mt")

    # create table
    node.query(
        f"""
            CREATE TABLE test_direct_join(k Int) Engine=Redis('{address}', 1, 'clickhouse') PRIMARY KEY (k);
            CREATE TABLE test_mt (k Int) ENGINE = MergeTree() ORDER BY tuple();
            INSERT INTO TABLE test_direct_join VALUES (1);
            INSERT INTO TABLE test_mt VALUES (1);
        """
    )

    response = TSV.toMat(node.query("SELECT * FROM test_direct_join JOIN test_mt ON "
                                    "test_direct_join.k = test_mt.k FORMAT TSV"))
    assert len(response) == 1
    assert response[0] == ["1", "1"]

    response = TSV.toMat(node.query("SELECT * FROM test_mt JOIN test_direct_join ON "
                                    "test_direct_join.k = test_mt.k FORMAT TSV"))
    assert len(response) == 1
    assert response[0] == ["1", "1"]


def test_direct_join_nullable_left(started_cluster):
    """DirectKeyValueJoin with a Nullable key on the left side of Redis storage."""
    address = get_address_for_ch()

    drop_table("redis_str_pk")
    drop_table("t_null")
    drop_table("t_str")

    node.query(
        f"""
        CREATE TABLE redis_str_pk(key String, value String)
        Engine=Redis('{address}', 3, 'clickhouse') PRIMARY KEY (key);

        INSERT INTO redis_str_pk VALUES ('a', 'A'), ('b', 'B'), ('c', 'C');

        CREATE TABLE t_str (k String) ENGINE = TinyLog;
        INSERT INTO t_str VALUES ('a'), ('b'), ('c'), ('d');

        CREATE TABLE t_null (k Nullable(String)) ENGINE = TinyLog;
        INSERT INTO t_null VALUES ('a'), ('b'), ('c'), ('d'), (NULL);
        """
    )

    response = TSV.toMat(node.query(
        "SELECT key, value FROM (SELECT k AS key FROM t_str) AS t "
        "INNER JOIN redis_str_pk USING (key) ORDER BY key FORMAT TSV"
    ))
    assert response == [["a", "A"], ["b", "B"], ["c", "C"]]

    response = TSV.toMat(node.query(
        "SELECT key, value FROM (SELECT k AS key FROM t_str) AS t "
        "LEFT JOIN redis_str_pk USING (key) ORDER BY key FORMAT TSV"
    ))
    assert response == [["a", "A"], ["b", "B"], ["c", "C"], ["d", ""]]

    response = TSV.toMat(node.query(
        "SELECT key, value FROM (SELECT k AS key FROM t_null) AS t "
        "INNER JOIN redis_str_pk USING (key) ORDER BY key FORMAT TSV"
    ))
    assert response == [["a", "A"], ["b", "B"], ["c", "C"]]

    response = TSV.toMat(node.query(
        "SELECT key, value FROM (SELECT k AS key FROM t_null) AS t "
        "LEFT JOIN redis_str_pk USING (key) ORDER BY key NULLS LAST, value FORMAT TSV"
    ))
    assert response == [
        ["a", "A"],
        ["b", "B"],
        ["c", "C"],
        ["d", ""],
        ["\\N", ""],
    ]

    plan = node.query(
        "EXPLAIN actions = 1 "
        "SELECT key, value FROM (SELECT k AS key FROM t_null) AS t "
        "INNER JOIN redis_str_pk USING (key)"
    )
    assert "Algorithm: DirectKeyValueJoin" in plan, plan

    drop_table("redis_str_pk")
    drop_table("t_null")
    drop_table("t_str")


def test_get_keys(started_cluster):
    """
    Checks that ClickHouse reads by key instead of full scan if possible.
    """
    address = get_address_for_ch()

    # clean all
    drop_table("test_get_keys")

    # create table
    node.query(f"""
               CREATE TABLE test_get_keys(k Int) Engine=Redis('{address}', 2, 'clickhouse') PRIMARY KEY (k);
               INSERT INTO test_get_keys VALUES (1), (2), (3);
               """)

    def check_query(query, read_type, keys_count, rows_read):
        plan = node.query(f'EXPLAIN actions=1 {query}')
        assert 'ReadFromRedis' in plan
        assert f'ReadType: {read_type}' in plan
        if read_type == 'GetKeys':
            assert f'Keys: {keys_count}' in plan

        res = node.query(f'{query} FORMAT JSON')
        assert json.loads(res)['statistics']['rows_read'] == rows_read, res

    check_query("SELECT * FROM test_get_keys", "FullScan", 0, 3)
    check_query("SELECT * FROM test_get_keys WHERE k = 1", "GetKeys", 1, 1)
    check_query("SELECT * FROM test_get_keys WHERE k in (3, 5)", "GetKeys", 2, 1)

    plan = node.query("EXPLAIN actions=1, optimize=0 SELECT * FROM test_get_keys")
    assert 'ReadType: FullScan' in plan


# Ports of the three fake Redis endpoints, inside the ClickHouse container.
FAKE_REDIS_PORT_OK = 16379
FAKE_REDIS_PORT_LONG = 16380
FAKE_REDIS_PORT_SHORT = 16381
FAKE_REDIS_PORT_SCAN = 16382


def start_fake_redis(port, delta, keys=""):
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"python3 /fake_redis.py {port} {delta} {keys}"
            f" > /var/log/clickhouse-server/fake_redis_{port}.log 2>&1",
        ],
        detach=True,
        user="root",
    )
    wait_condition(
        lambda: node.exec_in_container(
            ["bash", "-c", f"exec 3<>/dev/tcp/127.0.0.1/{port} && echo OK"],
            nothrow=True,
        ),
        lambda r: "OK" in r,
        max_attempts=40,
        delay=0.5,
    )


def test_malformed_mget_reply(started_cluster):
    """An MGET reply whose element count differs from the request must be rejected.

    The result loop of StorageRedis was bounded by the reply length while the null map it
    indexes is sized from the request, so an over-long reply read and wrote past the end of the
    null map. A short reply produced fewer rows than keys, which breaks the row-per-key contract
    that IKeyValueEntity::getByKeys promises to a direct join.
    """
    tables = ("redis_fake_ok", "redis_fake_long", "redis_fake_short", "t_fake_left")
    for table in tables:
        drop_table(table)

    node.copy_file_to_container(
        os.path.join(SCRIPT_DIR, "fake_redis.py"), "/fake_redis.py"
    )
    start_fake_redis(FAKE_REDIS_PORT_OK, 0)
    start_fake_redis(FAKE_REDIS_PORT_LONG, 1)
    start_fake_redis(FAKE_REDIS_PORT_SHORT, -1)

    node.query(
        f"""
        CREATE TABLE redis_fake_ok (key String, value String)
        Engine=Redis('127.0.0.1:{FAKE_REDIS_PORT_OK}') PRIMARY KEY (key);

        CREATE TABLE redis_fake_long (key String, value String)
        Engine=Redis('127.0.0.1:{FAKE_REDIS_PORT_LONG}') PRIMARY KEY (key);

        CREATE TABLE redis_fake_short (key String, value String)
        Engine=Redis('127.0.0.1:{FAKE_REDIS_PORT_SHORT}') PRIMARY KEY (key);

        CREATE TABLE t_fake_left (k String) ENGINE = TinyLog;
        INSERT INTO t_fake_left VALUES ('a'), ('b');
        """
    )

    def direct_join(table):
        return node.query(
            f"SELECT key, value FROM (SELECT k AS key FROM t_fake_left) AS t "
            f"INNER JOIN {table} USING (key) ORDER BY key "
            f"SETTINGS join_algorithm = 'direct' FORMAT TSV"
        )

    # An endpoint that answers with one element per key still works, so a mock that never
    # listens or frames RESP wrongly reddens here instead of green-washing the arms below.
    assert TSV.toMat(direct_join("redis_fake_ok")) == [["a", "a"], ["b", "b"]]

    with pytest.raises(QueryRuntimeException) as long_join:
        direct_join("redis_fake_long")
    assert "INTERNAL_REDIS_ERROR" in str(long_join.value)
    assert "for MGET of" in str(long_join.value)

    with pytest.raises(QueryRuntimeException) as short_join:
        direct_join("redis_fake_short")
    assert "INTERNAL_REDIS_ERROR" in str(short_join.value)

    # The other caller of the reply reads it with no null map at all.
    with pytest.raises(QueryRuntimeException) as long_in:
        node.query("SELECT * FROM redis_fake_long WHERE key IN ('a', 'b')")
    assert "INTERNAL_REDIS_ERROR" in str(long_in.value)

    # A zero-element reply is a null array in Poco, not an empty one, so it is the isNull()
    # term of the guard that rejects it. One key against the delta = -1 endpoint produces it.
    with pytest.raises(QueryRuntimeException) as zero_in:
        node.query("SELECT * FROM redis_fake_short WHERE key IN ('a')")
    assert "INTERNAL_REDIS_ERROR" in str(zero_in.value)
    assert "returned 0 values" in str(zero_in.value)

    for table in tables:
        drop_table(table)


def test_full_scan_skips_missing_values(started_cluster):
    """A full scan must skip the keys MGET answers with nil, not stop at the first one.

    MGET answers by position, and a key SCAN listed can hold a non-string type or expire
    before the MGET runs, so a nil marks one absent value and not the end of the batch.
    """
    address = get_address_for_ch()
    table = "test_full_scan_missing"
    fake_table = "redis_fake_scan"

    client = get_redis_connection(db_id=4)
    client.flushdb()
    drop_table(table)
    drop_table(fake_table)

    # Redis alone decides in what order SCAN reports keys, so the mock pins the one thing this
    # test is about: a nil arriving before a key that still has a value.
    node.copy_file_to_container(
        os.path.join(SCRIPT_DIR, "fake_redis.py"), "/fake_redis.py"
    )
    start_fake_redis(FAKE_REDIS_PORT_SCAN, 0, "a,b,c b")

    node.query(
        f"""
        CREATE TABLE {fake_table} (key String, value String)
        Engine=Redis('127.0.0.1:{FAKE_REDIS_PORT_SCAN}') PRIMARY KEY (key);
        """
    )
    rows = node.query(f"SELECT key, value FROM {fake_table} ORDER BY key FORMAT TSV")
    assert TSV.toMat(rows) == [["a", "a"], ["c", "c"]]

    # The same thing on a real Redis, which answers nil for any key that holds another type.
    node.query(
        f"""
        CREATE TABLE {table}(k String, v String)
        Engine=Redis('{address}', 4, 'clickhouse') PRIMARY KEY (k);

        INSERT INTO {table} SELECT toString(number), toString(number) FROM numbers(16);
        """
    )

    # Control: every row is readable before the keys below exist, so a fixture that writes
    # nothing reddens here instead of leaving the assertion after it vacuous.
    assert int(node.query(f"SELECT uniqExact(k) FROM {table}")) == 16

    for i in range(16):
        client.rpush(f"list_{i}", "x")

    # SCAN can report a key twice while Redis rehashes and no read path dedupes the rows, so the
    # oracle here is the key set: a lost key is the defect, a repeated one is not.
    keys = node.query(f"SELECT DISTINCT k FROM {table} ORDER BY toUInt32(k) FORMAT TSV")
    assert TSV.toMat(keys) == [[str(i)] for i in range(16)]

    client.flushdb()
    drop_table(table)
    drop_table(fake_table)
