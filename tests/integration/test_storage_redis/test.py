import time

## sudo -H pip install redis
import redis
import pytest
import struct
import sys

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

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
        host='localhost', port=cluster.redis_port, password="clickhouse", db=db_id
    )
    return client


def get_address_for_ch():
    return cluster.redis_host + ':6379'


def drop_table(table):
    node.query(f"DROP TABLE IF EXISTS {table} SYNC");


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
        buf += (bytes([byte]))
        length >>= 7
        if not length:
            break
    # write data
    buf += x.encode('utf-8')
    return bytes(buf)


# see SerializationNumber.serializeBinary
def serialize_binary_for_uint32(x):
    buf = bytearray()
    packed_num = struct.pack('I', x)
    buf += packed_num
    if sys.byteorder != 'little':
        buf.reverse()
    return bytes(buf)


def test_simple_select(started_cluster):
    client = get_redis_connection()
    address = get_address_for_ch()

    # clean all
    client.flushall()
    drop_table('test_simple_select')

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

    response = TSV.toMat(node.query("SELECT k, v FROM test_simple_select WHERE k='0' FORMAT TSV"))
    assert (len(response) == 1)
    assert (response[0] == ['0', '0'])

    response = TSV.toMat(node.query("SELECT * FROM test_simple_select ORDER BY k FORMAT TSV"))
    assert (len(response) == 100)
    assert (response[0] == ['0', '0'])


def test_select_int(started_cluster):
    client = get_redis_connection()
    address = get_address_for_ch()

    # clean all
    client.flushall()
    drop_table('test_select_int')

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

    response = TSV.toMat(node.query("SELECT k, v FROM test_select_int WHERE k=0 FORMAT TSV"))
    assert (len(response) == 1)
    assert (response[0] == ['0', '0'])

    response = TSV.toMat(node.query("SELECT * FROM test_select_int ORDER BY k FORMAT TSV"))
    assert (len(response) == 100)
    assert (response[0] == ['0', '0'])


def test_create_table(started_cluster):
    address = get_address_for_ch()

    # simple creation
    drop_table('test_create_table')
    node.query(
        f"""
        CREATE TABLE test_create_table(
            k String,
            v UInt32
        ) Engine=Redis('{address}') PRIMARY KEY (k)
        """
    )

    # simple creation with full engine args
    drop_table('test_create_table')
    node.query(
        f"""
        CREATE TABLE test_create_table(
            k String,
            v UInt32
        ) Engine=Redis('{address}', 0, 'clickhouse', 10) PRIMARY KEY (k)
        """
    )

    drop_table('test_create_table')
    node.query(
        f"""
        CREATE TABLE test_create_table(
            k String,
            f String,
            v UInt32
        ) Engine=Redis('{address}', 0, 'clickhouse', 10) PRIMARY KEY (k)
        """
    )

    drop_table('test_create_table')
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

    drop_table('test_create_table')
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

