import datetime
import struct
import sys

import pytest
import redis

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
        host="localhost", port=cluster.redis_port, password="clickhouse", db=db_id
    )
    return client


def get_address_for_ch():
    return cluster.redis_host + ":6379"


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

    data = {}
    for i in range(100):
        packed = serialize_binary_for_string(str(i))
        data[packed] = packed

    client.mset(data)
    client.close()

    response = TSV.toMat(
        node.query(
            f"""
            SELECT 
                key, value 
            FROM 
                redis('{address}', 'key', 'key String, value String', 0, 'clickhouse', 10) 
            WHERE 
                key='0' 
            FORMAT TSV
            """
        )
    )

    assert len(response) == 1
    assert response[0] == ["0", "0"]

    response = TSV.toMat(
        node.query(
            f"""
            SELECT 
                * 
            FROM 
                redis('{address}', 'key', 'key String, value String', 0, 'clickhouse', 10) 
            ORDER BY 
                key 
            FORMAT TSV
            """
        )
    )

    assert len(response) == 100
    assert response[0] == ["0", "0"]


def test_create_table(started_cluster):
    client = get_redis_connection()
    address = get_address_for_ch()

    # clean all
    client.flushall()
    client.close()

    node.query(
        f"""
        SELECT 
            *
        FROM 
            redis('{address}', 'k', 'k String, v UInt32', 0, 'clickhouse', 10) 
        """
    )

    # illegal data type
    with pytest.raises(QueryRuntimeException):
        node.query(
            f"""
            SELECT 
                *
            FROM 
                redis('{address}', 'k', 'k not_exist_type, v String', 0, 'clickhouse', 10) 
            """
        )

    # illegal key
    with pytest.raises(QueryRuntimeException):
        node.query(
            f"""
            SELECT 
                *
            FROM 
                redis('{address}', 'not_exist_key', 'k not_exist_type, v String', 0, 'clickhouse', 10) 
            """
        )


def test_data_type(started_cluster):
    client = get_redis_connection()
    address = get_address_for_ch()

    # string
    client.flushall()
    value = serialize_binary_for_string("0")
    client.set(value, value)

    response = TSV.toMat(
        node.query(
            f"""
            SELECT
                *
            FROM
                redis('{address}', 'k', 'k String, v String', 0, 'clickhouse', 10)
            WHERE
                k='0'
            FORMAT TSV
            """
        )
    )

    assert len(response) == 1
    assert response[0] == ["0", "0"]

    # number
    client.flushall()
    value = serialize_binary_for_uint32(0)
    client.set(value, value)

    response = TSV.toMat(
        node.query(
            f"""
            SELECT
                *
            FROM
                redis('{address}', 'k', 'k UInt32, v UInt32', 0, 'clickhouse', 10)
            WHERE
                k=0
            FORMAT TSV
            """
        )
    )

    assert len(response) == 1
    assert response[0] == ["0", "0"]

    # datetime
    client.flushall()
    # clickhouse store datatime as uint32 in internal
    dt = datetime.datetime(2023, 6, 1, 0, 0, 0)
    seconds_since_epoch = dt.timestamp()
    value = serialize_binary_for_uint32(int(seconds_since_epoch))
    client.set(value, value)

    response = TSV.toMat(
        node.query(
            f"""
            SELECT
                *
            FROM
                redis('{address}', 'k', 'k DateTime, v DateTime', 0, 'clickhouse', 10)
            WHERE
                k='2023-06-01 00:00:00'
            FORMAT TSV
            """
        )
    )

    assert len(response) == 1
    assert response[0] == ["2023-06-01 00:00:00", "2023-06-01 00:00:00"]
