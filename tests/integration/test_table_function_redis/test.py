import time

import redis
import pytest

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


def test_storage_simple(started_cluster):
    client = get_redis_connection()
    address = get_address_for_ch()

    # clean all
    client.flushall()

    data = {}
    for i in range(100):
        data[str(i)] = str(i)

    client.mset(data)
    client.close()

    response = TSV.toMat(node.query(
        f"""
        SELECT 
            key, value 
        FROM 
            redis('{address}', 0, 'clickhouse') 
        WHERE 
            key='0' 
        FORMAT TSV
        """))

    assert (len(response) == 1)
    assert (response[0] == ['0', '0'])

    response = TSV.toMat(node.query(
        f"""
        SELECT 
            * 
        FROM 
            redis('{address}', 0, 'clickhouse') 
        ORDER BY 
            key 
        FORMAT TSV
        """))

    assert (len(response) == 100)
    assert (response[0] == ['0', '0'])


def test_storage_hash_map(started_cluster):
    client = get_redis_connection()
    address = get_address_for_ch()

    # clean all
    client.flushall()

    key = 'k'
    data = {}
    for i in range(100):
        data[str(i)] = str(i)

    client.hset(key, mapping=data)
    client.close()

    response = TSV.toMat(node.query(
        f"""
        SELECT 
            key, field, value
        FROM 
            redis('{address}', 0, 'clickhouse','hash_map') 
        WHERE 
            field='0' 
        FORMAT TSV
        """))

    assert (len(response) == 1)
    assert (response[0] == ['k', '0', '0'])

    response = TSV.toMat(node.query(
        f"""
        SELECT 
            *
        FROM 
            redis('{address}', 0, 'clickhouse','hash_map') 
        ORDER BY 
            field 
        FORMAT TSV
        """))

    assert (len(response) == 100)
    assert (response[0] == ['k', '0', '0'])


def test_customized_table_structure(started_cluster):
    address = get_address_for_ch()

    node.query(
        f"""
        SELECT 
            *
        FROM 
            redis('{address}', 0, 'clickhouse', "simple", 10, "k String, v UInt8") 
        """)

    node.query(
        f"""
            SELECT 
                *
            FROM 
                redis('{address}', 0, 'clickhouse', "hash_map", 10, "k String, f UInt8, v String") 
            """)

    # illegal columns
    with pytest.raises(QueryRuntimeException):
        node.query(
            f"""
            SELECT 
                *
            FROM 
                redis('{address}', 0, 'clickhouse', "hash_map", 10, "k String, v String") 
            """)

    # illegal data type
    with pytest.raises(QueryRuntimeException):
        node.query(
            f"""
            SELECT 
                *
            FROM 
                redis('{address}', 0, 'clickhouse', "simple", 10, "k Ss, v String") 
            """)


def test_data_type(started_cluster):
    client = get_redis_connection()
    address = get_address_for_ch()

    # string
    client.flushall()
    client.set('0', '0')

    response = TSV.toMat(node.query(
        f"""
        SELECT
            *
        FROM
            redis('{address}', 0, 'clickhouse', 'simple', 10, "k String, v UInt8")
        WHERE
            k='0'
        FORMAT TSV
        """))

    assert (len(response) == 1)
    assert (response[0] == ['0', '0'])

    # number
    client.flushall()
    client.set('0', '0')

    response = TSV.toMat(node.query(
        f"""
        SELECT
            *
        FROM
            redis('{address}', 0, 'clickhouse', 'simple', 10, "k UInt8, v UInt8")
        WHERE
            k=0
        FORMAT TSV
        """))

    assert (len(response) == 1)
    assert (response[0] == ['0', '0'])

    # datetime
    client.flushall()
    client.set('2023-06-01 00:00:00', '0')

    response = TSV.toMat(node.query(
        f"""
        SELECT
            *
        FROM
            redis('{address}', 0, 'clickhouse', 'simple', 10, "k DateTime, v UInt8")
        WHERE
            k='2023-06-01 00:00:00'
        FORMAT TSV
        """))

    # TODO open
    # assert (len(response) == 1)
    # assert (response[0] == ['2023-06-01 00:00:00', '0'])
