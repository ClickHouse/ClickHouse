import time

## sudo -H pip install redis
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


def drop_table(table):
    node.query(f"DROP TABLE IF EXISTS {table} SYNC");


def test_storage_simple_select(started_cluster):
    client = get_redis_connection()
    address = get_address_for_ch()

    # clean all
    client.flushall()
    drop_table('test_storage_simple_select')

    data = {}
    for i in range(100):
        data[str(i)] = str(i)

    client.mset(data)
    client.close()

    # create table
    node.query(
        f"""
        CREATE TABLE test_storage_simple_select(
            k String, 
            v UInt32
        ) Engine=Redis('{address}', 0, 'clickhouse')
        """
    )

    response = TSV.toMat(node.query("SELECT k, v FROM test_storage_simple_select WHERE k='0' FORMAT TSV"))
    assert (len(response) == 1)
    assert (response[0] == ['0', '0'])

    response = TSV.toMat(node.query("SELECT * FROM test_storage_simple_select ORDER BY k FORMAT TSV"))
    assert (len(response) == 100)
    assert (response[0] == ['0', '0'])


def test_storage_hash_map_select(started_cluster):
    client = get_redis_connection()
    address = get_address_for_ch()

    # clean all
    client.flushall()
    drop_table('test_storage_hash_map_select')

    key = 'k'
    data = {}
    for i in range(100):
        data[str(i)] = str(i)

    client.hset(key, mapping=data)
    client.close()

    # create table
    node.query(
        f"""
        CREATE TABLE test_storage_hash_map_select(
            k String,
            f String, 
            v UInt32
        ) Engine=Redis('{address}', 0, 'clickhouse','hash_map')
        """
    )

    response = TSV.toMat(node.query("SELECT k, f, v FROM test_storage_hash_map_select WHERE f='0' FORMAT TSV"))
    assert (len(response) == 1)
    assert (response[0] == ['k', '0', '0'])

    response = TSV.toMat(node.query("SELECT * FROM test_storage_hash_map_select ORDER BY f FORMAT TSV"))
    assert (len(response) == 100)
    assert (response[0] == ['k', '0', '0'])


def test_create_table(started_cluster):
    address = get_address_for_ch()

    # simple creation
    drop_table('test_create_table')
    node.query(
        f"""
        CREATE TABLE test_create_table(
            k String,
            v UInt32
        ) Engine=Redis('{address}')
        """
    )

    # simple creation with full engine args
    drop_table('test_create_table')
    node.query(
        f"""
        CREATE TABLE test_create_table(
            k String,
            v UInt32
        ) Engine=Redis('{address}', 0, 'clickhouse','simple', 10)
        """
    )

    drop_table('test_create_table')
    node.query(
        f"""
        CREATE TABLE test_create_table(
            k String,
            f String,
            v UInt32
        ) Engine=Redis('{address}', 0, 'clickhouse','hash_map', 10)
        """
    )

    # illegal columns
    drop_table('test_create_table')
    with pytest.raises(QueryRuntimeException):
        node.query(
            f"""
            CREATE TABLE test_create_table(
                k String,
                f String,
                v UInt32
            ) Engine=Redis('{address}', 0, 'clickhouse','simple', 10)
            """
        )

    drop_table('test_create_table')
    with pytest.raises(QueryRuntimeException):
        node.query(
            f"""
            CREATE TABLE test_create_table(
                k String,
                f String,
                v UInt32,
                n UInt32
            ) Engine=Redis('{address}', 0, 'clickhouse','hash_map', 10)
            """
        )

    # illegal storage type
    drop_table('test_create_table')
    with pytest.raises(QueryRuntimeException):
        node.query(
            f"""
            CREATE TABLE test_create_table(
                k String,
                v UInt32
            ) Engine=Redis('{address}', 0, 'clickhouse','not_exist', 10)
            """
        )


