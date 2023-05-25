import redis
import pytest
from helpers.cluster import ClickHouseCluster

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
        host=node.name, port=started_cluster.redis_port, password="clickhouse", db=db_id
    )
    return client


def get_address():
    return node.name + started_cluster.redis_port


@pytest.mark.parametrize("started_cluster")
def test_storage_simple_select(started_cluster):
    client = get_redis_connection()
    address = get_address()

    data = {}
    for i in range(100):
        data['key{}'.format(i)] = 'value{}'.format(i)

    client.mset(data)

    # create table
    node.query(
        f"""
        CREATE TABLE test_storage_simple_select(
            k String, 
            v String
        ) Engine=Redis('{address}', 0, '','simple', 10)
        """
    )

    select_query = "SELECT k, v from test_storage_simple_select where k='0' FORMAT Values"
    assert (node.query(select_query) == "('0','0')")

    select_query = "SELECT * from test_storage_simple_select FORMAT Values"
    assert (len(node.query(select_query)) == 100)
    assert (node.query(select_query)[0] == "('0','0')")

