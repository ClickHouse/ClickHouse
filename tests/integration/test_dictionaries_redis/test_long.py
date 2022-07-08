import pytest
from helpers.cluster import ClickHouseCluster
import redis

cluster = ClickHouseCluster(__file__, name="long")

node = cluster.add_instance("node", with_redis=True)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        N = 1000
        client = redis.Redis(
            host="localhost", port=cluster.redis_port, password="clickhouse", db=0
        )
        client.flushdb()
        for i in range(N):
            client.hset("2020-10-10", i, i)

        node.query(
            """
            CREATE DICTIONARY redis_dict
            (
                date String,
                id UInt64,
                value UInt64
            )
            PRIMARY KEY date, id
            SOURCE(REDIS(HOST '{}' PORT 6379 STORAGE_TYPE 'hash_map' DB_INDEX 0 PASSWORD 'clickhouse'))
            LAYOUT(COMPLEX_KEY_DIRECT())
            """.format(
                cluster.redis_host
            )
        )

        node.query(
            """
            CREATE TABLE redis_dictionary_test
            (
                date Date,
                id UInt64
            )
            ENGINE = MergeTree ORDER BY id"""
        )

        node.query(
            "INSERT INTO default.redis_dictionary_test SELECT '2020-10-10', number FROM numbers(1000000)"
        )

        yield cluster

    finally:
        cluster.shutdown()


def test_redis_dict_long(start_cluster):
    assert (
        node.query("SELECT count(), uniqExact(date), uniqExact(id) FROM redis_dict")
        == "1000\t1\t1000\n"
    )
    assert (
        node.query(
            "SELECT count(DISTINCT dictGet('redis_dict', 'value', tuple(date, id % 1000))) FROM redis_dictionary_test"
        )
        == "1000\n"
    )
