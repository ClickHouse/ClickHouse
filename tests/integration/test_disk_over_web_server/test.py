import pytest

from helpers.cluster import ClickHouseCluster


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance("node", main_configs=["configs/storage_conf.xml"], with_nginx=True)
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_insert_select(cluster):
    node = cluster.instances["node"]
    node.query("""
        CREATE TABLE test1 (id Int32)
        ENGINE = MergeTree() ORDER BY id
        SETTINGS storage_policy = 'web';
    """)

    node.query("INSERT INTO test1 SELECT number FROM numbers(100)")
    result = node.query("SELECT count() FROM test1")
    assert(int(result) == 100)

    node.query("DETACH TABLE test1")
    node.query("ATTACH TABLE test1")
    result = node.query("SELECT count() FROM test1")
    assert(int(result) == 100)

    node = cluster.instances["node"]
    node.query("""
        CREATE TABLE test2 (id Int32)
        ENGINE = MergeTree() ORDER BY id
        SETTINGS storage_policy = 'web';
    """)

    node.query("INSERT INTO test2 SELECT number FROM numbers(500000)")
    result = node.query("SELECT id FROM test2 ORDER BY id")
    expected = node.query("SELECT number FROM numbers(500000)")
    assert(result == expected)
    node.query("INSERT INTO test2 SELECT number FROM numbers(500000, 500000)")
    node.query("DETACH TABLE test2")
    node.query("ATTACH TABLE test2")
    node.query("INSERT INTO test2 SELECT number FROM numbers(1000000, 500000)")
    result = node.query("SELECT count() FROM test2")
    assert(int(result) == 1500000)
    result = node.query("SELECT id FROM test2 WHERE id % 100 = 0 ORDER BY id")
    assert(result == node.query("SELECT number FROM numbers(1500000) WHERE number % 100 = 0 ORDER BY number"))
