import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance('node1', with_zookeeper=True)
node2 = cluster.add_instance('node2', with_zookeeper=True)

@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def test_attach_detach(start_cluster):

    node1.query("""
        CREATE TABLE test (key UInt64)
        ENGINE = ReplicatedMergeTree('/clickhouse/test', '1')
        ORDER BY tuple()
        SETTINGS index_granularity_bytes = 0""")

    node1.query("INSERT INTO test VALUES (1), (2)")

    node2.query("""
        CREATE TABLE test (key UInt64)
        ENGINE = ReplicatedMergeTree('/clickhouse/test', '2')
        ORDER BY tuple()""")

    node2.query("INSERT INTO test VALUES (3), (4)")

    node1.query("SYSTEM SYNC REPLICA test")
    node2.query("SYSTEM SYNC REPLICA test")

    assert node1.query("SELECT COUNT() FROM  test") == "4\n"
    assert node2.query("SELECT COUNT() FROM  test") == "4\n"

    node1.query("DETACH TABLE test")
    node2.query("DETACH TABLE test")

    node1.query("ATTACH TABLE test")
    node2.query("ATTACH TABLE test")

    assert node1.query("SELECT COUNT() FROM  test") == "4\n"
    assert node2.query("SELECT COUNT() FROM  test") == "4\n"
