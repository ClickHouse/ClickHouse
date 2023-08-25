import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1", main_configs=["configs/distributed_servers.xml"], with_zookeeper=True
)
node2 = cluster.add_instance(
    "node2", with_zookeeper=True, main_configs=["configs/distributed_servers.xml"]
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        for node in (node1, node2):
            node.query(
                """
                CREATE DATABASE test;
                CREATE TABLE test.local_table(id UInt32, val String) ENGINE = MergeTree ORDER BY id
                """
            )

        node1.query("INSERT INTO test.local_table VALUES (1, 'node1')")
        node2.query("INSERT INTO test.local_table VALUES (2, 'node2')")

        node1.query(
            "CREATE TABLE test.distributed(id UInt32, val String) ENGINE = Distributed(test_cluster, test, local_table)"
        )
        node2.query(
            "CREATE TABLE test.distributed(id UInt32, val String) ENGINE = Distributed(test_cluster, test, local_table)"
        )

        yield cluster

    finally:
        cluster.shutdown()


def test_truncate_database_distributed(started_cluster):
    query1 = "SELECT count() FROM test.distributed WHERE (id, val) IN ((1, 'node1'), (2, 'a'), (3, 'b'))"
    query2 = "SELECT sum((id, val) IN ((1, 'node1'), (2, 'a'), (3, 'b'))) FROM test.distributed"
    assert node1.query(query1) == "1\n"
    assert node1.query(query2) == "1\n"
    assert node2.query(query1) == "1\n"
    assert node2.query(query2) == "1\n"
    assert node2.query("SHOW DATABASES LIKE 'test'") == "test\n"
    node1.query("TRUNCATE DATABASE test ON CLUSTER test_cluster SYNC")
    assert node2.query("SHOW TABLES FROM test") == ""
