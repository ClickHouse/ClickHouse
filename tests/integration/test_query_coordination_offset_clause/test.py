import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance("node1", main_configs=["configs/remote_servers.xml"], with_zookeeper=True, macros={"shard": 1, "replica": 1},)
node2 = cluster.add_instance("node2", main_configs=["configs/remote_servers.xml"], with_zookeeper=True, macros={"shard": 1, "replica": 2},)
node3 = cluster.add_instance("node3", main_configs=["configs/remote_servers.xml"], with_zookeeper=True, macros={"shard": 2, "replica": 1},)
node4 = cluster.add_instance("node4", main_configs=["configs/remote_servers.xml"], with_zookeeper=True, macros={"shard": 2, "replica": 2},)
node5 = cluster.add_instance("node5", main_configs=["configs/remote_servers.xml"], with_zookeeper=True, macros={"shard": 3, "replica": 1},)
node6 = cluster.add_instance("node6", main_configs=["configs/remote_servers.xml"], with_zookeeper=True, macros={"shard": 3, "replica": 2},)
# test_two_shards

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        node1.query(
            """CREATE TABLE test_fetch_local ON CLUSTER test_two_shards (a UInt32, b UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/test_fetch_local', '{replica}') ORDER BY id SETTINGS index_granularity=100;"""
        )

        node1.query(
            """CREATE TABLE test_fetch ON CLUSTER test_two_shards (a UInt32, b UInt32) ENGINE = Distributed(test_two_shards, default, test_fetch_local, rand());"""
        )

        yield cluster

    finally:
        cluster.shutdown()


def test_query(started_cluster):
    node1.query("INSERT INTO test_fetch VALUES (1,1), (2,1), (3,4), (1,3), (5,4), (0,6), (5,7)")

    node1.query("SYSTEM FLUSH DISTRIBUTED test_fetch")

    node1.query("SELECT * FROM test_fetch ORDER BY a OFFSET 3 ROW FETCH FIRST 3 ROWS ONLY")

    node1.query("SELECT * FROM test_fetch ORDER BY a OFFSET 3 ROW FETCH FIRST 3 ROWS WITH TIES")
