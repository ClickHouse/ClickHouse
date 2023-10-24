import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance("node1", main_configs=["configs/remote_servers.xml"], with_zookeeper=True, macros={"shard": 1, "replica": 1},)
node2 = cluster.add_instance("node2", main_configs=["configs/remote_servers.xml"], with_zookeeper=True, macros={"shard": 2, "replica": 1},)
node3 = cluster.add_instance("node3", main_configs=["configs/remote_servers.xml"], with_zookeeper=True, macros={"shard": 3, "replica": 1},)
# test_two_shards

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        node1.query(
            """CREATE TABLE wf_partition_local ON CLUSTER test_two_shards (part_key UInt64, value UInt64, order UInt64) ENGINE = Memory;"""
        )

        node1.query(
            """CREATE TABLE wf_partition ON CLUSTER test_two_shards (part_key UInt64, value UInt64, order UInt64) ENGINE = Distributed(test_two_shards, default, wf_partition_local, rand());"""
        )

        yield cluster

    finally:
        cluster.shutdown()


def test_query(started_cluster):
    node1.query("INSERT INTO wf_partition FORMAT Values (1,1,1), (1,2,2), (1,3,3), (2,0,0), (3,0,0)")

    node1.query("SYSTEM FLUSH DISTRIBUTED wf_partition")

    node1.query("SELECT part_key, value, order, groupArray(value) OVER (PARTITION BY part_key) AS frame_values FROM wf_partition ORDER BY part_key ASC, value ASC")
