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
            """CREATE TABLE local_table(id UInt32, val String) ON CLUSTER test_two_shards ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/local_table', '{replica}') ORDER BY id SETTINGS index_granularity=1000;"""
        )

        node1.query(
            """CREATE TABLE distributed_table(id UInt32, val String) ON CLUSTER test_two_shards ENGINE = Distributed(test_two_shards, default, local_table, rund());"""
        )

        yield cluster

    finally:
        cluster.shutdown()


def test_aggregate_query(started_cluster):
    node1.query("INSERT INTO distributed_table SELECT * FROM generateRandom('id Int, val String') LIMIT 20000")

    print("local table select:")
    r = node1.query("SELECT sum(id),val FROM local_table GROUP BY val SETTINGS allow_experimental_fragment = 1, allow_experimental_analyzer = 0")
    print(r)

    print("distribute table select:")
    rr = node1.query("SELECT sum(id),val FROM distributed_table GROUP BY val")
    print(rr)

