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
            """CREATE TABLE local_table ON CLUSTER test_two_shards (id UInt32, val String, name String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/local_table', '{replica}') ORDER BY id SETTINGS index_granularity=100;"""
        )

        node1.query(
            """CREATE TABLE distributed_table ON CLUSTER test_two_shards (id UInt32, val String, name String) ENGINE = Distributed(test_two_shards, default, local_table, rand());"""
        )

        yield cluster

    finally:
        cluster.shutdown()


def test_aggregate_query(started_cluster):
    node1.query("INSERT INTO distributed_table SELECT id,'123','test' FROM generateRandom('id Int16') LIMIT 10000")
    node1.query("INSERT INTO distributed_table SELECT id,'124','test1' FROM generateRandom('id Int16') LIMIT 10000")

    node1.query("SYSTEM FLUSH DISTRIBUTED distributed_table")

    node1.query("SELECT val, count() * 10 AS nums FROM distributed_table SAMPLE 0.1 WHERE id > 34 GROUP BY val ORDER BY nums DESC LIMIT 1000")

    node1.query("SELECT val, count() * 10 AS nums FROM distributed_table SAMPLE 0.1 WHERE id > 34 GROUP BY val ORDER BY nums DESC LIMIT 1000 SETTINGS allow_experimental_query_coordination = 1")

    node1.query("SELECT sum(id * _sample_factor) AS nums FROM distributed_table SAMPLE 1000")

    node1.query("SELECT sum(_sample_factor) AS nums FROM distributed_table SAMPLE 1000")

    node1.query("SELECT avg(id) AS nums FROM distributed_table SAMPLE 1000")
