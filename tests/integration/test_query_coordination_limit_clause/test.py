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

def exec_query_compare_result(query_text):
    accurate_result = node1.query(query_text)
    test_result = node1.query(query_text + " SETTINGS allow_experimental_query_coordination = 1")

    assert accurate_result == test_result

def test_query(started_cluster):
    node1.query("INSERT INTO distributed_table SELECT number%50, '123', 'test' FROM numbers(200)")

    node1.query("SYSTEM FLUSH DISTRIBUTED distributed_table")

    exec_query_compare_result("SELECT * FROM distributed_table ORDER BY id LIMIT 0,5")

    exec_query_compare_result("SELECT * FROM distributed_table ORDER BY id LIMIT 0,5 WITH TIES")

    exec_query_compare_result("SELECT * FROM distributed_table ORDER BY id LIMIT 1,5")

    # exec_query_compare_result("SELECT * FROM distributed_table ORDER BY id LIMIT 1,5 WITH TIES")
