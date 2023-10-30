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
            """CREATE TABLE test_distinct ON CLUSTER test_two_shards (id UInt32, val String, name String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/test_distinct', '{replica}') ORDER BY id SETTINGS index_granularity=100;"""
        )

        node1.query(
            """CREATE TABLE test_distinct_all ON CLUSTER test_two_shards (id UInt32, val String, name String) ENGINE = Distributed(test_two_shards, default, test_distinct, rand());"""
        )

        yield cluster

    finally:
        cluster.shutdown()


def insert_data():
    node1.query("INSERT INTO test_distinct SELECT id,'AAA','BBB' FROM generateRandom('id Int16') LIMIT 10")
    node3.query("INSERT INTO test_distinct SELECT id,'BBB','CCC' FROM generateRandom('id Int16') LIMIT 11")
    node5.query("INSERT INTO test_distinct SELECT id,'AAA','CCC' FROM generateRandom('id Int16') LIMIT 12")
    node1.query("INSERT INTO test_distinct_all SELECT id,'AAA','BBB' FROM generateRandom('id Int16') LIMIT 13")
    node1.query("SYSTEM FLUSH DISTRIBUTED test_distinct_all")

def exec_query_compare_result(query_text):
    accurate_result = node1.query(query_text + " SETTINGS use_index_for_in_with_subqueries = 0")
    test_result = node1.query(query_text + " SETTINGS use_index_for_in_with_subqueries = 0, allow_experimental_query_coordination = 1")

    print(accurate_result)
    print(test_result)
    assert accurate_result == test_result

def test_query(started_cluster):
    insert_data()

    exec_query_compare_result("SELECT DISTINCT * FROM test_distinct_all ORDER BY id,val,name")

    exec_query_compare_result("SELECT DISTINCT ON (id,val) * FROM test_distinct_all ORDER BY id,val")
