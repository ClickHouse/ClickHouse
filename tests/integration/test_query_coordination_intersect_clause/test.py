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
            """CREATE TABLE intersect_1 ON CLUSTER test_two_shards (id UInt32, val String, name String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/intersect_1', '{replica}') ORDER BY id SETTINGS index_granularity=100;"""
        )

        node1.query(
            """CREATE TABLE intersect_all_1 ON CLUSTER test_two_shards (id UInt32, val String, name String) ENGINE = Distributed(test_two_shards, default, intersect_1, rand());"""
        )

        node1.query(
            """CREATE TABLE intersect_2 ON CLUSTER test_two_shards (id UInt32, text String, scores UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/intersect_2', '{replica}') ORDER BY id SETTINGS index_granularity=100;"""
        )

        node1.query(
            """CREATE TABLE intersect_all_2 ON CLUSTER test_two_shards (id UInt32, text String, scores UInt32) ENGINE = Distributed(test_two_shards, default, intersect_2, rand());"""
        )

        yield cluster

    finally:
        cluster.shutdown()

def insert_data():
    node1.query("INSERT INTO intersect_1 SELECT id,'AAA','BBB' FROM generateRandom('id Int16') LIMIT 200")
    node3.query("INSERT INTO intersect_1 SELECT id,'BBB','CCC' FROM generateRandom('id Int16') LIMIT 300")
    node5.query("INSERT INTO intersect_1 SELECT id,'AAA','CCC' FROM generateRandom('id Int16') LIMIT 400")
    node1.query("INSERT INTO intersect_all_1 SELECT id,'AAA','BBB' FROM generateRandom('id Int16') LIMIT 500")
    node1.query("SYSTEM FLUSH DISTRIBUTED intersect_all_1")

    node1.query("INSERT INTO intersect_2 SELECT id,'AAA',100 FROM generateRandom('id Int16') LIMIT 200")
    node3.query("INSERT INTO intersect_2 SELECT id,'BBB',95 FROM generateRandom('id Int16') LIMIT 300")
    node5.query("INSERT INTO intersect_2 SELECT id,'AAA',95 FROM generateRandom('id Int16') LIMIT 400")
    node1.query("INSERT INTO intersect_all_2 SELECT id,'AAA',90 FROM generateRandom('id Int16') LIMIT 500")
    node1.query("SYSTEM FLUSH DISTRIBUTED intersect_all_2")

def exec_query_compare_result(query_text):
    accurate_result = node1.query(query_text)
    test_result = node1.query(query_text + " SETTINGS allow_experimental_query_coordination = 1")

    print(accurate_result)
    print(test_result)
    assert accurate_result == test_result

def test_query(started_cluster):
    insert_data()

    exec_query_compare_result("SELECT id FROM (SELECT id FROM intersect_all_1 ORDER BY id) INTERSECT SELECT id FROM (SELECT id FROM intersect_all_2 ORDER BY id) ORDER BY id")

    exec_query_compare_result("SELECT id FROM (SELECT id FROM intersect_all_1 ORDER BY id) EXCEPT SELECT id FROM (SELECT id FROM intersect_all_2 ORDER BY id) ORDER BY id")
