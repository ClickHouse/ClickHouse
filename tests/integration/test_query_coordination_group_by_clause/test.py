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
            """CREATE TABLE test_aggregate ON CLUSTER test_two_shards (id UInt32, val String, name String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/test_aggregate', '{replica}') ORDER BY id SETTINGS index_granularity=100;"""
        )

        node1.query(
            """CREATE TABLE test_aggregate_all ON CLUSTER test_two_shards (id UInt32, val String, name String) ENGINE = Distributed(test_two_shards, default, test_aggregate, rand());"""
        )

        insert_data()

        yield cluster

    finally:
        cluster.shutdown()


def insert_data():
    node1.query("INSERT INTO test_aggregate SELECT id,'AAA','BBB' FROM generateRandom('id Int16') LIMIT 200")
    node3.query("INSERT INTO test_aggregate SELECT id,'BBB','CCC' FROM generateRandom('id Int16') LIMIT 300")
    node5.query("INSERT INTO test_aggregate SELECT id,'AAA','CCC' FROM generateRandom('id Int16') LIMIT 400")
    node1.query("INSERT INTO test_aggregate_all SELECT id,'AAA','BBB' FROM generateRandom('id Int16') LIMIT 500")
    node1.query("SYSTEM FLUSH DISTRIBUTED test_aggregate_all")


def exec_query_compare_result(query_text):
    accurate_result = node1.query(query_text)
    test_result = node1.query(query_text + " SETTINGS allow_experimental_query_coordination = 1")
    assert accurate_result == test_result


def test_simple_group_by(started_cluster):
    exec_query_compare_result("SELECT id, val, name FROM test_aggregate_all GROUP BY id, val, name ORDER BY id, val, name")

    exec_query_compare_result("SELECT sum(id), val, name FROM test_aggregate_all GROUP BY val, name ORDER BY val, name")

    exec_query_compare_result("SELECT sum(id), val FROM test_aggregate_all GROUP BY val ORDER BY val")

    exec_query_compare_result("SELECT sum(id) as ids, val, name FROM test_aggregate_all GROUP BY val, name HAVING ids > 10000 ORDER BY val, name")

    exec_query_compare_result("SELECT sum(id), substring(val, 1,2) as v FROM test_aggregate_all GROUP BY substring(val, 1,2) ORDER BY v")


def test_grouping_set(started_cluster):
    exec_query_compare_result("SELECT sum(id), val, name FROM test_aggregate_all GROUP BY GROUPING SETS((name,val),(name),(val),()) ORDER BY val, name")


def test_group_with_total(started_cluster):
    exec_query_compare_result("SELECT sum(id), val, name FROM test_aggregate_all GROUP BY val, name WITH totals ORDER BY val, name")


def test_rollup(started_cluster):
    exec_query_compare_result("SELECT sum(id), val, name FROM test_aggregate_all GROUP BY val, name WITH rollup  ORDER BY val, name")


def test_cube(started_cluster):
    exec_query_compare_result("SELECT sum(id), val, name FROM test_aggregate_all GROUP BY val, name WITH cube  ORDER BY val, name")

