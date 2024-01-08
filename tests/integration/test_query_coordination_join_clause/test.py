import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance("node1", main_configs=["configs/remote_servers.xml"], with_zookeeper=True,
                             macros={"shard": 1, "replica": 1}, )
node2 = cluster.add_instance("node2", main_configs=["configs/remote_servers.xml"], with_zookeeper=True,
                             macros={"shard": 1, "replica": 2}, )
node3 = cluster.add_instance("node3", main_configs=["configs/remote_servers.xml"], with_zookeeper=True,
                             macros={"shard": 2, "replica": 1}, )
node4 = cluster.add_instance("node4", main_configs=["configs/remote_servers.xml"], with_zookeeper=True,
                             macros={"shard": 2, "replica": 2}, )
node5 = cluster.add_instance("node5", main_configs=["configs/remote_servers.xml"], with_zookeeper=True,
                             macros={"shard": 3, "replica": 1}, )
node6 = cluster.add_instance("node6", main_configs=["configs/remote_servers.xml"], with_zookeeper=True,
                             macros={"shard": 3, "replica": 2}, )


# test_two_shards

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        node1.query(
            """CREATE TABLE local_table_1 ON CLUSTER test_two_shards (id UInt32, val String, name String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/local_table_1', '{replica}') ORDER BY id SETTINGS index_granularity=100;"""
        )

        node1.query(
            """CREATE TABLE table_1 ON CLUSTER test_two_shards (id UInt32, val String, name String) ENGINE = Distributed(test_two_shards, default, local_table_1, rand());"""
        )

        node1.query(
            """CREATE TABLE local_table_2 ON CLUSTER test_two_shards (id UInt32, text String, scores UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/local_table_2', '{replica}') ORDER BY id SETTINGS index_granularity=100;"""
        )

        node1.query(
            """CREATE TABLE table_2 ON CLUSTER test_two_shards (id UInt32, text String, scores UInt32) ENGINE = Distributed(test_two_shards, default, local_table_2, rand());"""
        )

        insert_data()

        yield cluster

    finally:
        cluster.shutdown()


def insert_data():
    node1.query("INSERT INTO table_1 SELECT id,'123','test' FROM generateRandom('id Int16') LIMIT 60")
    node1.query("INSERT INTO table_1 SELECT id,'234','test1' FROM generateRandom('id Int16') LIMIT 50")

    node1.query("INSERT INTO table_2 SELECT id,'123',10 FROM generateRandom('id Int16') LIMIT 50")
    node1.query("INSERT INTO table_2 SELECT id,'234',12 FROM generateRandom('id Int16') LIMIT 60")

    node1.query("SYSTEM FLUSH DISTRIBUTED table_1")
    node1.query("SYSTEM FLUSH DISTRIBUTED table_2")


def exec_query_compare_result(query_text):
    accurate_result = node1.query(query_text + " SETTINGS distributed_product_mode = 'global'")
    test_result = node1.query(query_text + " SETTINGS allow_experimental_query_coordination = 1")

    print("accurate_result: " + accurate_result)
    print("test_result: " + test_result)

    assert accurate_result == test_result


# SELECT <expr_list>
# FROM <left_table>
# [GLOBAL] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER|SEMI|ANTI|ANY|ASOF] JOIN <right_table>
# (ON <expr_list>)|(USING <column_list>) ...

def test_self_join(started_cluster):
    exec_query_compare_result(
        "SELECT name, text FROM table_1 as t1 INNER JOIN table_1 as t2 ON ti.id = t2.id AND startsWith(t1.text, '123') ORDER BY name, text")


def test_inner_join(started_cluster):
    exec_query_compare_result(
        "SELECT name, text FROM table_1 INNER ANY JOIN table_2 ON table_1.id = table_2.id AND startsWith(table_2.text, '123') ORDER BY name, text")

    exec_query_compare_result(
        "SELECT name, text FROM table_1 INNER ALL JOIN table_2 ON table_1.id = table_2.id AND startsWith(table_2.text, '123') ORDER BY name, text")


def test_outer_join(started_cluster):
    exec_query_compare_result(
        "SELECT name, text FROM table_1 RIGHT OUTER JOIN table_2 ON table_1.id = table_2.id AND startsWith(table_2.text, '234') ORDER BY name, text")

    exec_query_compare_result(
        "SELECT name, text FROM table_1 RIGHT ANY JOIN table_2 ON table_1.id = table_2.id AND startsWith(table_2.text, '234') ORDER BY name, text")

    exec_query_compare_result(
        "SELECT name, text FROM table_1 RIGHT ALL JOIN table_2 ON table_1.id = table_2.id AND startsWith(table_2.text, '234') ORDER BY name, text")

    # ANY FULL JOINs are not implemented
    exec_query_compare_result(
        "SELECT name, text FROM table_1 FULL OUTER JOIN table_2 ON table_1.id = table_2.id AND startsWith(table_2.text, '123') ORDER BY name, text")

    exec_query_compare_result(
        "SELECT name, text FROM table_1 LEFT ANY JOIN table_2 ON table_1.id = table_2.id AND startsWith(table_2.text, '123') ORDER BY name, text")

    exec_query_compare_result(
        "SELECT name, text FROM table_1 LEFT ALL JOIN table_2 ON table_1.id = table_2.id AND startsWith(table_2.text, '123') ORDER BY name, text")


def test_semi_and_anti(started_cluster):
    # SEMI|ANTI JOIN should be LEFT or RIGHT
    exec_query_compare_result(
        "SELECT name, text FROM table_1 LEFT OUTER JOIN table_2 ON table_1.id = table_2.id AND startsWith(table_2.text, '123') ORDER BY name, text")

    exec_query_compare_result(
        "SELECT name, text FROM table_1 LEFT SEMI JOIN table_2 ON table_1.id = table_2.id AND startsWith(table_2.text, '123') ORDER BY name, text")

    exec_query_compare_result(
        "SELECT name, text FROM table_1 LEFT ANTI JOIN table_2 ON table_1.id = table_2.id AND startsWith(table_2.text, '123') ORDER BY name, text")

    exec_query_compare_result("SELECT name, text FROM table_1 RIGHT SEMI JOIN table_2 ON table_1.id = table_2.id AND startsWith(table_2.text, '234') ORDER BY name, text")

    exec_query_compare_result("SELECT name, text FROM table_1 RIGHT ANTI JOIN table_2 ON table_1.id = table_2.id AND startsWith(table_2.text, '234') ORDER BY name, text")


def test_asof_join(started_cluster):
    # Only ASOF and LEFT ASOF joins are supported
    exec_query_compare_result(
        "SELECT name, text FROM table_1 INNER ASOF JOIN table_2 ON table_1.id > table_2.id AND table_1.val = table_2.text ORDER BY name, text")

    exec_query_compare_result(
        "SELECT name, text FROM table_1 LEFT ASOF JOIN table_2 ON table_1.id > table_2.id AND table_1.val = table_2.text ORDER BY name, text")


def test_cross_join(started_cluster):
    exec_query_compare_result("SELECT name, text FROM table_1 CROSS JOIN table_2 ORDER BY name, text")

    exec_query_compare_result(
        "SELECT name, text FROM table_1 CROSS JOIN table_2 WHERE table_1.val = table_2.text ORDER BY name, text")
