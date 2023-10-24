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

        yield cluster

    finally:
        cluster.shutdown()


def test_query(started_cluster):
    node1.query("INSERT INTO table_1 SELECT id,'123','test' FROM generateRandom('id Int16') LIMIT 600")
    node1.query("INSERT INTO table_1 SELECT id,'234','test1' FROM generateRandom('id Int16') LIMIT 500")

    node1.query("INSERT INTO table_2 SELECT id,'123',10 FROM generateRandom('id Int16') LIMIT 500")
    node1.query("INSERT INTO table_2 SELECT id,'234',12 FROM generateRandom('id Int16') LIMIT 600")

    node1.query("SYSTEM FLUSH DISTRIBUTED table_1")
    node1.query("SYSTEM FLUSH DISTRIBUTED table_2")

    # SELECT <expr_list>
    # FROM <left_table>
    # [GLOBAL] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER|SEMI|ANTI|ANY|ASOF] JOIN <right_table>
    # (ON <expr_list>)|(USING <column_list>) ...

    # settings join_algorithm 'hash', 'parallel_hash', 'full_sorting_merge', 'grace_hash'

    node1.query("SELECT name, text FROM table_1 INNER JOIN table_2 ON table_1.id = table_2.id AND startsWith(table_2.text, '123')")

    node1.query("SELECT name, text FROM table_1 INNER ANY JOIN table_2 ON table_1.id = table_2.id AND startsWith(table_2.text, '123')")

    node1.query("SELECT name, text FROM table_1 INNER ALL JOIN table_2 ON table_1.id = table_2.id AND startsWith(table_2.text, '123')")

    node1.query("SELECT name, text FROM table_1 INNER ASOF JOIN table_2 ON table_1.id > table_2.id AND table_1.val = table_2.text")


    # SEMI|ANTI JOIN should be LEFT or RIGHT
    node1.query("SELECT name, text FROM table_1 LEFT OUTER JOIN table_2 ON table_1.id = table_2.id AND startsWith(table_2.text, '123')")

    node1.query("SELECT name, text FROM table_1 LEFT SEMI JOIN table_2 ON table_1.id = table_2.id AND startsWith(table_2.text, '123')")

    node1.query("SELECT name, text FROM table_1 LEFT ANTI JOIN table_2 ON table_1.id = table_2.id AND startsWith(table_2.text, '123')")

    node1.query("SELECT name, text FROM table_1 LEFT ANY JOIN table_2 ON table_1.id = table_2.id AND startsWith(table_2.text, '123')")

    node1.query("SELECT name, text FROM table_1 LEFT ALL JOIN table_2 ON table_1.id = table_2.id AND startsWith(table_2.text, '123')")

    # Only ASOF and LEFT ASOF joins are supported
    node1.query("SELECT name, text FROM table_1 LEFT ASOF JOIN table_2 ON table_1.id > table_2.id AND table_1.val = table_2.text")


    node1.query("SELECT name, text FROM table_1 RIGHT OUTER JOIN table_2 ON table_1.id = table_2.id AND startsWith(table_2.text, '234')")

    node1.query("SELECT name, text FROM table_1 RIGHT SEMI JOIN table_2 ON table_1.id = table_2.id AND startsWith(table_2.text, '234')")

    node1.query("SELECT name, text FROM table_1 RIGHT ANTI JOIN table_2 ON table_1.id = table_2.id AND startsWith(table_2.text, '234')")

    node1.query("SELECT name, text FROM table_1 RIGHT ANY JOIN table_2 ON table_1.id = table_2.id AND startsWith(table_2.text, '234')")

    node1.query("SELECT name, text FROM table_1 RIGHT ALL JOIN table_2 ON table_1.id = table_2.id AND startsWith(table_2.text, '234')")


    # ANY FULL JOINs are not implemented
    node1.query("SELECT name, text FROM table_1 FULL OUTER JOIN table_2 ON table_1.id = table_2.id AND startsWith(table_2.text, '123')")


    node1.query("SELECT name, text FROM table_1 CROSS JOIN table_2")

    node1.query("SELECT name, text FROM table_1 CROSS JOIN table_2 WHERE table_1.val = table_2.text")

