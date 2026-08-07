import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)
node2 = cluster.add_instance(
    "node2", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)
node3 = cluster.add_instance(
    "node3", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)
node4 = cluster.add_instance(
    "node4", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        for node in [node1, node2]:
            node.query_with_retry(
                """
            CREATE TABLE IF NOT EXISTS test_table_replicated(date Date, id UInt32, value Int32)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/0/sometable', '{replica}') ORDER BY id;
                """.format(
                    replica=node.name
                )
            )
            node.query_with_retry(
                """CREATE TABLE IF NOT EXISTS test_table(date Date, id UInt32, value Int32) ENGINE=MergeTree ORDER BY id"""
            )

        for node in [node3, node4]:
            node.query_with_retry(
                """
            CREATE TABLE IF NOT EXISTS test_table_replicated(date Date, id UInt32, value Int32)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/1/someotable', '{replica}') ORDER BY id;
                """.format(
                    replica=node.name
                )
            )

            node.query_with_retry(
                """CREATE TABLE IF NOT EXISTS test_table(date Date, id UInt32, value Int32) ENGINE=MergeTree ORDER BY id"""
            )

        yield cluster

    finally:
        cluster.shutdown()


def test_alter_on_cluter_non_replicated(started_cluster):
    for node in [node1, node2, node3, node4]:
        node.query("INSERT INTO test_table VALUES(toDate('2019-10-01'), 1, 1)")

    assert node1.query("SELECT COUNT() FROM test_table") == "1\n"
    assert node2.query("SELECT COUNT() FROM test_table") == "1\n"
    assert node3.query("SELECT COUNT() FROM test_table") == "1\n"
    assert node4.query("SELECT COUNT() FROM test_table") == "1\n"

    node1.query(
        "ALTER TABLE test_table ON CLUSTER 'test_cluster_mixed' MODIFY COLUMN date DateTime"
    )

    assert node1.query("SELECT date FROM test_table") == "2019-10-01 00:00:00\n"
    assert node2.query("SELECT date FROM test_table") == "2019-10-01 00:00:00\n"
    assert node3.query("SELECT date FROM test_table") == "2019-10-01 00:00:00\n"
    assert node4.query("SELECT date FROM test_table") == "2019-10-01 00:00:00\n"

    node3.query(
        "ALTER TABLE test_table ON CLUSTER 'test_cluster_mixed' MODIFY COLUMN value String"
    )

    for node in [node1, node2, node3, node4]:
        node.query(
            "INSERT INTO test_table VALUES(toDateTime('2019-10-02 00:00:00'), 2, 'Hello')"
        )

    assert node1.query("SELECT COUNT() FROM test_table") == "2\n"
    assert node2.query("SELECT COUNT() FROM test_table") == "2\n"
    assert node3.query("SELECT COUNT() FROM test_table") == "2\n"
    assert node4.query("SELECT COUNT() FROM test_table") == "2\n"

    for node in [node1, node2, node3, node4]:
        node.query("TRUNCATE TABLE test_table")


def test_alter_replicated_on_cluster(started_cluster):
    for node in [node1, node3]:
        node.query(
            "INSERT INTO test_table_replicated VALUES(toDate('2019-10-01'), 1, 1)"
        )

    for node in [node2, node4]:
        node.query("SYSTEM SYNC REPLICA test_table_replicated", timeout=20)

    node1.query(
        "ALTER TABLE test_table_replicated ON CLUSTER 'test_cluster_mixed' MODIFY COLUMN date DateTime",
        settings={"replication_alter_partitions_sync": "2"},
    )

    assert (
        node1.query("SELECT date FROM test_table_replicated") == "2019-10-01 00:00:00\n"
    )
    assert (
        node2.query("SELECT date FROM test_table_replicated") == "2019-10-01 00:00:00\n"
    )
    assert (
        node3.query("SELECT date FROM test_table_replicated") == "2019-10-01 00:00:00\n"
    )
    assert (
        node4.query("SELECT date FROM test_table_replicated") == "2019-10-01 00:00:00\n"
    )

    node3.query_with_retry(
        "ALTER TABLE test_table_replicated ON CLUSTER 'test_cluster_mixed' MODIFY COLUMN value String",
        settings={"replication_alter_partitions_sync": "2"},
    )

    for node in [node2, node4]:
        node.query(
            "INSERT INTO test_table_replicated VALUES(toDateTime('2019-10-02 00:00:00'), 2, 'Hello')"
        )

    for node in [node1, node3]:
        node.query("SYSTEM SYNC REPLICA test_table_replicated", timeout=20)

    assert node1.query("SELECT COUNT() FROM test_table_replicated") == "2\n"
    assert node2.query("SELECT COUNT() FROM test_table_replicated") == "2\n"
    assert node3.query("SELECT COUNT() FROM test_table_replicated") == "2\n"
    assert node4.query("SELECT COUNT() FROM test_table_replicated") == "2\n"

    for node in [node1, node2, node3, node4]:
        node.query("TRUNCATE TABLE test_table_replicated")
