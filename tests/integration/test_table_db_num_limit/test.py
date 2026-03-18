import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node1",
    with_zookeeper=True,
    macros={"replica": "r1"},
    main_configs=["config/config.xml", "config/config1.xml"],
)

node2 = cluster.add_instance(
    "node2",
    with_zookeeper=True,
    macros={"replica": "r2"},
    main_configs=["config/config.xml", "config/config2.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def test_table_db_limit(started_cluster):
    # By the way, default database already exists.
    for i in range(9):
        node.query("create database db{}".format(i))

    assert "TOO_MANY_DATABASES" in node.query_and_get_error(
        "create database db_exp".format(i)
    )

    for i in range(10):
        node.query("create table t{} (a Int32) Engine = Log".format(i))

    # This checks that system tables are not accounted in the number of tables.
    node.query("system flush logs")

    # Regular tables
    for i in range(10):
        node.query("drop table t{}".format(i))

    for i in range(10):
        node.query("create table t{} (a Int32) Engine = Log".format(i))

    assert "TOO_MANY_TABLES" in node.query_and_get_error(
        "create table default.tx (a Int32) Engine = Log"
    )

    # Dictionaries
    for i in range(10):
        node.query(
            "create dictionary d{} (a Int32) primary key a source(null()) layout(flat()) lifetime(1000)".format(
                i
            )
        )

    assert "TOO_MANY_TABLES" in node.query_and_get_error(
        "create dictionary dx (a Int32) primary key a source(null()) layout(flat()) lifetime(1000)"
    )

    # Replicated tables
    for i in range(10):
        node.query("drop table t{}".format(i))

    for i in range(3):
        node.query(
            "create table t{} on cluster 'cluster' (a Int32) Engine = ReplicatedMergeTree('/clickhouse/tables/t{}', '{{replica}}') order by a".format(
                i, i
            )
        )

    # Test limit on other replica
    assert "Too many replicated tables" in node2.query_and_get_error(
        "create table tx (a Int32) Engine = ReplicatedMergeTree('/clickhouse/tables/tx', '{replica}') order by a"
    )

    for i in range(3, 5):
        node.query(
            "create table t{} (a Int32) Engine = ReplicatedMergeTree('/clickhouse/tables/t{}', '{{replica}}') order by a".format(
                i, i
            )
        )

    assert "Too many replicated tables" in node.query_and_get_error(
        "create table tx (a Int32) Engine = ReplicatedMergeTree('/clickhouse/tables/tx', '{replica}') order by a"
    )

    # Checks that replicated tables are also counted as regular tables
    for i in range(5, 10):
        node.query("create table t{} (a Int32) Engine = Log".format(i))

    assert "TOO_MANY_TABLES" in node.query_and_get_error(
        "create table tx (a Int32) Engine = Log"
    )

    # Cleanup
    for i in range(10):
        node.query("drop table t{} sync".format(i))
    for i in range(3):
        node2.query("drop table t{} sync".format(i))
    for i in range(9):
        node.query("drop database db{}".format(i))
    for i in range(10):
        node.query("drop dictionary d{}".format(i))


def test_replicated_database(started_cluster):
    node.query("CREATE DATABASE db_replicated ENGINE = Replicated('/clickhouse/db_replicated', '{replica}');")
    for i in range(10):
        node.query(f"CREATE TABLE db_replicated.t{i} (a Int32) ENGINE = Log;")
    assert "TOO_MANY_TABLES" in node.query_and_get_error(
        "CREATE TABLE db_replicated.tx (a Int32) ENGINE = Log;"
    )
    node.query("DROP DATABASE db_replicated SYNC;")
