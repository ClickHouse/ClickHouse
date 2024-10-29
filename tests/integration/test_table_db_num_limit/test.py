import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node1",
    with_zookeeper=True,
    main_configs=["config/config.xml"],
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

    for i in range(5):
        node.query(
            "create table t{} (a Int32) Engine = ReplicatedMergeTree('/clickhouse/tables/t{}', 'r1') order by a".format(
                i, i
            )
        )

    assert "Too many replicated tables" in node.query_and_get_error(
        "create table tx (a Int32) Engine = ReplicatedMergeTree('/clickhouse/tables/tx', 'r1') order by a"
    )

    # Checks that replicated tables are also counted as regular tables
    for i in range(5, 10):
        node.query("create table t{} (a Int32) Engine = Log".format(i))

    assert "TOO_MANY_TABLES" in node.query_and_get_error(
        "create table tx (a Int32) Engine = Log"
    )
