import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance("node1", stay_alive=True)
node2 = cluster.add_instance(
    "node2", main_configs=["configs/allow_use_non_utf8.xml"], stay_alive=True
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_create_with_non_utf(started_cluster):
    node = node1
    test_table = "test_table"

    node.query(f"DROP TABLE IF EXISTS {test_table}")
    node.query_and_get_error(
        f"""
        CREATE TABLE `{test_table}`(
            `привет` Int32,
            date DateTime
        ) ENGINE=MergeTree()
        ORDER BY date PARTITION BY toDate(date)
        SETTINGS ratio_of_defaults_for_sparse_serialization = 0.0
        """.encode(
            "cp1251"
        )
    )


def test_insert_with_non_utf_encoding(started_cluster):
    node = node2
    test_table = "test_table"

    node.query(f"DROP TABLE IF EXISTS {test_table}")
    node.query(
        f"""
        CREATE TABLE `{test_table}`(
        `привет` Int32,
        date DateTime
        ) ENGINE=MergeTree()
        ORDER BY date PARTITION BY toDate(date)
        """.encode(
            "cp1251"
        )
    )
    node.query_and_get_error(
        f"INSERT INTO `{test_table}` VALUES (100, '2024-11-22 10:00:00')"
    )

    assert (
        node.query(
            f"SELECT count() FROM system.parts WHERE table='{test_table}' and active=1;"
        )
        == "0\n"
    )
    node.restart_clickhouse()
    assert (
        node.query(
            f"SELECT count() FROM system.parts WHERE table='{test_table}' and active=1;"
        )
        == "0\n"
    )
    node.query(f"DROP TABLE {test_table} SYNC")


def test_alter_with_non_utf_encoding(started_cluster):
    node = node2
    test_table = "test_table"

    node.query(f"DROP TABLE IF EXISTS {test_table}")
    node.query(
        f"""
        CREATE TABLE `{test_table}`(
            val UInt32
        ) ENGINE=MergeTree()
        ORDER BY val PARTITION BY toDate(val)
        SETTINGS ratio_of_defaults_for_sparse_serialization = 0.0
        """
    )

    node.query(f"INSERT INTO `{test_table}` SELECT 1;")
    assert (
        node.query(
            f"SELECT count() FROM system.parts WHERE table='{test_table}' and active=1;"
        )
        == "1\n"
    )

    node.query_and_get_error(
        f"ALTER TABLE {test_table} ADD COLUMN `очень_странная_кодировка` UInt32 2".encode(
            "cp1251"
        )
    )
    assert (
        node.query(
            f"SELECT count() FROM system.parts WHERE table='{test_table}' and active=1;"
        )
        == "1\n"
    )

    node.query_and_get_error(f"INSERT INTO `{test_table}` SELECT 1,1")
    assert (
        node.query(
            f"SELECT count() FROM system.parts WHERE table='{test_table}' and active=1;"
        )
        == "1\n"
    )
    node.restart_clickhouse()

    assert (
        node.query(
            f"SELECT count() FROM system.parts WHERE table='{test_table}' and active=1;"
        )
        == "1\n"
    )
    node.query(f"DROP TABLE {test_table} SYNC")
