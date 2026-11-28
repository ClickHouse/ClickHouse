import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node", main_configs=["configs/config.xml"], with_zookeeper=True
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def create_force_drop_flag(node):
    force_drop_flag_path = "/var/lib/clickhouse/flags/force_drop_table"
    node.exec_in_container(
        [
            "bash",
            "-c",
            "touch {} && chmod a=rw {}".format(
                force_drop_flag_path, force_drop_flag_path
            ),
        ],
        user="root",
    )


@pytest.mark.parametrize("engine", ["Ordinary", "Atomic"])
def test_drop_materialized_view(started_cluster, engine):
    node.query(
        "CREATE DATABASE d ENGINE={}".format(engine),
        settings={"allow_deprecated_database_ordinary": 1},
    )
    node.query(
        "CREATE TABLE d.rmt (n UInt64) ENGINE=ReplicatedMergeTree('/test/rmt', 'r1') ORDER BY n PARTITION BY n % 2"
    )
    node.query(
        "CREATE MATERIALIZED VIEW d.mv (n UInt64, s String) ENGINE=MergeTree ORDER BY n PARTITION BY n % 2 AS SELECT n, toString(n) AS s FROM d.rmt"
    )
    node.query("INSERT INTO d.rmt VALUES (1), (2)")
    assert "is greater than max" in node.query_and_get_error("DROP TABLE d.rmt")
    assert "is greater than max" in node.query_and_get_error("DROP TABLE d.mv")
    assert "is greater than max" in node.query_and_get_error("TRUNCATE TABLE d.rmt")
    assert "is greater than max" in node.query_and_get_error("TRUNCATE TABLE d.mv")
    assert "is greater than max" in node.query_and_get_error(
        "ALTER TABLE d.rmt DROP PARTITION '0'"
    )
    assert node.query("SELECT * FROM d.rmt ORDER BY n") == "1\n2\n"
    assert node.query("SELECT * FROM d.mv ORDER BY n") == "1\t1\n2\t2\n"

    create_force_drop_flag(node)
    node.query("ALTER TABLE d.rmt DROP PARTITION '0'")
    assert node.query("SELECT * FROM d.rmt ORDER BY n") == "1\n"
    assert "is greater than max" in node.query_and_get_error(
        "ALTER TABLE d.mv DROP PARTITION '0'"
    )
    create_force_drop_flag(node)
    node.query("ALTER TABLE d.mv DROP PARTITION '0'")
    assert node.query("SELECT * FROM d.mv ORDER BY n") == "1\t1\n"
    assert "is greater than max" in node.query_and_get_error("DROP TABLE d.rmt SYNC")
    create_force_drop_flag(node)
    node.query("DROP TABLE d.rmt SYNC")
    assert "is greater than max" in node.query_and_get_error("DROP TABLE d.mv SYNC")
    create_force_drop_flag(node)
    node.query("DROP TABLE d.mv SYNC")
    node.query("DROP DATABASE d")


def test_drop_partition_where(started_cluster):
    # The matched partitions are checked before any of them is dropped. The check of the
    # matched partition must not consume the flag file needed to drop it.
    node.query(
        "CREATE TABLE mt_where (n UInt64) ENGINE=MergeTree ORDER BY n PARTITION BY n % 2"
    )
    node.query("INSERT INTO mt_where VALUES (1), (2)")
    assert "is greater than max" in node.query_and_get_error(
        "ALTER TABLE mt_where DROP PARTITION WHERE _partition_value.1 = 0"
    )
    assert node.query("SELECT * FROM mt_where ORDER BY n") == "1\n2\n"

    create_force_drop_flag(node)
    node.query("ALTER TABLE mt_where DROP PARTITION WHERE _partition_value.1 = 0")
    assert node.query("SELECT * FROM mt_where ORDER BY n") == "1\n"

    create_force_drop_flag(node)
    node.query("ALTER TABLE mt_where DETACH PARTITION WHERE _partition_value.1 = 1")
    assert node.query("SELECT count() FROM mt_where") == "0\n"
    node.query("DROP TABLE mt_where SYNC")
