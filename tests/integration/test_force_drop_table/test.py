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


def create_database_with_tables(db, engine, mt_rows):
    node.query(
        f"CREATE DATABASE {db} ENGINE={engine}",
        settings={"allow_deprecated_database_ordinary": 1},
    )
    node.query(
        f"CREATE TABLE {db}.rmt (n UInt64) ENGINE=ReplicatedMergeTree('/test/{db}/rmt', 'r1') ORDER BY n"
    )
    node.query(f"CREATE TABLE {db}.mt (n UInt64) ENGINE=MergeTree ORDER BY n")
    node.query(f"INSERT INTO {db}.rmt VALUES (1), (2)")
    if mt_rows:
        node.query(f"INSERT INTO {db}.mt VALUES (1), (2)")


def assert_tables_usable(db, tables_to_insert):
    assert (
        node.query(
            f"SELECT name FROM system.tables WHERE database = '{db}' ORDER BY name"
        )
        == "mt\nrmt\n"
    )
    for table in tables_to_insert:
        node.query(f"INSERT INTO {db}.{table} VALUES (3)")
    assert (
        node.query(f"SELECT is_readonly FROM system.replicas WHERE database = '{db}'")
        == "0\n"
    )


@pytest.mark.parametrize("engine", ["Ordinary", "Atomic"])
def test_drop_database(started_cluster, engine):
    # A DROP DATABASE refused by the size limit leaves every table usable.
    db = f"d2_{engine}"
    create_database_with_tables(db, engine, mt_rows=False)
    assert "is greater than max" in node.query_and_get_error(f"DROP DATABASE {db}")
    assert_tables_usable(db, ["rmt"])

    # One table above the limit: the flag allows the drop and is consumed once.
    create_force_drop_flag(node)
    node.query(f"DROP DATABASE {db} SYNC")
    assert (
        node.query(f"SELECT count() FROM system.databases WHERE name = '{db}'")
        == "0\n"
    )

    # Two tables above the limit and one flag: refused before any table is touched.
    db = f"d3_{engine}"
    create_database_with_tables(db, engine, mt_rows=True)
    create_force_drop_flag(node)
    assert "is greater than max" in node.query_and_get_error(f"DROP DATABASE {db}")
    assert_tables_usable(db, ["rmt", "mt"])
    node.query(f"DROP DATABASE {db} SYNC SETTINGS max_table_size_to_drop = 0")
