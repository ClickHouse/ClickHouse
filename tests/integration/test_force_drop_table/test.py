import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

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


def test_drop_database_during_refresh(started_cluster):
    # The temporary table of a refresh in progress is dropped by stopping the view, without a size check.
    node.query("CREATE DATABASE d4 ENGINE=Atomic")
    node.query(
        "CREATE MATERIALIZED VIEW d4.rmv REFRESH EVERY 1 YEAR (x UInt64) ENGINE=MergeTree ORDER BY x AS "
        "SELECT number + sleepEachRow(0.1) AS x FROM numbers(3000) "
        "SETTINGS max_block_size = 1, max_threads = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 1"
    )
    assert_eq_with_retry(
        node,
        "SELECT count() > 0 FROM system.parts WHERE database = 'd4' AND table LIKE '.tmp.inner_id.%' AND active",
        "1",
        retry_count=120,
        sleep_time=0.5,
    )
    node.query("DROP DATABASE d4 SYNC")
    assert node.query("SELECT count() FROM system.databases WHERE name = 'd4'") == "0\n"


def test_drop_database_refused_by_refresh_leftover(started_cluster):
    # A leftover temporary table of an idle refreshable view refuses the drop before the view is stopped.
    node.query("CREATE DATABASE d5 ENGINE=Atomic")
    node.query(
        "CREATE MATERIALIZED VIEW d5.rmv REFRESH EVERY 1 YEAR (x UInt64) ENGINE=MergeTree ORDER BY x EMPTY "
        "AS SELECT number AS x FROM numbers(10)"
    )
    uuid = node.query(
        "SELECT uuid FROM system.tables WHERE database = 'd5' AND name = 'rmv'"
    ).strip()
    node.query(
        f"CREATE TABLE d5.`.tmp.inner_id.{uuid}` (x UInt64) ENGINE=MergeTree ORDER BY x"
    )
    node.query(
        f"INSERT INTO d5.`.tmp.inner_id.{uuid}` SELECT number FROM numbers(1000)"
    )
    assert "is greater than max" in node.query_and_get_error("DROP DATABASE d5")
    assert (
        node.query(
            "SELECT count() FROM system.view_refreshes WHERE database = 'd5' AND view = 'rmv'"
        )
        == "1\n"
    )
    node.query("SYSTEM REFRESH VIEW d5.rmv")
    node.query("SYSTEM WAIT VIEW d5.rmv")
    assert node.query("SELECT count() FROM d5.rmv") == "10\n"
    node.query("DROP DATABASE d5 SYNC SETTINGS max_table_size_to_drop = 0")
