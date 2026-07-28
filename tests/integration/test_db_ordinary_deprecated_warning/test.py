import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    user_configs=[
        "configs/users.xml",
    ],
    stay_alive = True
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_warning():
    node.query("DROP DATABASE IF EXISTS testdb")
    assert node.query("SELECT name FROM system.databases where engine = 'Ordinary'") == ""
    assert node.query("SELECT count() = 0 FROM system.databases where engine = 'Ordinary'") == "1\n"

    node.query("CREATE DATABASE testdb ENGINE = Ordinary")
    assert node.query("SELECT engine FROM system.databases where name = 'testdb'") == "Ordinary\n"
    assert node.query("SELECT count() = 1 FROM system.warnings where startsWith(message, 'Server has databases (for example `testdb`) with Ordinary engine')") == "1\n"

    node.stop_clickhouse()
    node.exec_in_container(
        ["bash", "-c", "touch /var/lib/clickhouse/flags/convert_ordinary_to_atomic"]
    )
    node.start_clickhouse()

    assert node.query("SELECT engine FROM system.databases where name = 'testdb'") == "Atomic\n"
    assert node.query("SELECT count() = 0 FROM system.warnings where startsWith(message, 'Server has databases (for example `testdb`) with Ordinary engine')") == "1\n"

    node.query("DROP DATABASE testdb")
    node.stop_clickhouse()
    node.start_clickhouse()


def _convert_to_atomic():
    node.stop_clickhouse()
    node.exec_in_container(
        ["bash", "-c", "touch /var/lib/clickhouse/flags/convert_ordinary_to_atomic"]
    )
    node.start_clickhouse()


def test_conversion_with_row_policies():
    # The Ordinary -> Atomic conversion moves every table into a temporary database and then renames
    # that database back, so each table ends up under its original (database, table). Row policies
    # must not block that: a database-wide policy cannot follow a table across databases, and the
    # rejection that protects a user rename would abort the conversion and thus server startup.
    #
    # The inner table of a materialized view is the opposite case: the conversion assigns a fresh UUID,
    # so renameInMemory changes the inner name from `.inner.<view>` to `.inner_id.<uuid>`. That name
    # genuinely changes, so a policy on the inner table must FOLLOW it.
    node.query("DROP DATABASE IF EXISTS testdb")
    node.query("DROP ROW POLICY IF EXISTS dbp ON testdb.*")
    node.query("CREATE DATABASE testdb ENGINE = Ordinary")
    node.query(
        "CREATE TABLE testdb.t (x UInt64, dept String) ENGINE = MergeTree ORDER BY x"
    )
    node.query("INSERT INTO testdb.t VALUES (1, 'eng'), (2, 'fin')")
    node.query("CREATE USER IF NOT EXISTS pol_user")
    node.query("GRANT SELECT ON testdb.* TO pol_user")
    node.query("CREATE ROW POLICY dbp ON testdb.* USING dept = 'eng' TO pol_user")
    node.query(
        "CREATE MATERIALIZED VIEW testdb.mv ENGINE = MergeTree ORDER BY x "
        "AS SELECT x, dept FROM testdb.t"
    )
    inner_before = node.query(
        "SELECT name FROM system.tables WHERE database = 'testdb' AND name LIKE '.inner%'"
    ).strip()
    assert inner_before == ".inner.mv", inner_before
    node.query(
        f"CREATE ROW POLICY innerp ON testdb.`{inner_before}` USING dept = 'eng' TO pol_user"
    )

    _convert_to_atomic()

    assert node.query("SELECT engine FROM system.databases WHERE name = 'testdb'") == "Atomic\n"
    # The database-wide policy stayed on testdb, which is the database name the conversion restored.
    assert (
        node.query("SELECT database, table FROM system.row_policies WHERE short_name = 'dbp'")
        == "testdb\t\n"
    )
    assert node.query("SELECT count() FROM testdb.t", user="pol_user") == "1\n"
    # The inner table was renamed, so its policy followed to the new name.
    inner_after = node.query(
        "SELECT name FROM system.tables WHERE database = 'testdb' AND name LIKE '.inner%'"
    ).strip()
    assert inner_after.startswith(".inner_id."), inner_after
    assert (
        node.query("SELECT database, table FROM system.row_policies WHERE short_name = 'innerp'")
        == f"testdb\t{inner_after}\n"
    )

    node.query("DROP ROW POLICY dbp ON testdb.*")
    node.query(f"DROP ROW POLICY innerp ON testdb.`{inner_after}`")
    node.query("DROP DATABASE testdb")
    node.query("DROP USER pol_user")
    node.stop_clickhouse()
    node.start_clickhouse()
