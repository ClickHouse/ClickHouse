import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    user_configs=[
        "configs/users.xml",
        # Declares a READ-ONLY (users.xml) row policy on an OUTER table of an Ordinary database.
        # See test_conversion_with_a_readonly_policy_on_an_outer_table.
        "configs/readonly_outer_policy.xml",
        # The same, on a materialized-view INNER table.
        # See test_conversion_with_a_readonly_policy_on_a_view_inner_table.
        "configs/readonly_inner_policy.xml",
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


def test_conversion_with_a_readonly_policy_on_an_outer_table():
    # This is the arm that pins `conversion_keeps_table_name` in InterpreterRenameQuery.
    #
    # A users.xml row policy lives in a READ-ONLY access storage, so it can never be re-keyed and the
    # preflight rejects any rename that would have to move it. The startup conversion of an Ordinary
    # database to Atomic is a chain of renames, but its OUTER moves preserve the table name (the tables
    # go into a staging database which is then renamed back), so for those the transition must be
    # skipped -- the policy is already on the name the table will have when the conversion finishes.
    # Without that exemption the read-only rejection fires during the conversion, which runs at
    # startup, and the server does not come back up.
    #
    # The sibling arm above cannot catch this: its policies are writable, and its database-wide `dbp`
    # is additionally exempted by the cross-database rejection's own `!converting_database_engine`
    # guard, so a writable per-table policy just moves and moves back either way.
    #
    # This is the OUTER-table case. The INNER-table case is
    # test_conversion_with_a_readonly_policy_on_a_view_inner_table below.
    node.query("DROP DATABASE IF EXISTS rodb")
    node.query("CREATE DATABASE rodb ENGINE = Ordinary")
    node.query(
        "CREATE TABLE rodb.outer (x UInt64, dept String) ENGINE = MergeTree ORDER BY x"
    )
    node.query("INSERT INTO rodb.outer VALUES (1, 'eng'), (2, 'fin')")
    # The read-only policy comes from configs/readonly_outer_policy.xml and is bound to (rodb, outer).
    assert (
        node.query(
            "SELECT database, table FROM system.row_policies WHERE database = 'rodb'"
        )
        == "rodb\touter\n"
    )
    assert node.query("SELECT count() FROM rodb.outer", user="ro_user") == "1\n"

    try:
        _convert_to_atomic()

        # The server came back up and the conversion completed.
        assert (
            node.query("SELECT engine FROM system.databases WHERE name = 'rodb'")
            == "Atomic\n"
        )
        # The read-only policy is still bound to the same (database, table) -- nothing moved it ...
        assert (
            node.query(
                "SELECT database, table FROM system.row_policies WHERE database = 'rodb'"
            )
            == "rodb\touter\n"
        )
        # ... and it still filters. The true row count is 2.
        assert (
            node.query(
                "SELECT sum(rows) FROM system.parts "
                "WHERE database = 'rodb' AND table = 'outer' AND active"
            )
            == "2\n"
        )
        assert node.query("SELECT count() FROM rodb.outer", user="ro_user") == "1\n"
        assert node.query("SELECT x FROM rodb.outer", user="ro_user") == "1\n"
    finally:
        node.query("DROP DATABASE IF EXISTS rodb")
        node.stop_clickhouse()
        node.start_clickhouse()


def test_conversion_with_a_readonly_policy_on_a_view_inner_table():
    # The INNER-table counterpart of the arm above, and the one case where the conversion cannot
    # keep the name: it assigns the view a fresh UUID, so the inner table goes from `.inner.mv` to
    # `.inner_id.<uuid>`. A read-only policy cannot be re-keyed onto that new name, and refusing
    # runs at startup, where there is no user to report to -- the server would not come back up, and
    # the flag file survives, so every later restart would fail the same way. The move is therefore
    # declined: the conversion completes and the policy stays on the name it had, which is exactly
    # where a server without this feature leaves it.
    node.query("DROP DATABASE IF EXISTS roinner")
    node.query("CREATE DATABASE roinner ENGINE = Ordinary")
    node.query(
        "CREATE TABLE roinner.src (x UInt64, dept String) ENGINE = MergeTree ORDER BY x"
    )
    node.query(
        "CREATE MATERIALIZED VIEW roinner.mv ENGINE = MergeTree ORDER BY x "
        "AS SELECT x, dept FROM roinner.src"
    )
    node.query("INSERT INTO roinner.src VALUES (1, 'eng'), (2, 'fin')")

    inner_before = node.query(
        "SELECT name FROM system.tables WHERE database = 'roinner' AND name LIKE '.inner%'"
    ).strip()
    assert inner_before == ".inner.mv", inner_before
    # The policy is declared in configs/readonly_inner_policy.xml and binds that hidden name.
    assert (
        node.query(
            "SELECT database, table, storage FROM system.row_policies WHERE database = 'roinner'"
        )
        == "roinner\t.inner.mv\tusers_xml\n"
    )

    try:
        _convert_to_atomic()

        # The server came back up -- this is the assertion the fix exists for -- and converted.
        assert (
            node.query("SELECT engine FROM system.databases WHERE name = 'roinner'")
            == "Atomic\n"
        )
        # The inner table did get the UUID-based name, so the rename really happened.
        inner_after = node.query(
            "SELECT name FROM system.tables WHERE database = 'roinner' AND name LIKE '.inner%'"
        ).strip()
        assert inner_after.startswith(".inner_id."), inner_after
        # The read-only policy could not follow it and stayed where it was.
        assert (
            node.query(
                "SELECT database, table FROM system.row_policies WHERE database = 'roinner'"
            )
            == "roinner\t.inner.mv\n"
        )
        # Declining at startup did not weaken the check itself. The stranded policy still names
        # `.inner.mv`, so a table created there is covered by it, and a USER rename of that table is
        # still refused with nothing committed. (Renaming the view itself would not test this: the
        # database is Atomic now, so its inner table keeps its UUID name and no policy has to move.)
        node.query(
            "CREATE TABLE roinner.`.inner.mv` (x UInt64) ENGINE = MergeTree ORDER BY x"
        )
        assert "ACCESS_STORAGE_READONLY" in node.query_and_get_error(
            "RENAME TABLE roinner.`.inner.mv` TO roinner.moved"
        )
        assert (
            node.query(
                "SELECT count() FROM system.tables "
                "WHERE database = 'roinner' AND name = '.inner.mv'"
            )
            == "1\n"
        )
    finally:
        node.query("DROP DATABASE IF EXISTS roinner")
        node.stop_clickhouse()
        node.start_clickhouse()
