import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance("instance", stay_alive=True)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def create_entities():
    instance.query(
        "CREATE SETTINGS PROFILE s1 SETTINGS max_memory_usage = 123456789 MIN 100000000 MAX 200000000"
    )
    instance.query("CREATE USER u1 SETTINGS PROFILE s1")
    instance.query("CREATE ROLE rx SETTINGS PROFILE s1")
    instance.query("CREATE USER u2 IDENTIFIED BY 'qwerty' HOST LOCAL DEFAULT ROLE rx")
    instance.query("CREATE SETTINGS PROFILE s2 SETTINGS PROFILE s1 TO u2")
    instance.query(
        "CREATE ROW POLICY p ON mydb.mytable FOR SELECT USING a<1000 TO u1, u2"
    )
    instance.query(
        "CREATE QUOTA q FOR INTERVAL 1 HOUR MAX QUERIES 100 TO ALL EXCEPT rx"
    )


@pytest.fixture(autouse=True)
def drop_entities():
    instance.query("DROP USER IF EXISTS u1, u2")
    instance.query("DROP ROLE IF EXISTS rx, ry")
    instance.query("DROP ROW POLICY IF EXISTS p ON mydb.mytable")
    instance.query("DROP QUOTA IF EXISTS q")
    instance.query("DROP SETTINGS PROFILE IF EXISTS s1, s2")


def test_create():
    create_entities()

    def check():
        assert (
            instance.query("SHOW CREATE USER u1")
            == "CREATE USER u1 IDENTIFIED WITH no_password SETTINGS PROFILE `s1`\n"
        )
        assert (
            instance.query("SHOW CREATE USER u2")
            == "CREATE USER u2 IDENTIFIED WITH sha256_password HOST LOCAL DEFAULT ROLE rx\n"
        )
        assert (
            instance.query("SHOW CREATE ROW POLICY p ON mydb.mytable")
            == "CREATE ROW POLICY p ON mydb.mytable FOR SELECT USING a < 1000 TO u1, u2\n"
        )
        assert (
            instance.query("SHOW CREATE QUOTA q")
            == "CREATE QUOTA q FOR INTERVAL 1 hour MAX queries = 100 TO ALL EXCEPT rx\n"
        )
        assert instance.query("SHOW GRANTS FOR u1") == ""
        assert instance.query("SHOW GRANTS FOR u2") == "GRANT rx TO u2\n"
        assert (
            instance.query("SHOW CREATE ROLE rx")
            == "CREATE ROLE rx SETTINGS PROFILE `s1`\n"
        )
        assert instance.query("SHOW GRANTS FOR rx") == ""
        assert (
            instance.query("SHOW CREATE SETTINGS PROFILE s1")
            == "CREATE SETTINGS PROFILE `s1` SETTINGS max_memory_usage = 123456789 MIN 100000000 MAX 200000000\n"
        )
        assert (
            instance.query("SHOW CREATE SETTINGS PROFILE s2")
            == "CREATE SETTINGS PROFILE `s2` SETTINGS INHERIT `s1` TO u2\n"
        )

    check()
    instance.restart_clickhouse()  # Check persistency
    check()


def test_alter():
    create_entities()
    instance.restart_clickhouse()

    instance.query("CREATE ROLE ry")
    instance.query("GRANT ry TO u2")
    instance.query("ALTER USER u2 DEFAULT ROLE ry")
    instance.query("GRANT rx TO ry WITH ADMIN OPTION")
    instance.query("ALTER ROLE rx SETTINGS PROFILE s2")
    instance.query("GRANT SELECT ON mydb.mytable TO u1")
    instance.query("GRANT SELECT ON mydb.* TO rx WITH GRANT OPTION")
    instance.query(
        "ALTER SETTINGS PROFILE s1 SETTINGS max_memory_usage = 987654321 CONST"
    )

    def check():
        assert (
            instance.query("SHOW CREATE USER u1")
            == "CREATE USER u1 IDENTIFIED WITH no_password SETTINGS PROFILE `s1`\n"
        )
        assert (
            instance.query("SHOW CREATE USER u2")
            == "CREATE USER u2 IDENTIFIED WITH sha256_password HOST LOCAL DEFAULT ROLE ry\n"
        )
        assert (
            instance.query("SHOW GRANTS FOR u1")
            == "GRANT SELECT ON mydb.mytable TO u1\n"
        )
        assert instance.query("SHOW GRANTS FOR u2") == "GRANT rx, ry TO u2\n"
        assert (
            instance.query("SHOW CREATE ROLE rx")
            == "CREATE ROLE rx SETTINGS PROFILE `s2`\n"
        )
        assert instance.query("SHOW CREATE ROLE ry") == "CREATE ROLE ry\n"
        assert (
            instance.query("SHOW GRANTS FOR rx")
            == "GRANT SELECT ON mydb.* TO rx WITH GRANT OPTION\n"
        )
        assert (
            instance.query("SHOW GRANTS FOR ry") == "GRANT rx TO ry WITH ADMIN OPTION\n"
        )
        assert (
            instance.query("SHOW CREATE SETTINGS PROFILE s1")
            == "CREATE SETTINGS PROFILE `s1` SETTINGS max_memory_usage = 987654321 CONST\n"
        )
        assert (
            instance.query("SHOW CREATE SETTINGS PROFILE s2")
            == "CREATE SETTINGS PROFILE `s2` SETTINGS INHERIT `s1` TO u2\n"
        )

    check()
    instance.restart_clickhouse()  # Check persistency
    check()


def test_drop():
    create_entities()
    instance.restart_clickhouse()

    instance.query("DROP USER u2")
    instance.query("DROP ROLE rx")
    instance.query("DROP ROW POLICY p ON mydb.mytable")
    instance.query("DROP QUOTA q")
    instance.query("DROP SETTINGS PROFILE s1")

    def check():
        assert (
            instance.query("SHOW CREATE USER u1")
            == "CREATE USER u1 IDENTIFIED WITH no_password\n"
        )
        assert (
            instance.query("SHOW CREATE SETTINGS PROFILE s2")
            == "CREATE SETTINGS PROFILE `s2`\n"
        )
        assert "There is no user `u2`" in instance.query_and_get_error(
            "SHOW CREATE USER u2"
        )
        assert (
            "There is no row policy `p ON mydb.mytable`"
            in instance.query_and_get_error("SHOW CREATE ROW POLICY p ON mydb.mytable")
        )
        assert "There is no quota `q`" in instance.query_and_get_error(
            "SHOW CREATE QUOTA q"
        )

    check()
    instance.restart_clickhouse()  # Check persistency
    check()


def test_drop_role_cascades_to_disk():
    """
    Regression test for https://github.com/ClickHouse/ClickHouse/issues/104298.

    Before the fix, DROP ROLE only removed the role from the in-memory entity map. Other
    entities that referenced the dropped role (e.g. a user's DEFAULT ROLE list, a settings
    profile's TO list) still kept the dropped UUID in their on-disk *.sql files. The
    SHOW CREATE output looked clean only because the rendering layer filters unknown UUIDs,
    but on the next server restart those references were loaded back into memory and
    surfaced as `ACCESS_ENTITY_NOT_FOUND` errors during distributed/Remote query execution.

    This test creates a role, references it from several other entities, drops the role,
    and asserts that the dropped UUID is no longer present in any *.sql file under the
    access directory — and that after a restart no entity refers back to it.
    """
    instance.query(
        """
        DROP USER     IF EXISTS u_104298;
        DROP ROLE     IF EXISTS r_keep_104298, r_drop_104298, r_parent_104298;
        DROP SETTINGS PROFILE IF EXISTS sp_104298;
        DROP ROW POLICY IF EXISTS rp_104298 ON mydb.mytable;
        DROP QUOTA    IF EXISTS q_104298;
        """
    )

    instance.query("CREATE ROLE r_keep_104298")
    instance.query("CREATE ROLE r_drop_104298")
    instance.query("CREATE ROLE r_parent_104298")
    instance.query(
        "CREATE USER u_104298 DEFAULT ROLE r_keep_104298, r_drop_104298"
    )
    instance.query("GRANT r_keep_104298, r_drop_104298 TO u_104298")
    instance.query("GRANT r_drop_104298 TO r_parent_104298")
    instance.query(
        "CREATE SETTINGS PROFILE sp_104298 SETTINGS max_memory_usage = 1000000 TO r_drop_104298"
    )
    instance.query(
        "CREATE ROW POLICY rp_104298 ON mydb.mytable FOR SELECT USING 1 TO r_drop_104298"
    )
    instance.query(
        "CREATE QUOTA q_104298 FOR INTERVAL 1 HOUR MAX QUERIES 100 TO r_drop_104298"
    )

    drop_uuid = instance.query(
        "SELECT id FROM system.roles WHERE name = 'r_drop_104298'"
    ).strip()
    assert drop_uuid

    instance.query("DROP ROLE r_drop_104298")

    def grep_dropped_uuid():
        # Returns the names of any *.sql file under the access dir that still
        # references the dropped role's UUID.
        return instance.exec_in_container(
            [
                "bash",
                "-c",
                f"grep -l '{drop_uuid}' /var/lib/clickhouse/access/*.sql || true",
            ]
        ).strip()

    # The fix must propagate the cleanup to disk synchronously.
    assert grep_dropped_uuid() == "", (
        f"Dropped role UUID {drop_uuid} still found in access dir on disk"
    )

    # And after a restart the in-memory state must still be clean: nobody references
    # the dropped role any more.
    instance.restart_clickhouse()

    assert grep_dropped_uuid() == ""

    assert (
        instance.query("SHOW CREATE USER u_104298")
        == "CREATE USER u_104298 IDENTIFIED WITH no_password DEFAULT ROLE r_keep_104298\n"
    )
    assert (
        instance.query("SHOW GRANTS FOR u_104298") == "GRANT r_keep_104298 TO u_104298\n"
    )
    assert instance.query("SHOW GRANTS FOR r_parent_104298") == ""
    assert (
        instance.query("SHOW CREATE SETTINGS PROFILE sp_104298")
        == "CREATE SETTINGS PROFILE `sp_104298` SETTINGS max_memory_usage = 1000000\n"
    )
    assert (
        instance.query("SHOW CREATE ROW POLICY rp_104298 ON mydb.mytable")
        == "CREATE ROW POLICY rp_104298 ON mydb.mytable FOR SELECT USING 1\n"
    )
    assert (
        instance.query("SHOW CREATE QUOTA q_104298")
        == "CREATE QUOTA q_104298 FOR INTERVAL 1 hour MAX queries = 100\n"
    )

    # Cleanup so we don't leak state into other tests.
    instance.query(
        """
        DROP USER     IF EXISTS u_104298;
        DROP ROLE     IF EXISTS r_keep_104298, r_parent_104298;
        DROP SETTINGS PROFILE IF EXISTS sp_104298;
        DROP ROW POLICY IF EXISTS rp_104298 ON mydb.mytable;
        DROP QUOTA    IF EXISTS q_104298;
        """
    )
