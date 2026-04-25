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


def test_replace_rolls_back_on_write_failure():
    # If `writeAccessEntityToDisk` throws in the `replace_if_exists` path,
    # `insertNoLock` must restore the previously replaced entity and leave
    # the disk in a consistent state. Verify this end-to-end via a failpoint.
    instance.query("DROP USER IF EXISTS u_replace")
    instance.query("CREATE USER u_replace IDENTIFIED WITH plaintext_password BY 'old_pw'")
    original = instance.query("SHOW CREATE USER u_replace")

    instance.query("SYSTEM ENABLE FAILPOINT disk_access_storage_write_entity_fails")
    try:
        error = instance.query_and_get_error(
            "CREATE USER OR REPLACE u_replace IDENTIFIED WITH plaintext_password BY 'new_pw'"
        )
        assert "FAULT_INJECTED" in error or "Injected fault" in error
    finally:
        instance.query("SYSTEM DISABLE FAILPOINT disk_access_storage_write_entity_fails")

    # The previous entity must still be there after the failed replace.
    assert instance.query("SHOW CREATE USER u_replace") == original

    # After persistency the replacement should still not be visible.
    instance.restart_clickhouse()
    assert instance.query("SHOW CREATE USER u_replace") == original

    instance.query("DROP USER u_replace")


def test_recovery_when_sql_file_is_missing():
    # Server should not fail to start when an entity is referenced from `users.list`
    # but its corresponding `<uuid>.sql` file is missing on disk. The list rebuild
    # recovery path in `readLists` should kick in and the server should start cleanly.
    instance.query("DROP USER IF EXISTS u_recover")
    instance.query("CREATE USER u_recover IDENTIFIED WITH no_password")

    user_id = instance.query(
        "SELECT id FROM system.users WHERE name = 'u_recover'"
    ).strip()
    assert user_id

    instance.stop_clickhouse()
    try:
        instance.exec_in_container(
            ["rm", "/var/lib/clickhouse/access/{}.sql".format(user_id)]
        )
    finally:
        instance.start_clickhouse()

    # The server has started; the broken entity must be gone from the list.
    assert (
        instance.query(
            "SELECT count() FROM system.users WHERE name = 'u_recover'"
        ).strip()
        == "0"
    )

    # Subsequent inserts continue to work after the recovery.
    instance.query("CREATE USER u_recover IDENTIFIED WITH no_password")
    assert (
        instance.query(
            "SELECT count() FROM system.users WHERE name = 'u_recover'"
        ).strip()
        == "1"
    )
    instance.query("DROP USER u_recover")
