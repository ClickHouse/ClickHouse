import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV
from textwrap import dedent

cluster = ClickHouseCluster(__file__, "test_normal_operations")
instance = cluster.add_instance(
    'instance',
    base_config_dir="configs",
    main_configs=["configs/config.d/foundationdb.xml"],
    user_configs=["configs/users.d/alice.xml"],
    with_foundationdb=True,
    stay_alive=True
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start(destroy_dirs=True)

        instance.query("CREATE TABLE test_table(x UInt32, y UInt32) ENGINE = MergeTree ORDER BY tuple()")
        instance.query("INSERT INTO test_table VALUES (1,5), (2,10)")

        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def cleanup_after_test():
    try:
        yield
    finally:
        instance.query("DROP USER IF EXISTS A, B")
        instance.query("DROP ROLE IF EXISTS R1, R2, R3, R4")


def test_load_access_storage_on_fdb():
    assert instance.query(f"SELECT name, type FROM system.user_directories") == TSV(
        [['config in fdb', 'ConfigFDBAccessStorage'], ['sql_driven in fdb', 'SqlDrivenFDBAccessStorage']])


def test_create_acl_entity_have_correct_privilege():
    instance.query("CREATE USER A")
    instance.query('CREATE ROLE R1')

    assert "Not enough privileges" in instance.query_and_get_error("SELECT * FROM test_table", user='A')

    instance.query('GRANT SELECT ON test_table TO R1')
    assert "Not enough privileges" in instance.query_and_get_error("SELECT * FROM test_table", user='A')

    instance.query('GRANT R1 TO A')
    assert instance.query("SELECT * FROM test_table", user='A') == "1\t5\n2\t10\n"

    instance.query('REVOKE R1 FROM A')
    assert "Not enough privileges" in instance.query_and_get_error("SELECT * FROM test_table", user='A')


def test_combine_privileges():
    instance.query("CREATE USER A ")
    instance.query('CREATE ROLE R1')
    instance.query('CREATE ROLE R2')

    assert "Not enough privileges" in instance.query_and_get_error("SELECT * FROM test_table", user='A')

    instance.query('GRANT R1 TO A')
    instance.query('GRANT SELECT(x) ON test_table TO R1')
    assert "Not enough privileges" in instance.query_and_get_error("SELECT * FROM test_table", user='A')
    assert instance.query("SELECT x FROM test_table", user='A') == "1\n2\n"

    instance.query('GRANT SELECT(y) ON test_table TO R2')
    instance.query('GRANT R2 TO A')
    assert instance.query("SELECT * FROM test_table", user='A') == "1\t5\n2\t10\n"


def test_grant_role_to_role():
    instance.query("CREATE USER A")
    instance.query('CREATE ROLE R1')
    instance.query('CREATE ROLE R2')

    assert "Not enough privileges" in instance.query_and_get_error("SELECT * FROM test_table", user='A')

    instance.query('GRANT R1 TO A')
    assert "Not enough privileges" in instance.query_and_get_error("SELECT * FROM test_table", user='A')

    instance.query('GRANT R2 TO R1')
    assert "Not enough privileges" in instance.query_and_get_error("SELECT * FROM test_table", user='A')

    instance.query('GRANT SELECT ON test_table TO R2')
    assert instance.query("SELECT * FROM test_table", user='A') == "1\t5\n2\t10\n"


def test_revoke_requires_admin_option():
    instance.query("CREATE USER A, B")
    instance.query("CREATE ROLE R1, R2")

    instance.query("GRANT R1 TO B")
    assert instance.query("SHOW GRANTS FOR B") == "GRANT R1 TO B\n"

    expected_error = "necessary to have the role R1 granted"
    assert expected_error in instance.query_and_get_error("REVOKE R1 FROM B", user='A')
    assert instance.query("SHOW GRANTS FOR B") == "GRANT R1 TO B\n"

    instance.query("GRANT R1 TO A")
    expected_error = "granted, but without ADMIN option"
    assert expected_error in instance.query_and_get_error("REVOKE R1 FROM B", user='A')
    assert instance.query("SHOW GRANTS FOR B") == "GRANT R1 TO B\n"

    instance.query("GRANT R1 TO A WITH ADMIN OPTION")
    instance.query("REVOKE R1 FROM B", user='A')
    assert instance.query("SHOW GRANTS FOR B") == ""

    instance.query("GRANT R1 TO B")
    assert instance.query("SHOW GRANTS FOR B") == "GRANT R1 TO B\n"
    instance.query("REVOKE ALL FROM B", user='A')
    assert instance.query("SHOW GRANTS FOR B") == ""

    instance.query("GRANT R1, R2 TO B")
    assert instance.query("SHOW GRANTS FOR B") == "GRANT R1, R2 TO B\n"
    expected_error = "necessary to have the role R2 granted"
    assert expected_error in instance.query_and_get_error("REVOKE ALL FROM B", user='A')
    assert instance.query("SHOW GRANTS FOR B") == "GRANT R1, R2 TO B\n"
    instance.query("REVOKE ALL EXCEPT R2 FROM B", user='A')
    assert instance.query("SHOW GRANTS FOR B") == "GRANT R2 TO B\n"
    instance.query("GRANT R2 TO A WITH ADMIN OPTION")
    instance.query("REVOKE ALL FROM B", user='A')
    assert instance.query("SHOW GRANTS FOR B") == ""

    instance.query("GRANT R1, R2 TO B")
    assert instance.query("SHOW GRANTS FOR B") == "GRANT R1, R2 TO B\n"
    instance.query("REVOKE ALL FROM B", user='A')
    assert instance.query("SHOW GRANTS FOR B") == ""


def test_normal_operations_on_sql_driven():
    user_name = "rose"

    instance.query(f"CREATE USER `{user_name}`")
    assert instance.query(f"SELECT name, storage FROM system.users WHERE name = '{user_name}'") == TSV(
        [[f'{user_name}', 'sql_driven in fdb']])

    old_user_name = "jack"
    new_user_name = "jacky"

    instance.query(dedent(f"""\
        CREATE USER `{old_user_name}`;
        ALTER USER `{old_user_name}` RENAME TO `{new_user_name}`;
    """))

    assert instance.query(f"SELECT COUNT() FROM system.users WHERE name = '{old_user_name}'").strip() == "0"
    assert instance.query(f"SELECT COUNT() FROM system.users WHERE name = '{new_user_name}'").strip() == "1"


def test_create_should_persist_on_sql_driven():
    user_name = "test"
    instance.query(f"CREATE USER '{user_name}'")
    instance.query(f"DROP USER IF EXISTS '{user_name}'")

    instance.restart_clickhouse()
    assert instance.query(f"SELECT count() FROM system.users WHERE name = '{user_name}'").strip() == "0"


def test_alter_should_persist_on_sql_driven():
    quota_name = "qA"
    quota_name_new = "QuotaA"

    instance.query(f"CREATE QUOTA {quota_name} FOR INTERVAL 15 month MAX queries = 123 TO CURRENT_USER")
    instance.query(f"ALTER QUOTA IF EXISTS {quota_name} RENAME TO {quota_name_new}")

    instance.restart_clickhouse()

    assert instance.query(
        f"SELECT name FROM system.quotas WHERE storage = 'sql_driven in fdb'").strip() == f"{quota_name_new}"


def test_drop_should_persist_on_sql_driven():
    role_name = "accountant"

    instance.query(dedent(f"""\
    CREATE ROLE {role_name};
    GRANT SELECT ON test_table TO {role_name};
    """))
    assert instance.query(f"SELECT count() FROM system.roles WHERE name = '{role_name}'").strip() == "1"

    instance.query(f"DROP ROLE IF EXISTS '{role_name}'")

    instance.restart_clickhouse()

    assert instance.query(f"SELECT count() FROM system.roles WHERE name = '{role_name}'").strip() == "0"


def test_startup_should_persist_on_config():
    assert instance.query(f"SELECT name, storage FROM system.users WHERE name = 'alice'") == TSV(
        [["alice", "config in fdb"]]
    )
    assert instance.query(f"SELECT name, storage FROM system.users WHERE name = 'default'") == TSV(
        [["default", "config in fdb"]]
    )


def test_restart_should_persist_on_config():
    assert instance.query(f"SELECT name, storage FROM system.users WHERE name = 'alice'") == TSV(
        [["alice", "config in fdb"]]
    )
    assert instance.query(f"SELECT name, storage FROM system.users WHERE name = 'default'") == TSV(
        [["default", "config in fdb"]]
    )

    instance.restart_clickhouse()

    assert instance.query(f"SELECT name, storage FROM system.users WHERE name = 'alice'") == TSV(
        [["alice", "config in fdb"]]
    )
    assert instance.query(f"SELECT name, storage FROM system.users WHERE name = 'default'") == TSV(
        [["default", "config in fdb"]]
    )


def test_readonly_on_config():
    assert instance.query(f"SELECT name, storage FROM system.users WHERE name = 'alice'") == TSV(
        [["alice", "config in fdb"]]
    )

    # remove
    assert "Cannot remove user `alice` from config in fdb because this storage is readonly." \
           in instance.query_and_get_error(f"DROP USER alice")

    # rename
    assert "Cannot update user `alice` in config in fdb because this storage is readonly." \
           in instance.query_and_get_error(f"ALTER USER alice RENAME TO Alice")

    # update
    role_name = "accountant"

    instance.query(dedent(f"""\
    CREATE ROLE {role_name};
    GRANT SELECT ON test_table TO {role_name};
    """))
    assert instance.query(f"SELECT count() FROM system.roles WHERE name = '{role_name}'").strip() == "1"

    assert "Cannot update user `alice` in config in fdb because this storage is readonly." \
           in instance.query_and_get_error(f"ALTER USER alice DEFAULT ROLE {role_name}")


def create_entities():
    instance.query("CREATE SETTINGS PROFILE s1 SETTINGS max_memory_usage = 123456789 MIN 100000000 MAX 200000000")
    instance.query("CREATE USER u1 SETTINGS PROFILE s1")
    instance.query("CREATE ROLE rx SETTINGS PROFILE s1")
    instance.query("CREATE USER u2 IDENTIFIED BY 'qwerty' HOST LOCAL DEFAULT ROLE rx")
    instance.query("CREATE SETTINGS PROFILE s2 SETTINGS PROFILE s1 TO u2")
    instance.query("CREATE ROW POLICY p ON mydb.mytable FOR SELECT USING a<1000 TO u1, u2")
    instance.query("CREATE QUOTA q FOR INTERVAL 1 HOUR MAX QUERIES 100 TO ALL EXCEPT rx")


@pytest.fixture(autouse=True)
def drop_entities():
    instance.query("DROP USER IF EXISTS u1, u2")
    instance.query("DROP ROLE IF EXISTS rx, ry")
    instance.query("DROP ROW POLICY IF EXISTS p ON mydb.mytable")
    instance.query("DROP QUOTA IF EXISTS q")
    instance.query("DROP SETTINGS PROFILE IF EXISTS s1, s2")


def test_create_on_sql_driven():
    create_entities()

    def check():
        assert instance.query("SHOW CREATE USER u1") == "CREATE USER u1 SETTINGS PROFILE s1\n"
        assert instance.query(
            "SHOW CREATE USER u2") == "CREATE USER u2 IDENTIFIED WITH sha256_password HOST LOCAL DEFAULT ROLE rx\n"
        assert instance.query(
            "SHOW CREATE ROW POLICY p ON mydb.mytable") == "CREATE ROW POLICY p ON mydb.mytable FOR SELECT USING a < 1000 TO u1, u2\n"
        assert instance.query(
            "SHOW CREATE QUOTA q") == "CREATE QUOTA q FOR INTERVAL 1 hour MAX queries = 100 TO ALL EXCEPT rx\n"
        assert instance.query("SHOW GRANTS FOR u1") == ""
        assert instance.query("SHOW GRANTS FOR u2") == "GRANT rx TO u2\n"
        assert instance.query("SHOW CREATE ROLE rx") == "CREATE ROLE rx SETTINGS PROFILE s1\n"
        assert instance.query("SHOW GRANTS FOR rx") == ""
        assert instance.query(
            "SHOW CREATE SETTINGS PROFILE s1") == "CREATE SETTINGS PROFILE s1 SETTINGS max_memory_usage = 123456789 MIN 100000000 MAX 200000000\n"
        assert instance.query(
            "SHOW CREATE SETTINGS PROFILE s2") == "CREATE SETTINGS PROFILE s2 SETTINGS INHERIT s1 TO u2\n"

    check()
    instance.restart_clickhouse()  # Check persistency
    check()


def test_alter_on_sql_driven():
    create_entities()
    instance.restart_clickhouse()

    instance.query("CREATE ROLE ry")
    instance.query("GRANT ry TO u2")
    instance.query("ALTER USER u2 DEFAULT ROLE ry")
    instance.query("GRANT rx TO ry WITH ADMIN OPTION")
    instance.query("ALTER ROLE rx SETTINGS PROFILE s2")
    instance.query("GRANT SELECT ON mydb.mytable TO u1")
    instance.query("GRANT SELECT ON mydb.* TO rx WITH GRANT OPTION")
    instance.query("ALTER SETTINGS PROFILE s1 SETTINGS max_memory_usage = 987654321 READONLY")

    def check():
        assert instance.query("SHOW CREATE USER u1") == "CREATE USER u1 SETTINGS PROFILE s1\n"
        assert instance.query(
            "SHOW CREATE USER u2") == "CREATE USER u2 IDENTIFIED WITH sha256_password HOST LOCAL DEFAULT ROLE ry\n"
        assert instance.query("SHOW GRANTS FOR u1") == "GRANT SELECT ON mydb.mytable TO u1\n"
        assert instance.query("SHOW GRANTS FOR u2") == "GRANT rx, ry TO u2\n"
        assert instance.query("SHOW CREATE ROLE rx") == "CREATE ROLE rx SETTINGS PROFILE s2\n"
        assert instance.query("SHOW CREATE ROLE ry") == "CREATE ROLE ry\n"
        assert instance.query("SHOW GRANTS FOR rx") == "GRANT SELECT ON mydb.* TO rx WITH GRANT OPTION\n"
        assert instance.query("SHOW GRANTS FOR ry") == "GRANT rx TO ry WITH ADMIN OPTION\n"
        assert instance.query(
            "SHOW CREATE SETTINGS PROFILE s1") == "CREATE SETTINGS PROFILE s1 SETTINGS max_memory_usage = 987654321 READONLY\n"
        assert instance.query(
            "SHOW CREATE SETTINGS PROFILE s2") == "CREATE SETTINGS PROFILE s2 SETTINGS INHERIT s1 TO u2\n"

    check()
    instance.restart_clickhouse()  # Check persistency
    check()


def test_drop_on_sql_driven():
    create_entities()
    instance.restart_clickhouse()

    instance.query("DROP USER u2")
    instance.query("DROP ROLE rx")
    instance.query("DROP ROW POLICY p ON mydb.mytable")
    instance.query("DROP QUOTA q")
    instance.query("DROP SETTINGS PROFILE s1")

    def check():
        assert instance.query("SHOW CREATE USER u1") == "CREATE USER u1\n"
        assert instance.query("SHOW CREATE SETTINGS PROFILE s2") == "CREATE SETTINGS PROFILE s2\n"
        assert "There is no user `u2`" in instance.query_and_get_error("SHOW CREATE USER u2")
        assert "There is no row policy `p ON mydb.mytable` in user directories." in instance.query_and_get_error(
            "SHOW CREATE ROW POLICY p ON mydb.mytable")
        assert "There is no quota `q` in user directories." in instance.query_and_get_error("SHOW CREATE QUOTA q")

    check()
    instance.restart_clickhouse()  # Check persistency
    check()
