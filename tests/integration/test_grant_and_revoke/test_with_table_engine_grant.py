import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/config_with_table_engine_grant.xml"],
    user_configs=["configs/users.d/users.xml"],
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()

        instance.query("CREATE DATABASE test")
        instance.query(
            "CREATE TABLE test.table(x UInt32, y UInt32) ENGINE = MergeTree ORDER BY tuple()"
        )
        instance.query(
            "CREATE TABLE test.table2(x UInt32, y UInt32) ENGINE = MergeTree ORDER BY tuple()"
        )
        instance.query("INSERT INTO test.table VALUES (1,5), (2,10)")

        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def cleanup_after_test():
    try:
        yield
    finally:
        instance.query("DROP USER IF EXISTS A, B, C")

        instance.query("DROP TABLE IF EXISTS test.view_1, test.view_2, default.table")


def test_smoke():
    instance.query("CREATE USER A")
    assert "Not enough privileges" in instance.query_and_get_error(
        "SELECT * FROM test.table", user="A"
    )

    instance.query("GRANT SELECT ON test.table TO A")
    assert instance.query("SELECT * FROM test.table", user="A") == "1\t5\n2\t10\n"

    instance.query("REVOKE SELECT ON test.table FROM A")
    assert "Not enough privileges" in instance.query_and_get_error(
        "SELECT * FROM test.table", user="A"
    )


def test_grant_option():
    instance.query("CREATE USER A")
    instance.query("CREATE USER B")

    instance.query("GRANT SELECT ON test.table TO A")
    assert instance.query("SELECT * FROM test.table", user="A") == "1\t5\n2\t10\n"
    assert "Not enough privileges" in instance.query_and_get_error(
        "GRANT SELECT ON test.table TO B", user="A"
    )

    instance.query("GRANT SELECT ON test.table TO A WITH GRANT OPTION")
    instance.query("GRANT SELECT ON test.table TO B", user="A")
    assert instance.query("SELECT * FROM test.table", user="B") == "1\t5\n2\t10\n"

    instance.query("REVOKE SELECT ON test.table FROM A, B")


def test_revoke_requires_grant_option():
    instance.query("CREATE USER A")
    instance.query("CREATE USER B")

    instance.query("GRANT SELECT ON test.table TO B")
    assert instance.query("SHOW GRANTS FOR B") == "GRANT SELECT ON test.`table` TO B\n"

    expected_error = "Not enough privileges"
    assert expected_error in instance.query_and_get_error(
        "REVOKE SELECT ON test.table FROM B", user="A"
    )
    assert instance.query("SHOW GRANTS FOR B") == "GRANT SELECT ON test.`table` TO B\n"

    instance.query("GRANT SELECT ON test.table TO A")
    expected_error = "privileges have been granted, but without grant option"
    assert expected_error in instance.query_and_get_error(
        "REVOKE SELECT ON test.table FROM B", user="A"
    )
    assert instance.query("SHOW GRANTS FOR B") == "GRANT SELECT ON test.`table` TO B\n"

    instance.query("GRANT SELECT ON test.table TO A WITH GRANT OPTION")
    assert instance.query("SHOW GRANTS FOR B") == "GRANT SELECT ON test.`table` TO B\n"
    instance.query("REVOKE SELECT ON test.table FROM B", user="A")
    assert instance.query("SHOW GRANTS FOR B") == ""

    instance.query("GRANT SELECT ON test.table TO B")
    assert instance.query("SHOW GRANTS FOR B") == "GRANT SELECT ON test.`table` TO B\n"
    instance.query("REVOKE SELECT ON test.* FROM B", user="A")
    assert instance.query("SHOW GRANTS FOR B") == ""

    instance.query("GRANT SELECT ON test.table TO B")
    assert instance.query("SHOW GRANTS FOR B") == "GRANT SELECT ON test.`table` TO B\n"
    instance.query("REVOKE ALL ON test.* FROM B", user="A")
    assert instance.query("SHOW GRANTS FOR B") == ""

    instance.query("GRANT SELECT ON test.table TO B")
    assert instance.query("SHOW GRANTS FOR B") == "GRANT SELECT ON test.`table` TO B\n"
    instance.query("REVOKE ALL ON *.* FROM B", user="A")
    assert instance.query("SHOW GRANTS FOR B") == ""

    instance.query("REVOKE GRANT OPTION FOR ALL ON *.* FROM A")
    instance.query("GRANT SELECT ON test.table TO B")
    assert instance.query("SHOW GRANTS FOR B") == "GRANT SELECT ON test.`table` TO B\n"
    expected_error = "privileges have been granted, but without grant option"
    assert expected_error in instance.query_and_get_error(
        "REVOKE SELECT ON test.table FROM B", user="A"
    )
    assert instance.query("SHOW GRANTS FOR B") == "GRANT SELECT ON test.`table` TO B\n"

    instance.query("GRANT SELECT ON test.* TO A WITH GRANT OPTION")
    instance.query("GRANT SELECT ON test.table TO B")
    assert instance.query("SHOW GRANTS FOR B") == "GRANT SELECT ON test.`table` TO B\n"
    instance.query("REVOKE SELECT ON test.table FROM B", user="A")
    assert instance.query("SHOW GRANTS FOR B") == ""


def test_allowed_grantees():
    instance.query("CREATE USER A")
    instance.query("CREATE USER B")

    instance.query("GRANT SELECT ON test.table TO A WITH GRANT OPTION")
    instance.query("GRANT SELECT ON test.table TO B", user="A")
    assert instance.query("SELECT * FROM test.table", user="B") == "1\t5\n2\t10\n"
    instance.query("REVOKE SELECT ON test.table FROM B", user="A")

    instance.query("ALTER USER A GRANTEES NONE")
    expected_error = "user `B` is not allowed as grantee"
    assert expected_error in instance.query_and_get_error(
        "GRANT SELECT ON test.table TO B", user="A"
    )

    instance.query("ALTER USER A GRANTEES ANY EXCEPT B")
    assert (
        instance.query("SHOW CREATE USER A")
        == "CREATE USER A IDENTIFIED WITH no_password GRANTEES ANY EXCEPT B\n"
    )
    expected_error = "user `B` is not allowed as grantee"
    assert expected_error in instance.query_and_get_error(
        "GRANT SELECT ON test.table TO B", user="A"
    )

    instance.query("ALTER USER A GRANTEES B")
    instance.query("GRANT SELECT ON test.table TO B", user="A")
    assert instance.query("SELECT * FROM test.table", user="B") == "1\t5\n2\t10\n"
    instance.query("REVOKE SELECT ON test.table FROM B", user="A")

    instance.query("ALTER USER A GRANTEES ANY")
    assert (
        instance.query("SHOW CREATE USER A")
        == "CREATE USER A IDENTIFIED WITH no_password\n"
    )
    instance.query("GRANT SELECT ON test.table TO B", user="A")
    assert instance.query("SELECT * FROM test.table", user="B") == "1\t5\n2\t10\n"

    instance.query("ALTER USER A GRANTEES NONE")
    expected_error = "user `B` is not allowed as grantee"
    assert expected_error in instance.query_and_get_error(
        "REVOKE SELECT ON test.table FROM B", user="A"
    )

    instance.query("CREATE USER C GRANTEES ANY EXCEPT C")
    assert (
        instance.query("SHOW CREATE USER C")
        == "CREATE USER C IDENTIFIED WITH no_password GRANTEES ANY EXCEPT C\n"
    )
    instance.query("GRANT SELECT ON test.table TO C WITH GRANT OPTION")
    assert instance.query("SELECT * FROM test.table", user="C") == "1\t5\n2\t10\n"
    expected_error = "user `C` is not allowed as grantee"
    assert expected_error in instance.query_and_get_error(
        "REVOKE SELECT ON test.table FROM C", user="C"
    )


def test_grant_all_on_table():
    instance.query("CREATE USER A, B")
    instance.query("GRANT ALL ON test.table TO A WITH GRANT OPTION")
    instance.query("GRANT ALL ON test.table TO B", user="A")
    assert (
        instance.query("SHOW GRANTS FOR B")
        == "GRANT SHOW TABLES, SHOW COLUMNS, SHOW DICTIONARIES, SELECT, INSERT, ALTER TABLE, ALTER VIEW, CREATE TABLE, CREATE VIEW, CREATE DICTIONARY, DROP TABLE, DROP VIEW, DROP DICTIONARY, UNDROP TABLE, TRUNCATE, OPTIMIZE, BACKUP, CREATE ROW POLICY, ALTER ROW POLICY, DROP ROW POLICY, SHOW ROW POLICIES, SYSTEM MERGES, SYSTEM TTL MERGES, SYSTEM FETCHES, SYSTEM MOVES, SYSTEM PULLING REPLICATION LOG, SYSTEM CLEANUP, SYSTEM VIEWS, SYSTEM SENDS, SYSTEM REPLICATION QUEUES, SYSTEM VIRTUAL PARTS UPDATE, SYSTEM DROP REPLICA, SYSTEM SYNC REPLICA, SYSTEM RESTART REPLICA, SYSTEM RESTORE REPLICA, SYSTEM WAIT LOADING PARTS, SYSTEM FLUSH DISTRIBUTED, SYSTEM UNLOAD PRIMARY KEY, dictGet ON test.`table` TO B\n"
    )
    instance.query("REVOKE ALL ON test.table FROM B", user="A")
    assert instance.query("SHOW GRANTS FOR B") == ""


def test_implicit_show_grants():
    instance.query("CREATE USER A")
    assert (
        instance.query(
            "select count() FROM system.databases WHERE name='test'", user="A"
        )
        == "0\n"
    )
    assert (
        instance.query(
            "select count() FROM system.tables WHERE database='test' AND name='table'",
            user="A",
        )
        == "0\n"
    )
    assert (
        instance.query(
            "select count() FROM system.columns WHERE database='test' AND table='table'",
            user="A",
        )
        == "0\n"
    )

    instance.query("GRANT SELECT(x) ON test.table TO A")
    assert (
        instance.query("SHOW GRANTS FOR A") == "GRANT SELECT(x) ON test.`table` TO A\n"
    )
    assert (
        instance.query(
            "select count() FROM system.databases WHERE name='test'", user="A"
        )
        == "1\n"
    )
    assert (
        instance.query(
            "select count() FROM system.tables WHERE database='test' AND name='table'",
            user="A",
        )
        == "1\n"
    )
    assert (
        instance.query(
            "select count() FROM system.columns WHERE database='test' AND table='table'",
            user="A",
        )
        == "1\n"
    )

    instance.query("GRANT SELECT ON test.table TO A")
    assert instance.query("SHOW GRANTS FOR A") == "GRANT SELECT ON test.`table` TO A\n"
    assert (
        instance.query(
            "select count() FROM system.databases WHERE name='test'", user="A"
        )
        == "1\n"
    )
    assert (
        instance.query(
            "select count() FROM system.tables WHERE database='test' AND name='table'",
            user="A",
        )
        == "1\n"
    )
    assert (
        instance.query(
            "select count() FROM system.columns WHERE database='test' AND table='table'",
            user="A",
        )
        == "2\n"
    )

    instance.query("GRANT SELECT ON test.* TO A")
    assert instance.query("SHOW GRANTS FOR A") == "GRANT SELECT ON test.* TO A\n"
    assert (
        instance.query(
            "select count() FROM system.databases WHERE name='test'", user="A"
        )
        == "1\n"
    )
    assert (
        instance.query(
            "select count() FROM system.tables WHERE database='test' AND name='table'",
            user="A",
        )
        == "1\n"
    )
    assert (
        instance.query(
            "select count() FROM system.columns WHERE database='test' AND table='table'",
            user="A",
        )
        == "2\n"
    )

    instance.query("GRANT SELECT ON *.* TO A")
    assert instance.query("SHOW GRANTS FOR A") == "GRANT SELECT ON *.* TO A\n"
    assert (
        instance.query(
            "select count() FROM system.databases WHERE name='test'", user="A"
        )
        == "1\n"
    )
    assert (
        instance.query(
            "select count() FROM system.tables WHERE database='test' AND name='table'",
            user="A",
        )
        == "1\n"
    )
    assert (
        instance.query(
            "select count() FROM system.columns WHERE database='test' AND table='table'",
            user="A",
        )
        == "2\n"
    )

    instance.query("REVOKE ALL ON *.* FROM A")
    assert (
        instance.query(
            "select count() FROM system.databases WHERE name='test'", user="A"
        )
        == "0\n"
    )
    assert (
        instance.query(
            "select count() FROM system.tables WHERE database='test' AND name='table'",
            user="A",
        )
        == "0\n"
    )
    assert (
        instance.query(
            "select count() FROM system.columns WHERE database='test' AND table='table'",
            user="A",
        )
        == "0\n"
    )


def test_implicit_create_view_grant():
    instance.query("CREATE USER A")
    expected_error = "Not enough privileges"
    assert expected_error in instance.query_and_get_error(
        "CREATE VIEW test.view_1 AS SELECT 1", user="A"
    )

    instance.query("GRANT CREATE TABLE ON test.* TO A")
    instance.query("CREATE VIEW test.view_1 AS SELECT 1", user="A")
    assert instance.query("SELECT * FROM test.view_1") == "1\n"

    instance.query("REVOKE CREATE TABLE ON test.* FROM A")
    instance.query("DROP TABLE test.view_1")
    assert expected_error in instance.query_and_get_error(
        "CREATE VIEW test.view_1 AS SELECT 1", user="A"
    )

    # check grant option
    instance.query("CREATE USER B")
    expected_error = "Not enough privileges"
    assert expected_error in instance.query_and_get_error(
        "GRANT CREATE VIEW ON test.* TO B", user="A"
    )

    instance.query("GRANT CREATE TABLE ON test.* TO A WITH GRANT OPTION")
    instance.query("GRANT CREATE VIEW ON test.* TO B", user="A")
    instance.query("CREATE VIEW test.view_2 AS SELECT 1", user="B")
    assert instance.query("SELECT * FROM test.view_2") == "1\n"
    instance.query("DROP USER A")
    instance.query("DROP VIEW test.view_2")


def test_implicit_create_temporary_table_grant():
    instance.query("CREATE USER A")
    expected_error = "Not enough privileges"
    assert expected_error in instance.query_and_get_error(
        "CREATE TEMPORARY TABLE tmp(name String)", user="A"
    )

    instance.query("GRANT CREATE TABLE ON test.* TO A")
    instance.query("GRANT TABLE ENGINE ON Memory TO A")
    instance.query("CREATE TEMPORARY TABLE tmp(name String)", user="A")

    instance.query("REVOKE CREATE TABLE ON *.* FROM A")
    assert expected_error in instance.query_and_get_error(
        "CREATE TEMPORARY TABLE tmp(name String)", user="A"
    )


def test_introspection():
    instance.query("CREATE USER A")
    instance.query("CREATE USER B")
    instance.query("GRANT SELECT ON test.table TO A")
    instance.query("GRANT CREATE ON *.* TO B WITH GRANT OPTION")

    assert instance.query("SHOW USERS") == TSV(["A", "B", "default"])
    assert instance.query("SHOW CREATE USERS A") == TSV(
        ["CREATE USER A IDENTIFIED WITH no_password"]
    )
    assert instance.query("SHOW CREATE USERS B") == TSV(
        ["CREATE USER B IDENTIFIED WITH no_password"]
    )
    assert instance.query("SHOW CREATE USERS A,B") == TSV(
        [
            "CREATE USER A IDENTIFIED WITH no_password",
            "CREATE USER B IDENTIFIED WITH no_password",
        ]
    )
    assert instance.query("SHOW CREATE USERS") == TSV(
        [
            "CREATE USER A IDENTIFIED WITH no_password",
            "CREATE USER B IDENTIFIED WITH no_password",
            "CREATE USER default IDENTIFIED WITH plaintext_password SETTINGS PROFILE `default`",
        ]
    )

    assert instance.query("SHOW GRANTS FOR A") == TSV(
        ["GRANT SELECT ON test.`table` TO A"]
    )
    assert instance.query("SHOW GRANTS FOR B") == TSV(
        ["GRANT CREATE ON *.* TO B WITH GRANT OPTION"]
    )
    assert instance.query("SHOW GRANTS FOR default") == TSV(
        ["GRANT ALL ON *.* TO default WITH GRANT OPTION"]
    )
    assert instance.query("SHOW GRANTS FOR A,B") == TSV(
        [
            "GRANT SELECT ON test.`table` TO A",
            "GRANT CREATE ON *.* TO B WITH GRANT OPTION",
        ]
    )
    assert instance.query("SHOW GRANTS FOR B,A") == TSV(
        [
            "GRANT SELECT ON test.`table` TO A",
            "GRANT CREATE ON *.* TO B WITH GRANT OPTION",
        ]
    )
    assert instance.query("SHOW GRANTS FOR ALL") == TSV(
        [
            "GRANT SELECT ON test.`table` TO A",
            "GRANT CREATE ON *.* TO B WITH GRANT OPTION",
            "GRANT ALL ON *.* TO default WITH GRANT OPTION",
        ]
    )

    assert instance.query("SHOW GRANTS", user="A") == TSV(
        ["GRANT SELECT ON test.`table` TO A"]
    )
    assert instance.query("SHOW GRANTS", user="B") == TSV(
        ["GRANT CREATE ON *.* TO B WITH GRANT OPTION"]
    )

    assert instance.query("SHOW GRANTS FOR ALL", user="A") == TSV(
        ["GRANT SELECT ON test.`table` TO A"]
    )
    assert instance.query("SHOW GRANTS FOR ALL", user="B") == TSV(
        ["GRANT CREATE ON *.* TO B WITH GRANT OPTION"]
    )
    assert instance.query("SHOW GRANTS FOR ALL") == TSV(
        [
            "GRANT SELECT ON test.`table` TO A",
            "GRANT CREATE ON *.* TO B WITH GRANT OPTION",
            "GRANT ALL ON *.* TO default WITH GRANT OPTION",
        ]
    )

    expected_error = "necessary to have the grant SHOW USERS"
    assert expected_error in instance.query_and_get_error("SHOW GRANTS FOR B", user="A")

    expected_access1 = (
        "CREATE USER A IDENTIFIED WITH no_password\n"
        "CREATE USER B IDENTIFIED WITH no_password\n"
        "CREATE USER default IDENTIFIED WITH plaintext_password SETTINGS PROFILE `default`"
    )
    expected_access2 = (
        "GRANT SELECT ON test.`table` TO A\n"
        "GRANT CREATE ON *.* TO B WITH GRANT OPTION\n"
        "GRANT ALL ON *.* TO default WITH GRANT OPTION\n"
    )
    assert expected_access1 in instance.query("SHOW ACCESS")
    assert expected_access2 in instance.query("SHOW ACCESS")

    assert instance.query(
        "SELECT name, storage, auth_type, auth_params, host_ip, host_names, host_names_regexp, host_names_like, default_roles_all, default_roles_list, default_roles_except from system.users WHERE name IN ('A', 'B') ORDER BY name"
    ) == TSV(
        [
            [
                "A",
                "local_directory",
                "['no_password']",
                "['{}']",
                "['::/0']",
                "[]",
                "[]",
                "[]",
                1,
                "[]",
                "[]",
            ],
            [
                "B",
                "local_directory",
                "['no_password']",
                "['{}']",
                "['::/0']",
                "[]",
                "[]",
                "[]",
                1,
                "[]",
                "[]",
            ],
        ]
    )

    assert instance.query(
        "SELECT * from system.grants WHERE user_name IN ('A', 'B') ORDER BY user_name, access_type, grant_option"
    ) == TSV(
        [
            ["A", "\\N", "SELECT", "test", "table", "\\N", 0, 0],
            ["B", "\\N", "CREATE", "\\N", "\\N", "\\N", 0, 1],
        ]
    )


def test_current_database():
    instance.query("CREATE USER A")
    instance.query("GRANT SELECT ON table TO A", database="test")

    assert instance.query("SHOW GRANTS FOR A") == TSV(
        ["GRANT SELECT ON test.`table` TO A"]
    )
    assert instance.query("SHOW GRANTS FOR A", database="test") == TSV(
        ["GRANT SELECT ON test.`table` TO A"]
    )

    assert instance.query("SELECT * FROM test.table", user="A") == "1\t5\n2\t10\n"
    assert (
        instance.query("SELECT * FROM table", user="A", database="test")
        == "1\t5\n2\t10\n"
    )

    instance.query(
        "CREATE TABLE default.table(x UInt32, y UInt32) ENGINE = MergeTree ORDER BY tuple()"
    )
    assert "Not enough privileges" in instance.query_and_get_error(
        "SELECT * FROM table", user="A"
    )
    instance.query("DROP TABLE default.table SYNC")


def test_grant_with_replace_option():
    instance.query("CREATE USER A")
    instance.query("GRANT SELECT ON test.table TO A")
    assert instance.query("SHOW GRANTS FOR A") == TSV(
        ["GRANT SELECT ON test.`table` TO A"]
    )

    instance.query("GRANT INSERT ON test.table TO A WITH REPLACE OPTION")
    assert instance.query("SHOW GRANTS FOR A") == TSV(
        ["GRANT INSERT ON test.`table` TO A"]
    )

    instance.query("GRANT NONE ON *.* TO A WITH REPLACE OPTION")
    assert instance.query("SHOW GRANTS FOR A") == TSV([])

    instance.query("CREATE USER B")
    instance.query("GRANT SELECT ON test.table TO B")
    assert instance.query("SHOW GRANTS FOR A") == TSV([])
    assert instance.query("SHOW GRANTS FOR B") == TSV(
        ["GRANT SELECT ON test.`table` TO B"]
    )

    expected_error = (
        "it's necessary to have the grant INSERT ON test.`table` WITH GRANT OPTION"
    )
    assert expected_error in instance.query_and_get_error(
        "GRANT INSERT ON test.`table` TO B WITH REPLACE OPTION", user="A"
    )
    assert instance.query("SHOW GRANTS FOR A") == TSV([])
    assert instance.query("SHOW GRANTS FOR B") == TSV(
        ["GRANT SELECT ON test.`table` TO B"]
    )

    instance.query("GRANT INSERT ON test.table TO A WITH GRANT OPTION")
    expected_error = (
        "it's necessary to have the grant SELECT ON test.`table` WITH GRANT OPTION"
    )
    assert expected_error in instance.query_and_get_error(
        "GRANT INSERT ON test.`table` TO B WITH REPLACE OPTION", user="A"
    )
    assert instance.query("SHOW GRANTS FOR A") == TSV(
        ["GRANT INSERT ON test.`table` TO A WITH GRANT OPTION"]
    )
    assert instance.query("SHOW GRANTS FOR B") == TSV(
        ["GRANT SELECT ON test.`table` TO B"]
    )

    instance.query("GRANT SELECT ON test.`table` TO A WITH GRANT OPTION")
    instance.query("GRANT INSERT ON test.`table` TO B WITH REPLACE OPTION", user="A")
    assert instance.query("SHOW GRANTS FOR A") == TSV(
        ["GRANT SELECT, INSERT ON test.`table` TO A WITH GRANT OPTION"]
    )
    assert instance.query("SHOW GRANTS FOR B") == TSV(
        ["GRANT INSERT ON test.`table` TO B"]
    )


def test_grant_current_grants():
    instance.query("CREATE USER A")
    instance.query(
        "GRANT SELECT, CREATE TABLE, CREATE VIEW ON test.* TO A WITH GRANT OPTION"
    )
    assert instance.query("SHOW GRANTS FOR A") == TSV(
        ["GRANT SELECT, CREATE TABLE, CREATE VIEW ON test.* TO A WITH GRANT OPTION"]
    )

    instance.query("CREATE USER B")
    instance.query("GRANT CURRENT GRANTS ON *.* TO B", user="A")
    assert instance.query("SHOW GRANTS FOR B") == TSV(
        ["GRANT SELECT, CREATE TABLE, CREATE VIEW ON test.* TO B"]
    )

    instance.query("CREATE USER C")
    instance.query("GRANT CURRENT GRANTS(CREATE ON test.*) TO C", user="A")
    assert instance.query("SHOW GRANTS FOR C") == TSV(
        ["GRANT CREATE TABLE, CREATE VIEW ON test.* TO C"]
    )

    instance.query("DROP USER IF EXISTS C")
    instance.query("CREATE USER C")
    instance.query("GRANT CURRENT GRANTS(NONE ON *.*) TO C", user="A")
    assert instance.query("SHOW GRANTS FOR C") == TSV([])


def test_grant_current_grants_with_partial_revoke():
    instance.query("CREATE USER A")
    instance.query("GRANT CREATE TABLE ON *.* TO A")
    instance.query("REVOKE CREATE TABLE ON test.* FROM A")
    instance.query("GRANT CREATE TABLE ON test.table TO A WITH GRANT OPTION")
    instance.query("GRANT SELECT ON *.* TO A WITH GRANT OPTION")
    instance.query("REVOKE SELECT ON test.* FROM A")
    instance.query("GRANT SELECT ON test.table TO A WITH GRANT OPTION")
    instance.query("GRANT SELECT ON test.table2 TO A")

    assert instance.query("SHOW GRANTS FOR A") == TSV(
        [
            "GRANT CREATE TABLE ON *.* TO A",
            "GRANT SELECT ON *.* TO A WITH GRANT OPTION",
            "REVOKE SELECT, CREATE TABLE ON test.* FROM A",
            "GRANT SELECT, CREATE TABLE ON test.`table` TO A WITH GRANT OPTION",
            "GRANT SELECT ON test.table2 TO A",
        ]
    )

    instance.query("CREATE USER B")
    instance.query("GRANT CURRENT GRANTS ON *.* TO B", user="A")
    assert instance.query("SHOW GRANTS FOR B") == TSV(
        [
            "GRANT SELECT ON *.* TO B",
            "REVOKE SELECT ON test.* FROM B",
            "GRANT SELECT, CREATE TABLE ON test.`table` TO B",
        ]
    )

    instance.query("DROP USER IF EXISTS B")
    instance.query("CREATE USER B")
    instance.query("GRANT CURRENT GRANTS ON *.* TO B WITH GRANT OPTION", user="A")
    assert instance.query("SHOW GRANTS FOR B") == TSV(
        [
            "GRANT SELECT ON *.* TO B WITH GRANT OPTION",
            "REVOKE SELECT ON test.* FROM B",
            "GRANT SELECT, CREATE TABLE ON test.`table` TO B WITH GRANT OPTION",
        ]
    )

    instance.query("DROP USER IF EXISTS C")
    instance.query("CREATE USER C")
    instance.query("GRANT SELECT ON test.* TO B")
    instance.query("GRANT CURRENT GRANTS ON *.* TO C", user="B")
    assert instance.query("SHOW GRANTS FOR C") == TSV(
        [
            "GRANT SELECT ON *.* TO C",
            "GRANT CREATE TABLE ON test.`table` TO C",
        ]
    )

    instance.query("DROP USER IF EXISTS B")
    instance.query("CREATE USER B")
    instance.query("GRANT CURRENT GRANTS ON test.* TO B WITH GRANT OPTION", user="A")
    assert instance.query("SHOW GRANTS FOR B") == TSV(
        [
            "GRANT SELECT, CREATE TABLE ON test.`table` TO B WITH GRANT OPTION",
        ]
    )


def test_current_grants_override():
    instance.query("CREATE USER A")
    instance.query("GRANT SELECT ON *.* TO A WITH GRANT OPTION")
    instance.query("REVOKE SELECT ON test.* FROM A")
    assert instance.query("SHOW GRANTS FOR A") == TSV(
        [
            "GRANT SELECT ON *.* TO A WITH GRANT OPTION",
            "REVOKE SELECT ON test.* FROM A",
        ]
    )

    instance.query("CREATE USER B")
    instance.query("GRANT SELECT ON test.table TO B")
    assert instance.query("SHOW GRANTS FOR B") == TSV(
        ["GRANT SELECT ON test.`table` TO B"]
    )

    instance.query("GRANT CURRENT GRANTS ON *.* TO B", user="A")
    assert instance.query("SHOW GRANTS FOR B") == TSV(
        [
            "GRANT SELECT ON *.* TO B",
            "REVOKE SELECT ON test.* FROM B",
            "GRANT SELECT ON test.`table` TO B",
        ]
    )

    instance.query("DROP USER IF EXISTS B")
    instance.query("CREATE USER B")
    instance.query("GRANT SELECT ON test.table TO B")
    assert instance.query("SHOW GRANTS FOR B") == TSV(
        ["GRANT SELECT ON test.`table` TO B"]
    )

    instance.query("GRANT CURRENT GRANTS ON *.* TO B WITH REPLACE OPTION", user="A")
    assert instance.query("SHOW GRANTS FOR B") == TSV(
        [
            "GRANT SELECT ON *.* TO B",
            "REVOKE SELECT ON test.* FROM B",
        ]
    )


def test_table_engine_grant_and_revoke():
    instance.query("DROP USER IF EXISTS A")
    instance.query("CREATE USER A")
    instance.query("GRANT CREATE TABLE ON test.table1 TO A")
    assert "Not enough privileges" in instance.query_and_get_error(
        "CREATE TABLE test.table1(a Integer) engine=TinyLog", user="A"
    )

    instance.query("GRANT TABLE ENGINE ON TinyLog TO A")

    instance.query("CREATE TABLE test.table1(a Integer) engine=TinyLog", user="A")

    assert instance.query("SHOW GRANTS FOR A") == TSV(
        [
            "GRANT TABLE ENGINE ON TinyLog TO A",
            "GRANT CREATE TABLE ON test.table1 TO A",
        ]
    )

    instance.query("REVOKE TABLE ENGINE ON TinyLog FROM A")

    assert "Not enough privileges" in instance.query_and_get_error(
        "CREATE TABLE test.table1(a Integer) engine=TinyLog", user="A"
    )

    instance.query("REVOKE CREATE TABLE ON test.table1 FROM A")
    instance.query("DROP TABLE test.table1")

    assert instance.query("SHOW GRANTS FOR A") == TSV([])


def test_table_engine_and_source_grant():
    instance.query("DROP USER IF EXISTS A")
    instance.query("CREATE USER A")
    instance.query("GRANT CREATE TABLE ON test.table1 TO A")

    instance.query("GRANT TABLE ENGINE ON PostgreSQL TO A")

    instance.query(
        """
        CREATE TABLE test.table1(a Integer)
        engine=PostgreSQL('localhost:5432', 'dummy', 'dummy', 'dummy', 'dummy');
        """,
        user="A",
    )

    instance.query("DROP TABLE test.table1")

    instance.query("REVOKE TABLE ENGINE ON PostgreSQL FROM A")

    assert "Not enough privileges" in instance.query_and_get_error(
        """
        CREATE TABLE test.table1(a Integer)
        engine=PostgreSQL('localhost:5432', 'dummy', 'dummy', 'dummy', 'dummy');
        """,
        user="A",
    )

    instance.query("GRANT SOURCES ON *.* TO A")

    instance.query(
        """
        CREATE TABLE test.table1(a Integer)
        engine=PostgreSQL('localhost:5432', 'dummy', 'dummy', 'dummy', 'dummy');
        """,
        user="A",
    )

    instance.query("DROP TABLE test.table1")
