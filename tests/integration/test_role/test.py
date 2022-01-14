import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('instance')


session_id_counter = 0
def new_session_id():
    global session_id_counter
    session_id_counter += 1
    return 'session #' + str(session_id_counter)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()

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
        instance.query("DROP ROLE IF EXISTS R1, R2")


def test_create_role():
    instance.query("CREATE USER A")
    instance.query('CREATE ROLE R1')

    assert "Not enough privileges" in instance.query_and_get_error("SELECT * FROM test_table", user='A')

    instance.query('GRANT SELECT ON test_table TO R1')
    assert "Not enough privileges" in instance.query_and_get_error("SELECT * FROM test_table", user='A')

    instance.query('GRANT R1 TO A')
    assert instance.query("SELECT * FROM test_table", user='A') == "1\t5\n2\t10\n"

    instance.query('REVOKE R1 FROM A')
    assert "Not enough privileges" in instance.query_and_get_error("SELECT * FROM test_table", user='A')


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


def test_admin_option():
    instance.query("CREATE USER A")
    instance.query("CREATE USER B")
    instance.query('CREATE ROLE R1')

    instance.query('GRANT SELECT ON test_table TO R1')
    assert "Not enough privileges" in instance.query_and_get_error("SELECT * FROM test_table", user='B')

    instance.query('GRANT R1 TO A')
    assert "Not enough privileges" in instance.query_and_get_error("GRANT R1 TO B", user='A')
    assert "Not enough privileges" in instance.query_and_get_error("SELECT * FROM test_table", user='B')

    instance.query('GRANT R1 TO A WITH ADMIN OPTION')
    instance.query("GRANT R1 TO B", user='A')
    assert instance.query("SELECT * FROM test_table", user='B') == "1\t5\n2\t10\n"


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


def test_set_role():
    instance.query("CREATE USER A")
    instance.query("CREATE ROLE R1, R2")
    instance.query("GRANT R1, R2 TO A")

    session_id = new_session_id()
    assert instance.http_query('SHOW CURRENT ROLES', user='A', params={'session_id':session_id}) == TSV([["R1", 0, 1], ["R2", 0, 1]])

    instance.http_query('SET ROLE R1', user='A', params={'session_id':session_id})
    assert instance.http_query('SHOW CURRENT ROLES', user='A', params={'session_id':session_id}) == TSV([["R1", 0, 1]])

    instance.http_query('SET ROLE R2', user='A', params={'session_id':session_id})
    assert instance.http_query('SHOW CURRENT ROLES', user='A', params={'session_id':session_id}) == TSV([["R2", 0, 1]])

    instance.http_query('SET ROLE NONE', user='A', params={'session_id':session_id})
    assert instance.http_query('SHOW CURRENT ROLES', user='A', params={'session_id':session_id}) == TSV([])

    instance.http_query('SET ROLE DEFAULT', user='A', params={'session_id':session_id})
    assert instance.http_query('SHOW CURRENT ROLES', user='A', params={'session_id':session_id}) == TSV([["R1", 0, 1], ["R2", 0, 1]])


def test_introspection():
    instance.query("CREATE USER A")
    instance.query("CREATE USER B")
    instance.query('CREATE ROLE R1')
    instance.query('CREATE ROLE R2')
    instance.query('GRANT R1 TO A')
    instance.query('GRANT R2 TO B WITH ADMIN OPTION')
    instance.query('GRANT SELECT ON test.table TO A, R2')
    instance.query('GRANT CREATE ON *.* TO B WITH GRANT OPTION')
    instance.query('REVOKE SELECT(x) ON test.table FROM R2')

    assert instance.query("SHOW ROLES") == TSV(["R1", "R2"])
    assert instance.query("SHOW CREATE ROLE R1") == TSV(["CREATE ROLE R1"])
    assert instance.query("SHOW CREATE ROLE R2") == TSV(["CREATE ROLE R2"])
    assert instance.query("SHOW CREATE ROLES R1, R2") == TSV(["CREATE ROLE R1", "CREATE ROLE R2"])
    assert instance.query("SHOW CREATE ROLES") == TSV(["CREATE ROLE R1", "CREATE ROLE R2"])

    assert instance.query("SHOW GRANTS FOR A") == TSV(["GRANT SELECT ON test.table TO A", "GRANT R1 TO A"])
    assert instance.query("SHOW GRANTS FOR B") == TSV(
        ["GRANT CREATE ON *.* TO B WITH GRANT OPTION", "GRANT R2 TO B WITH ADMIN OPTION"])
    assert instance.query("SHOW GRANTS FOR R1") == ""
    assert instance.query("SHOW GRANTS FOR R2") == TSV(
        ["GRANT SELECT ON test.table TO R2", "REVOKE SELECT(x) ON test.table FROM R2"])

    assert instance.query("SHOW GRANTS", user='A') == TSV(["GRANT SELECT ON test.table TO A", "GRANT R1 TO A"])
    assert instance.query("SHOW GRANTS", user='B') == TSV(
        ["GRANT CREATE ON *.* TO B WITH GRANT OPTION", "GRANT R2 TO B WITH ADMIN OPTION"])
    assert instance.query("SHOW CURRENT ROLES", user='A') == TSV([["R1", 0, 1]])
    assert instance.query("SHOW CURRENT ROLES", user='B') == TSV([["R2", 1, 1]])
    assert instance.query("SHOW ENABLED ROLES", user='A') == TSV([["R1", 0, 1, 1]])
    assert instance.query("SHOW ENABLED ROLES", user='B') == TSV([["R2", 1, 1, 1]])

    expected_access1 = "CREATE ROLE R1\n" \
                       "CREATE ROLE R2\n"
    expected_access2 = "GRANT R1 TO A\n"
    expected_access3 = "GRANT R2 TO B WITH ADMIN OPTION"
    assert expected_access1 in instance.query("SHOW ACCESS")
    assert expected_access2 in instance.query("SHOW ACCESS")
    assert expected_access3 in instance.query("SHOW ACCESS")

    assert instance.query("SELECT name, storage from system.roles WHERE name IN ('R1', 'R2') ORDER BY name") == \
           TSV([["R1", "local directory"],
                ["R2", "local directory"]])

    assert instance.query(
        "SELECT * from system.grants WHERE user_name IN ('A', 'B') OR role_name IN ('R1', 'R2') ORDER BY user_name, role_name, access_type, grant_option") == \
           TSV([["A", "\\N", "SELECT", "test", "table", "\\N", 0, 0],
                ["B", "\\N", "CREATE", "\\N", "\\N", "\\N", 0, 1],
                ["\\N", "R2", "SELECT", "test", "table", "\\N", 0, 0],
                ["\\N", "R2", "SELECT", "test", "table", "x", 1, 0]])

    assert instance.query(
        "SELECT * from system.role_grants WHERE user_name IN ('A', 'B') OR role_name IN ('R1', 'R2') ORDER BY user_name, role_name, granted_role_name") == \
           TSV([["A", "\\N", "R1", 1, 0],
                ["B", "\\N", "R2", 1, 1]])

    assert instance.query("SELECT * from system.current_roles ORDER BY role_name", user='A') == TSV([["R1", 0, 1]])
    assert instance.query("SELECT * from system.current_roles ORDER BY role_name", user='B') == TSV([["R2", 1, 1]])
    assert instance.query("SELECT * from system.enabled_roles ORDER BY role_name", user='A') == TSV([["R1", 0, 1, 1]])
    assert instance.query("SELECT * from system.enabled_roles ORDER BY role_name", user='B') == TSV([["R2", 1, 1, 1]])
