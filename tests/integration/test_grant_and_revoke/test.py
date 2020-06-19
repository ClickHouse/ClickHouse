import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV
import re

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('instance')


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
def reset_users_and_roles():
    try:
        yield
    finally:
        instance.query("DROP USER IF EXISTS A, B")
        instance.query("DROP ROLE IF EXISTS R1, R2")


def test_login():
    instance.query("CREATE USER A")
    instance.query("CREATE USER B")
    assert instance.query("SELECT 1", user='A') == "1\n"
    assert instance.query("SELECT 1", user='B') == "1\n"


def test_grant_and_revoke():
    instance.query("CREATE USER A")
    assert "Not enough privileges" in instance.query_and_get_error("SELECT * FROM test_table", user='A')
    
    instance.query('GRANT SELECT ON test_table TO A')
    assert instance.query("SELECT * FROM test_table", user='A') == "1\t5\n2\t10\n"

    instance.query('REVOKE SELECT ON test_table FROM A')
    assert "Not enough privileges" in instance.query_and_get_error("SELECT * FROM test_table", user='A')


def test_grant_option():
    instance.query("CREATE USER A")
    instance.query("CREATE USER B")

    instance.query('GRANT SELECT ON test_table TO A')
    assert instance.query("SELECT * FROM test_table", user='A') == "1\t5\n2\t10\n"
    assert "Not enough privileges" in instance.query_and_get_error("GRANT SELECT ON test_table TO B", user='A')
    
    instance.query('GRANT SELECT ON test_table TO A WITH GRANT OPTION')
    instance.query("GRANT SELECT ON test_table TO B", user='A')
    assert instance.query("SELECT * FROM test_table", user='B') == "1\t5\n2\t10\n"

    instance.query('REVOKE SELECT ON test_table FROM A, B')


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

    assert instance.query("SHOW USERS") == TSV([ "A", "B", "default" ])
    assert instance.query("SHOW ROLES") == TSV([ "R1", "R2" ])
    assert instance.query("SHOW GRANTS FOR A") == TSV([ "GRANT SELECT ON test.table TO A", "GRANT R1 TO A" ])
    assert instance.query("SHOW GRANTS FOR B") == TSV([ "GRANT CREATE ON *.* TO B WITH GRANT OPTION", "GRANT R2 TO B WITH ADMIN OPTION" ])
    assert instance.query("SHOW GRANTS FOR R1") == ""
    assert instance.query("SHOW GRANTS FOR R2") == TSV([ "GRANT SELECT ON test.table TO R2", "REVOKE SELECT(x) ON test.table FROM R2" ])
    
    assert instance.query("SHOW GRANTS", user='A') == TSV([ "GRANT SELECT ON test.table TO A", "GRANT R1 TO A" ])
    assert instance.query("SHOW GRANTS", user='B') == TSV([ "GRANT CREATE ON *.* TO B WITH GRANT OPTION", "GRANT R2 TO B WITH ADMIN OPTION" ])
    assert instance.query("SHOW CURRENT ROLES", user='A') == TSV([[ "R1", 0, 1 ]])
    assert instance.query("SHOW CURRENT ROLES", user='B') == TSV([[ "R2", 1, 1 ]])
    assert instance.query("SHOW ENABLED ROLES", user='A') == TSV([[ "R1", 0, 1, 1 ]])
    assert instance.query("SHOW ENABLED ROLES", user='B') == TSV([[ "R2", 1, 1, 1 ]])

    assert instance.query("SELECT name, storage, auth_type, auth_params, host_ip, host_names, host_names_regexp, host_names_like, default_roles_all, default_roles_list, default_roles_except from system.users WHERE name IN ('A', 'B') ORDER BY name") ==\
           TSV([[ "A", "disk", "no_password", "[]", "['::/0']", "[]", "[]", "[]", 1, "[]", "[]" ],
                [ "B", "disk", "no_password", "[]", "['::/0']", "[]", "[]", "[]", 1, "[]", "[]" ]])
    
    assert instance.query("SELECT name, storage from system.roles WHERE name IN ('R1', 'R2') ORDER BY name") ==\
           TSV([[ "R1", "disk" ],
                [ "R2", "disk" ]])

    assert instance.query("SELECT * from system.grants WHERE user_name IN ('A', 'B') OR role_name IN ('R1', 'R2') ORDER BY user_name, role_name, access_type, grant_option") ==\
           TSV([[ "A",  "\N", "SELECT", "test", "table", "\N", 0, 0 ],
                [ "B",  "\N", "CREATE", "\N",   "\N",    "\N", 0, 0 ],
                [ "B",  "\N", "CREATE", "\N",   "\N",    "\N", 0, 1 ],
                [ "\N", "R2", "SELECT", "test", "table", "\N", 0, 0 ],
                [ "\N", "R2", "SELECT", "test", "table", "x",  1, 0 ]])

    assert instance.query("SELECT * from system.role_grants WHERE user_name IN ('A', 'B') OR role_name IN ('R1', 'R2') ORDER BY user_name, role_name, granted_role_name") ==\
           TSV([[ "A", "\N", "R1", 1, 0 ],
                [ "B", "\N", "R2", 1, 1 ]])

    assert instance.query("SELECT * from system.current_roles ORDER BY role_name", user='A') == TSV([[ "R1", 0, 1 ]])
    assert instance.query("SELECT * from system.current_roles ORDER BY role_name", user='B') == TSV([[ "R2", 1, 1 ]])
    assert instance.query("SELECT * from system.enabled_roles ORDER BY role_name", user='A') == TSV([[ "R1", 0, 1, 1 ]])
    assert instance.query("SELECT * from system.enabled_roles ORDER BY role_name", user='B') == TSV([[ "R2", 1, 1, 1 ]])
