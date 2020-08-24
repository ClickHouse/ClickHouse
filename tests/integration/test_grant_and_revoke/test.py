import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV
import re

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('instance')


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        
        instance.query("CREATE DATABASE test")
        instance.query("CREATE TABLE test.table(x UInt32, y UInt32) ENGINE = MergeTree ORDER BY tuple()")
        instance.query("INSERT INTO test.table VALUES (1,5), (2,10)")
        
        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def cleanup_after_test():
    try:
        yield
    finally:
        instance.query("DROP USER IF EXISTS A, B")
        instance.query("DROP TABLE IF EXISTS test.view_1")


def test_smoke():
    instance.query("CREATE USER A")
    assert "Not enough privileges" in instance.query_and_get_error("SELECT * FROM test.table", user='A')
    
    instance.query('GRANT SELECT ON test.table TO A')
    assert instance.query("SELECT * FROM test.table", user='A') == "1\t5\n2\t10\n"

    instance.query('REVOKE SELECT ON test.table FROM A')
    assert "Not enough privileges" in instance.query_and_get_error("SELECT * FROM test.table", user='A')


def test_grant_option():
    instance.query("CREATE USER A")
    instance.query("CREATE USER B")

    instance.query('GRANT SELECT ON test.table TO A')
    assert instance.query("SELECT * FROM test.table", user='A') == "1\t5\n2\t10\n"
    assert "Not enough privileges" in instance.query_and_get_error("GRANT SELECT ON test.table TO B", user='A')
    
    instance.query('GRANT SELECT ON test.table TO A WITH GRANT OPTION')
    instance.query("GRANT SELECT ON test.table TO B", user='A')
    assert instance.query("SELECT * FROM test.table", user='B') == "1\t5\n2\t10\n"

    instance.query('REVOKE SELECT ON test.table FROM A, B')


def test_revoke_requires_grant_option():
    instance.query("CREATE USER A")
    instance.query("CREATE USER B")
    
    instance.query("GRANT SELECT ON test.table TO B")
    assert instance.query("SHOW GRANTS FOR B") == "GRANT SELECT ON test.table TO B\n"

    expected_error = "Not enough privileges"
    assert expected_error in instance.query_and_get_error("REVOKE SELECT ON test.table FROM B", user='A')
    assert instance.query("SHOW GRANTS FOR B") == "GRANT SELECT ON test.table TO B\n"

    instance.query("GRANT SELECT ON test.table TO A")
    expected_error = "privileges have been granted, but without grant option"
    assert expected_error in instance.query_and_get_error("REVOKE SELECT ON test.table FROM B", user='A')
    assert instance.query("SHOW GRANTS FOR B") == "GRANT SELECT ON test.table TO B\n"

    instance.query("GRANT SELECT ON test.table TO A WITH GRANT OPTION")
    assert instance.query("SHOW GRANTS FOR B") == "GRANT SELECT ON test.table TO B\n"
    instance.query("REVOKE SELECT ON test.table FROM B", user='A')
    assert instance.query("SHOW GRANTS FOR B") == ""

    instance.query("GRANT SELECT ON test.table TO B")
    assert instance.query("SHOW GRANTS FOR B") == "GRANT SELECT ON test.table TO B\n"
    instance.query("REVOKE SELECT ON test.* FROM B", user='A')
    assert instance.query("SHOW GRANTS FOR B") == ""

    instance.query("GRANT SELECT ON test.table TO B")
    assert instance.query("SHOW GRANTS FOR B") == "GRANT SELECT ON test.table TO B\n"
    instance.query("REVOKE ALL ON test.* FROM B", user='A')
    assert instance.query("SHOW GRANTS FOR B") == ""

    instance.query("GRANT SELECT ON test.table TO B")
    assert instance.query("SHOW GRANTS FOR B") == "GRANT SELECT ON test.table TO B\n"
    instance.query("REVOKE ALL ON *.* FROM B", user='A')
    assert instance.query("SHOW GRANTS FOR B") == ""

    instance.query("REVOKE GRANT OPTION FOR ALL ON *.* FROM A")
    instance.query("GRANT SELECT ON test.table TO B")
    assert instance.query("SHOW GRANTS FOR B") == "GRANT SELECT ON test.table TO B\n"
    expected_error = "privileges have been granted, but without grant option"
    assert expected_error in instance.query_and_get_error("REVOKE SELECT ON test.table FROM B", user='A')
    assert instance.query("SHOW GRANTS FOR B") == "GRANT SELECT ON test.table TO B\n"

    instance.query("GRANT SELECT ON test.* TO A WITH GRANT OPTION")
    instance.query("GRANT SELECT ON test.table TO B")
    assert instance.query("SHOW GRANTS FOR B") == "GRANT SELECT ON test.table TO B\n"
    instance.query("REVOKE SELECT ON test.table FROM B", user='A')
    assert instance.query("SHOW GRANTS FOR B") == ""


def test_implicit_show_grants():
    instance.query("CREATE USER A")
    assert instance.query("select count() FROM system.databases WHERE name='test'", user="A") == "0\n"
    assert instance.query("select count() FROM system.tables WHERE database='test' AND name='table'", user="A") == "0\n"
    assert instance.query("select count() FROM system.columns WHERE database='test' AND table='table'", user="A") == "0\n"

    instance.query("GRANT SELECT(x) ON test.table TO A")
    assert instance.query("SHOW GRANTS FOR A") == "GRANT SELECT(x) ON test.table TO A\n"
    assert instance.query("select count() FROM system.databases WHERE name='test'", user="A") == "1\n"
    assert instance.query("select count() FROM system.tables WHERE database='test' AND name='table'", user="A") == "1\n"
    assert instance.query("select count() FROM system.columns WHERE database='test' AND table='table'", user="A") == "1\n"

    instance.query("GRANT SELECT ON test.table TO A")
    assert instance.query("SHOW GRANTS FOR A") == "GRANT SELECT ON test.table TO A\n"
    assert instance.query("select count() FROM system.databases WHERE name='test'", user="A") == "1\n"
    assert instance.query("select count() FROM system.tables WHERE database='test' AND name='table'", user="A") == "1\n"
    assert instance.query("select count() FROM system.columns WHERE database='test' AND table='table'", user="A") == "2\n"

    instance.query("GRANT SELECT ON test.* TO A")
    assert instance.query("SHOW GRANTS FOR A") == "GRANT SELECT ON test.* TO A\n"
    assert instance.query("select count() FROM system.databases WHERE name='test'", user="A") == "1\n"
    assert instance.query("select count() FROM system.tables WHERE database='test' AND name='table'", user="A") == "1\n"
    assert instance.query("select count() FROM system.columns WHERE database='test' AND table='table'", user="A") == "2\n"

    instance.query("GRANT SELECT ON *.* TO A")
    assert instance.query("SHOW GRANTS FOR A") == "GRANT SELECT ON *.* TO A\n"
    assert instance.query("select count() FROM system.databases WHERE name='test'", user="A") == "1\n"
    assert instance.query("select count() FROM system.tables WHERE database='test' AND name='table'", user="A") == "1\n"
    assert instance.query("select count() FROM system.columns WHERE database='test' AND table='table'", user="A") == "2\n"

    instance.query("REVOKE ALL ON *.* FROM A")
    assert instance.query("select count() FROM system.databases WHERE name='test'", user="A") == "0\n"
    assert instance.query("select count() FROM system.tables WHERE database='test' AND name='table'", user="A") == "0\n"
    assert instance.query("select count() FROM system.columns WHERE database='test' AND table='table'", user="A") == "0\n"


def test_implicit_create_view_grant():
    instance.query("CREATE USER A")
    expected_error = "Not enough privileges"
    assert expected_error in instance.query_and_get_error("CREATE VIEW test.view_1 AS SELECT 1", user="A")

    instance.query("GRANT CREATE TABLE ON test.* TO A")
    instance.query("CREATE VIEW test.view_1 AS SELECT 1", user="A")
    assert instance.query("SELECT * FROM test.view_1") == "1\n"

    instance.query("REVOKE CREATE TABLE ON test.* FROM A")
    instance.query("DROP TABLE test.view_1")
    assert expected_error in instance.query_and_get_error("CREATE VIEW test.view_1 AS SELECT 1", user="A")


def test_implicit_create_temporary_table_grant():
    instance.query("CREATE USER A")
    expected_error = "Not enough privileges"
    assert expected_error in instance.query_and_get_error("CREATE TEMPORARY TABLE tmp(name String)", user="A")

    instance.query("GRANT CREATE TABLE ON test.* TO A")
    instance.query("CREATE TEMPORARY TABLE tmp(name String)", user="A")

    instance.query("REVOKE CREATE TABLE ON *.* FROM A")
    assert expected_error in instance.query_and_get_error("CREATE TEMPORARY TABLE tmp(name String)", user="A")


def test_introspection():
    instance.query("CREATE USER A")
    instance.query("CREATE USER B")
    instance.query('GRANT SELECT ON test.table TO A')
    instance.query('GRANT CREATE ON *.* TO B WITH GRANT OPTION')

    assert instance.query("SHOW USERS") == TSV([ "A", "B", "default" ])
    assert instance.query("SHOW CREATE USERS A") == TSV([ "CREATE USER A" ])
    assert instance.query("SHOW CREATE USERS B") == TSV([ "CREATE USER B" ])
    assert instance.query("SHOW CREATE USERS A,B") == TSV([ "CREATE USER A", "CREATE USER B" ])
    assert instance.query("SHOW CREATE USERS") == TSV([ "CREATE USER A", "CREATE USER B", "CREATE USER default IDENTIFIED WITH plaintext_password SETTINGS PROFILE default" ])

    assert instance.query("SHOW GRANTS FOR A") == TSV([ "GRANT SELECT ON test.table TO A" ])
    assert instance.query("SHOW GRANTS FOR B") == TSV([ "GRANT CREATE ON *.* TO B WITH GRANT OPTION" ])
    assert instance.query("SHOW GRANTS FOR A,B") == TSV([ "GRANT SELECT ON test.table TO A", "GRANT CREATE ON *.* TO B WITH GRANT OPTION" ])
    assert instance.query("SHOW GRANTS FOR B,A") == TSV([ "GRANT SELECT ON test.table TO A", "GRANT CREATE ON *.* TO B WITH GRANT OPTION" ])
    assert instance.query("SHOW GRANTS FOR ALL") == TSV([ "GRANT SELECT ON test.table TO A", "GRANT CREATE ON *.* TO B WITH GRANT OPTION", "GRANT ALL ON *.* TO default WITH GRANT OPTION" ])

    assert instance.query("SHOW GRANTS", user='A') == TSV([ "GRANT SELECT ON test.table TO A" ])
    assert instance.query("SHOW GRANTS", user='B') == TSV([ "GRANT CREATE ON *.* TO B WITH GRANT OPTION" ])

    expected_access1 = "CREATE USER A\n"\
                       "CREATE USER B\n"\
                       "CREATE USER default IDENTIFIED WITH plaintext_password SETTINGS PROFILE default"
    expected_access2 = "GRANT SELECT ON test.table TO A\n"\
                       "GRANT CREATE ON *.* TO B WITH GRANT OPTION\n"\
                       "GRANT ALL ON *.* TO default WITH GRANT OPTION\n"
    assert expected_access1 in instance.query("SHOW ACCESS")
    assert expected_access2 in instance.query("SHOW ACCESS")

    assert instance.query("SELECT name, storage, auth_type, auth_params, host_ip, host_names, host_names_regexp, host_names_like, default_roles_all, default_roles_list, default_roles_except from system.users WHERE name IN ('A', 'B') ORDER BY name") ==\
           TSV([[ "A", "disk", "no_password", "{}", "['::/0']", "[]", "[]", "[]", 1, "[]", "[]" ],
                [ "B", "disk", "no_password", "{}", "['::/0']", "[]", "[]", "[]", 1, "[]", "[]" ]])
    
    assert instance.query("SELECT * from system.grants WHERE user_name IN ('A', 'B') ORDER BY user_name, access_type, grant_option") ==\
           TSV([[ "A",  "\N", "SELECT", "test", "table", "\N", 0, 0 ],
                [ "B",  "\N", "CREATE", "\N",   "\N",    "\N", 0, 1 ]])


def test_current_database():
    instance.query("CREATE USER A")
    instance.query("GRANT SELECT ON table TO A", database="test")
    
    assert instance.query("SHOW GRANTS FOR A") == TSV([ "GRANT SELECT ON test.table TO A" ])
    assert instance.query("SHOW GRANTS FOR A", database="test") == TSV([ "GRANT SELECT ON test.table TO A" ])
    
    assert instance.query("SELECT * FROM test.table", user='A') == "1\t5\n2\t10\n"
    assert instance.query("SELECT * FROM table", user='A', database='test') == "1\t5\n2\t10\n"

    instance.query("CREATE TABLE default.table(x UInt32, y UInt32) ENGINE = MergeTree ORDER BY tuple()")
    assert "Not enough privileges" in instance.query_and_get_error("SELECT * FROM table", user='A')
