import pytest
from helpers.cluster import ClickHouseCluster
import re

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('instance', config_dir="configs")


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
