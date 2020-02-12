import pytest
from helpers.cluster import ClickHouseCluster
import re

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('instance', config_dir="configs")


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        
        instance.query("CREATE TABLE test_table(x UInt32) ENGINE = MergeTree ORDER BY tuple()")
        instance.query("INSERT INTO test_table SELECT number FROM numbers(3)")
        instance.query("CREATE USER A PROFILE 'default'")
        instance.query("CREATE USER B PROFILE 'default'")

        yield cluster

    finally:
        cluster.shutdown()


def test_login():
    assert instance.query("SELECT 1", user='A') == "1\n"
    assert instance.query("SELECT 1", user='B') == "1\n"


def test_grant_and_revoke():
    assert "Not enough privileges" in instance.query_and_get_error("SELECT * FROM test_table", user='A')
    
    instance.query('GRANT SELECT ON test_table TO A')
    assert instance.query("SELECT * FROM test_table", user='A') == "0\n1\n2\n"

    instance.query('REVOKE SELECT ON test_table FROM A')
    assert "Not enough privileges" in instance.query_and_get_error("SELECT * FROM test_table", user='A')


def test_grant_option():
    instance.query('GRANT SELECT ON test_table TO A')
    assert instance.query("SELECT * FROM test_table", user='A') == "0\n1\n2\n"
    assert "Not enough privileges" in instance.query_and_get_error("GRANT SELECT ON test_table TO B", user='A')
    
    instance.query('GRANT SELECT ON test_table TO A WITH GRANT OPTION')
    instance.query("GRANT SELECT ON test_table TO B", user='A')
    assert instance.query("SELECT * FROM test_table", user='B') == "0\n1\n2\n"

    instance.query('REVOKE SELECT ON test_table FROM A, B')
