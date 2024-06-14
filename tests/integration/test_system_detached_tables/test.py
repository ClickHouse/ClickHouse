import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node_default", stay_alive=True)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_system_detached_tables():
    node.query("CREATE TABLE test_table (n Int64) ENGINE=MergeTree ORDER BY n;")
    node.query("CREATE TABLE test_table_perm (n Int64) ENGINE=MergeTree ORDER BY n;")

    result = node.query("SELECT * FROM system.detached_tables")
    assert result == ""

    node.query("DETACH TABLE test_table")
    node.query("DETACH TABLE test_table_perm PERMANENTLY")

    result = node.query("SELECT name FROM system.detached_tables")
    assert result == "test_table\ntest_table_perm\n"

    node.restart_clickhouse()

    result = node.query("SELECT name FROM system.detached_tables")
    assert result == "test_table_perm\n"

    node.restart_clickhouse()

    result = node.query("SELECT name FROM system.detached_tables")
    assert result == "test_table_perm\n"
