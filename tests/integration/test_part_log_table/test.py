import time
import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance("node1")
node2 = cluster.add_instance("node2", main_configs=["configs/config_with_standard_part_log.xml"])
node3 = cluster.add_instance("node3", main_configs=["configs/config_with_non_standard_part_log.xml"])

@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

def test_config_without_part_log(start_cluster):
    assert "Table system.part_log doesn't exist" in node1.query_and_get_error("SELECT * FROM system.part_log")
    node1.query("CREATE TABLE test_table(word String, value UInt64) ENGINE=MergeTree() ORDER BY value")
    assert "Table system.part_log doesn't exist" in node1.query_and_get_error("SELECT * FROM system.part_log")
    node1.query("INSERT INTO test_table VALUES ('name', 1)")
    node1.query("SYSTEM FLUSH LOGS")
    assert "Table system.part_log doesn't exist" in node1.query_and_get_error("SELECT * FROM system.part_log")

def test_config_with_standard_part_log(start_cluster):
    assert "Table system.part_log doesn't exist" in node2.query_and_get_error("SELECT * FROM system.part_log")
    node2.query("CREATE TABLE test_table(word String, value UInt64) ENGINE=MergeTree() Order by value")
    assert "Table system.part_log doesn't exist" in node2.query_and_get_error("SELECT * FROM system.part_log")
    node2.query("INSERT INTO test_table VALUES ('name', 1)")
    node1.query("SYSTEM FLUSH LOGS")
    assert node2.query("SELECT * FROM system.part_log") != ""

def test_config_with_non_standard_part_log(start_cluster):
    assert "Table system.table_name doesn't exist" in node3.query_and_get_error("SELECT * FROM system.table_name")
    node3.query("CREATE TABLE test_table(word String, value UInt64) ENGINE=MergeTree() Order by value")
    assert "Table system.table_name doesn't exist" in node3.query_and_get_error("SELECT * FROM system.table_name")
    node3.query("INSERT INTO test_table VALUES ('name', 1)")
    node1.query("SYSTEM FLUSH LOGS")
    assert node3.query("SELECT * FROM system.table_name") != ""

