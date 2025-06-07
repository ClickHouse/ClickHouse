import pytest
import time
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance('node', main_configs=['configs/config.d/tcp_connection_limits.xml'])

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

def execute_queries_in_single_connection(queries):
    """Helper to execute multiple queries in a single connection"""
    script = ";\n".join(queries) + ";"
    node.exec_in_container(["bash", "-c", f"echo '{script}' > /tmp/test_script.sql"])
    return node.exec_in_container(["bash", "-c", "cat /tmp/test_script.sql | /clickhouse client"])

def test_query_count_limit(started_cluster):
    with pytest.raises(Exception):
        execute_queries_in_single_connection([
            "SELECT 1", "SELECT 2", "SELECT 3", "SELECT 4"
        ])

def test_time_limit(started_cluster):
    with pytest.raises(Exception):
        execute_queries_in_single_connection([
            "SELECT 1", "SELECT sleep(3)", "SELECT 2"
        ])
