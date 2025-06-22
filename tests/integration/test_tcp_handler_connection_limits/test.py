import pytest
import subprocess
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

def execute_queries_persistent_connection(queries):
    """Execute multiple queries through a single persistent clickhouse-client connection"""
    proc = subprocess.Popen(
        ["docker", "exec", "-i", node.docker_id, "clickhouse", "client"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    query_string = "\n".join(queries) + "\n"
    stdout, stderr = proc.communicate(query_string, timeout=15)

    return stdout, stderr

def test_query_count_limit(started_cluster):
    queries = ["SELECT 1;", "SELECT 2;", "SELECT 3;", "SELECT 4;"]
    stdout, stderr = execute_queries_persistent_connection(queries)

    assert '1' in stdout and '2' in stdout and '3' in stdout
    assert '4' not in stdout
    assert 'TCP_CONNECTION_LIMIT_EXCEEDED' in stderr

def test_time_limit(started_cluster):
    queries = ["SELECT sleep(3);", "SELECT 1;"]
    stdout, stderr = execute_queries_persistent_connection(queries)

    assert '1' not in stdout
    assert 'TCP_CONNECTION_LIMIT_EXCEEDED' in stderr
