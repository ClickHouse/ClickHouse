import pytest
import subprocess
import time
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/config.d/tcp_connection_limits.xml"])

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

@pytest.fixture(scope="module", autouse=True)
def stabilize_container(started_cluster):
    """Wait for container startup processes to complete before running tests"""
    time.sleep(1)

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

def get_connection_done_count():
    try:
        log_result = node.exec_in_container(
            ["grep", "-c", "Done processing connection", "/var/log/clickhouse-server/clickhouse-server.log"]
        )
        return int(log_result.strip())
    except Exception:
        return 0

def test_query_count_limit(started_cluster):
    initial_count = get_connection_done_count()

    queries = ["SELECT 1;", "SELECT 2;", "SELECT 3;", "SELECT 4;", "SELECT 5;"]
    stdout, stderr = execute_queries_persistent_connection(queries)

    assert "1" in stdout and "2" in stdout and "3" in stdout
    assert "4" not in stdout and "5" not in stdout
    assert "TCP_CONNECTION_LIMIT_REACHED" in stderr

    final_count = get_connection_done_count()
    assert final_count == initial_count + 1, f"Expected exactly 1 connection closure, got {final_count - initial_count}"

def test_time_limit(started_cluster):
    initial_count = get_connection_done_count()

    queries = ["SELECT sleep(3);", "SELECT 1;", "SELECT 2;"]
    stdout, stderr = execute_queries_persistent_connection(queries)

    assert "1" not in stdout and "2" not in stdout
    assert "TCP_CONNECTION_LIMIT_REACHED" in stderr

    final_count = get_connection_done_count()
    assert final_count == initial_count + 1, f"Expected exactly 1 connection closure, got {final_count - initial_count}"
