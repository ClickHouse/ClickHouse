import pytest
import subprocess
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/config.d/tcp_connection_limits.xml"])

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        # `cluster.start` only waits until the TCP port completes a handshake, which the
        # kernel does as soon as the socket is listening - the server can still be starting
        # up and not accepting yet. Wait until a query actually goes through, so that the
        # connection of a test is not queued behind the rest of the startup.
        node.query("SELECT 1")
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

def get_limit_closed_count(reason):
    """Count the connections that the server closed because a limit was reached.

    Counting every closed connection instead would be racy: the readiness probe of
    `cluster.start` connects to the port and closes it without sending any data, and the
    server accepts that connection only once it starts serving, which can happen after the
    test has already sampled the initial count. Connections closed for other reasons never
    report a limit, so counting only those keeps the assertion exact.
    """
    return int(node.count_in_log(f"Closing connection due to limits: {reason}").strip())

def test_query_count_limit(started_cluster):
    initial_count = get_limit_closed_count("queries=")

    queries = ["SELECT 1;", "SELECT 2;", "SELECT 3;", "SELECT 4;", "SELECT 5;"]
    stdout, stderr = execute_queries_persistent_connection(queries)

    assert "1" in stdout and "2" in stdout and "3" in stdout
    assert "4" not in stdout and "5" not in stdout
    assert "TCP_CONNECTION_LIMIT_REACHED" in stderr

    final_count = get_limit_closed_count("queries=")
    assert final_count == initial_count + 1, f"Expected exactly 1 connection closure, got {final_count - initial_count}"

def test_time_limit(started_cluster):
    initial_count = get_limit_closed_count("elapsed=")

    queries = ["SELECT sleep(3);", "SELECT 1;", "SELECT 2;"]
    stdout, stderr = execute_queries_persistent_connection(queries)

    assert "1" not in stdout and "2" not in stdout
    assert "TCP_CONNECTION_LIMIT_REACHED" in stderr

    final_count = get_limit_closed_count("elapsed=")
    assert final_count == initial_count + 1, f"Expected exactly 1 connection closure, got {final_count - initial_count}"
