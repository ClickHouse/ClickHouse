"""
Integration tests for system.connections table.

The table is enabled via collect_connection_metrics = true in the server config
(see configs/config.d/connections.xml). It exposes all active TCP and HTTP
connections with their status (active / idle), query_id, user, protocol, etc.

Scenarios covered:
- TCP idle:     a persistent connection waiting for the next query shows status='idle'
- TCP active:   while a slow query is running, the connection shows status='active'
                with the correct query_id
- TCP exception: after a query raises an exception, the connection returns to
                status='idle'
- HTTP active:  every HTTP request is visible for its duration as status='active'
                with protocol='HTTP'
- Multiple TCP connections are all visible simultaneously
"""

import subprocess
import threading
import time

import pytest
import requests

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/config.d/connections.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def query(sql, **kwargs):
    """Run a query on the node and return stripped output."""
    return node.query(sql, **kwargs).strip()


def connections(extra_where=""):
    """Return system.connections rows as a list of dicts."""
    where = "WHERE protocol IN ('TCP', 'HTTP')"
    if extra_where:
        where += " AND " + extra_where
    result = node.query(
        f"SELECT connection_id, protocol, user, status, query_id "
        f"FROM system.connections {where} "
        f"ORDER BY connection_id"
    )
    rows = []
    for line in result.strip().splitlines():
        if not line:
            continue
        parts = line.split("\t")
        rows.append(
            {
                "connection_id": int(parts[0]),
                "protocol": parts[1],
                "user": parts[2],
                "status": parts[3],
                "query_id": parts[4],
            }
        )
    return rows


def open_persistent_tcp_client():
    """
    Open a clickhouse-client process that keeps a persistent TCP connection
    alive by reading queries from stdin. Returns the Popen object.
    stdin/stdout use line-buffered text mode so we can write and read
    individual queries.
    """
    return subprocess.Popen(
        ["docker", "exec", "-i", node.docker_id, "clickhouse", "client"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )


def run_query_in_client(proc, sql):
    """Write a query to a persistent client and read the response line."""
    proc.stdin.write(sql + "\n")
    proc.stdin.flush()
    return proc.stdout.readline().strip()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_table_exists(started_cluster):
    """system.connections must exist and expose the expected columns."""
    columns = query(
        "SELECT name FROM system.columns "
        "WHERE database = 'system' AND table = 'connections' "
        "ORDER BY position"
    )
    expected = [
        "connection_id",
        "protocol",
        "client_address",
        "client_port",
        "server_port",
        "user",
        "status",
        "query_id",
        "client_name",
        "client_version_major",
        "client_version_minor",
        "client_version_patch",
        "connected_time",
        "last_query_time",
    ]
    assert columns.splitlines() == expected


def test_server_setting_enabled(started_cluster):
    """The collect_connection_metrics ServerSetting must report true."""
    value = query(
        "SELECT value FROM system.server_settings "
        "WHERE name = 'collect_connection_metrics'"
    )
    assert value == "1"


def test_tcp_idle(started_cluster):
    """
    A persistent TCP connection that is not currently running a query
    must appear in system.connections with status='idle'.
    """
    proc = open_persistent_tcp_client()
    try:
        # Send a lightweight query to make sure the connection is authenticated
        # and registered, then wait for it to become idle.
        run_query_in_client(proc, "SELECT 1")
        time.sleep(0.5)

        rows = connections("status = 'idle' AND protocol = 'TCP'")
        assert len(rows) >= 1, "Expected at least one idle TCP connection"
        assert all(r["query_id"] == "" for r in rows), (
            "Idle connections must have empty query_id"
        )
        assert all(r["user"] == "default" for r in rows)
    finally:
        proc.stdin.close()
        proc.wait(timeout=5)


def test_tcp_active_shows_correct_query_id(started_cluster):
    """
    While a slow query is executing on a TCP connection, system.connections
    must show status='active' with the matching query_id.
    """
    unique_id = f"test_active_{int(time.time() * 1000)}"
    result_holder = []
    error_holder = []

    def run_slow_query():
        try:
            # SLEEP for 5 s; we observe the connection during this window.
            result_holder.append(
                node.query(
                    f"SELECT sleep(5)",
                    query_id=unique_id,
                )
            )
        except Exception as e:
            error_holder.append(str(e))

    t = threading.Thread(target=run_slow_query)
    t.start()
    try:
        # Give the query a moment to start.
        time.sleep(1)

        rows = connections(f"query_id = '{unique_id}'")
        assert len(rows) == 1, (
            f"Expected exactly one connection with query_id={unique_id!r}, "
            f"got {rows}"
        )
        assert rows[0]["status"] == "active"
        assert rows[0]["protocol"] == "TCP"
        assert rows[0]["user"] == "default"
    finally:
        # Kill the running query so the test does not block for 5 s.
        node.query(f"KILL QUERY WHERE query_id = '{unique_id}' SYNC")
        t.join(timeout=10)


def test_tcp_idle_after_query_completes(started_cluster):
    """
    After a query finishes on a persistent TCP connection, the connection
    must return to status='idle' and have an empty query_id.
    """
    proc = open_persistent_tcp_client()
    try:
        run_query_in_client(proc, "SELECT 42")
        time.sleep(0.5)

        rows = connections("status = 'idle' AND protocol = 'TCP'")
        assert len(rows) >= 1
        assert all(r["query_id"] == "" for r in rows)
    finally:
        proc.stdin.close()
        proc.wait(timeout=5)


def test_tcp_idle_after_exception(started_cluster):
    """
    After a query raises an exception on a TCP connection, the connection
    must return to status='idle' and have an empty query_id.
    """
    proc = open_persistent_tcp_client()
    try:
        # Trigger an exception (division by zero).
        run_query_in_client(proc, "SELECT 1 / 0")
        time.sleep(0.5)

        rows = connections("status = 'idle' AND protocol = 'TCP'")
        assert len(rows) >= 1, (
            "TCP connection must be idle after an exception"
        )
        assert all(r["query_id"] == "" for r in rows)
    finally:
        proc.stdin.close()
        proc.wait(timeout=5)


def test_http_active_during_request(started_cluster):
    """
    An HTTP request that is currently executing must appear in
    system.connections with protocol='HTTP' and status='active'.
    """
    unique_id = f"test_http_{int(time.time() * 1000)}"
    result_holder = []
    error_holder = []

    def run_http_slow_query():
        try:
            url = f"http://{node.ip_address}:8123/"
            resp = requests.get(
                url,
                params={
                    "query": "SELECT sleep(5)",
                    "query_id": unique_id,
                },
                timeout=15,
            )
            result_holder.append(resp.status_code)
        except Exception as e:
            error_holder.append(str(e))

    t = threading.Thread(target=run_http_slow_query)
    t.start()
    try:
        time.sleep(1)

        rows = connections(f"query_id = '{unique_id}'")
        assert len(rows) == 1, (
            f"Expected exactly one HTTP connection with query_id={unique_id!r}, "
            f"got {rows}"
        )
        assert rows[0]["protocol"] == "HTTP"
        assert rows[0]["status"] == "active"
        assert rows[0]["user"] == "default"
    finally:
        node.query(f"KILL QUERY WHERE query_id = '{unique_id}' SYNC")
        t.join(timeout=10)


def test_http_gone_after_request(started_cluster):
    """
    After an HTTP request finishes, it must no longer appear in
    system.connections (HTTP connections are ephemeral — they exist only
    for the duration of the request).
    """
    unique_id = f"test_http_done_{int(time.time() * 1000)}"

    url = f"http://{node.ip_address}:8123/"
    requests.get(
        url,
        params={"query": "SELECT 1", "query_id": unique_id},
        timeout=10,
    )

    # After the request is done, the connection entry must be gone.
    rows = connections(f"query_id = '{unique_id}'")
    assert rows == [], (
        "HTTP connection must be deregistered after the request completes"
    )


def test_multiple_tcp_connections(started_cluster):
    """
    Multiple simultaneous TCP connections must all be visible in
    system.connections.
    """
    n_clients = 3
    procs = [open_persistent_tcp_client() for _ in range(n_clients)]
    try:
        for proc in procs:
            run_query_in_client(proc, "SELECT 1")
        time.sleep(0.5)

        rows = connections("status = 'idle' AND protocol = 'TCP'")
        # At least n_clients idle TCP connections (there may be more from
        # the querying connection itself).
        assert len(rows) >= n_clients, (
            f"Expected at least {n_clients} idle TCP connections, got {len(rows)}"
        )
    finally:
        for proc in procs:
            proc.stdin.close()
            proc.wait(timeout=5)


def test_tcp_connection_removed_on_disconnect(started_cluster):
    """
    After a TCP connection is closed, it must disappear from system.connections.
    """
    proc = open_persistent_tcp_client()
    run_query_in_client(proc, "SELECT 1")
    time.sleep(0.5)

    before = connections("status = 'idle' AND protocol = 'TCP'")

    proc.stdin.close()
    proc.wait(timeout=5)
    time.sleep(0.5)

    after = connections("status = 'idle' AND protocol = 'TCP'")
    assert len(after) < len(before) or len(after) == 0, (
        "Closed TCP connection must be removed from system.connections"
    )
