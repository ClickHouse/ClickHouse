"""
Tests for the admission queue in ProcessList.

When max_concurrent_queries is reached, incoming queries wait in a FIFO
admission queue (per-waiter CV with notify_one — no thundering herd). These tests verify:

1. All queued queries are eventually admitted
2. replace_running_query works correctly after passing through the admission queue
3. Queue slot is not leaked when a queued query times out
4. Client disconnect while waiting in queue is detected, and queue length metric is accurate
5. QueryAdmissionQueueWaitMicroseconds is recorded per-query and globally
"""

import re
import socket
import time
import urllib.parse
import uuid
from multiprocessing.dummy import Pool

import pytest
import requests

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/server.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_prometheus_metric(node, metric_name, timeout=5):
    """Read a CurrentMetric value from the Prometheus /metrics endpoint.

    This bypasses the query pipeline entirely, so it works even when all
    query execution slots are occupied.
    """
    resp = requests.get(
        f"http://{node.ip_address}:9363/metrics",
        timeout=timeout,
    )
    resp.raise_for_status()
    # Prometheus format: ClickHouseMetrics_<name> <value>
    pattern = rf"^ClickHouseMetrics_{metric_name}\s+(\d+)"
    for line in resp.text.splitlines():
        m = re.match(pattern, line)
        if m:
            return int(m.group(1))
    raise ValueError(f"Metric ClickHouseMetrics_{metric_name} not found in Prometheus output")


def wait_for_query_start(node, query_id, timeout=30):
    """Wait until a query appears in system.processes."""
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        result = node.query(
            f"SELECT count() FROM system.processes WHERE query_id = '{query_id}'"
        ).strip()
        if result == "1":
            return
        time.sleep(0.1)
    raise RuntimeError(f"Query {query_id} did not appear in system.processes within {timeout}s")


def wait_for_query_finish(node, query_id, timeout=60):
    """Wait until a query disappears from system.processes."""
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        result = node.query(
            f"SELECT count() FROM system.processes WHERE query_id = '{query_id}'"
        ).strip()
        if result == "0":
            return
        time.sleep(0.2)
    raise RuntimeError(f"Query {query_id} still running after {timeout}s")


def test_all_queued_queries_admitted(started_cluster):
    """
    Verify that all queued queries are eventually admitted.

    Strategy:
    1. Saturate both slots with blocker queries
    2. Submit 4 queries — they enter the admission queue
    3. Kill blockers to release slots
    4. All 4 queries should complete (appear in query_log with QueryFinish)
    """
    prefix = uuid.uuid4().hex[:8]

    blocker_ids = [f"blocker_{prefix}_{i}" for i in range(2)]
    waiter_ids = [f"waiter_{prefix}_{i}" for i in range(4)]

    pool = Pool(10)

    def run_blocker(qid):
        node.query(
            "SELECT sleep(30) FORMAT Null",
            settings={
                "function_sleep_max_microseconds_per_block": 0,
                "queue_max_wait_ms": 60000,
            },
            query_id=qid,
        )

    for qid in blocker_ids:
        pool.apply_async(run_blocker, (qid,))

    for qid in blocker_ids:
        wait_for_query_start(node, qid)

    # Submit 4 queries — they enter the admission queue.
    def run_waiter(qid):
        node.query(
            "SELECT 1 FORMAT Null",
            settings={"queue_max_wait_ms": 60000},
            query_id=qid,
        )

    for qid in waiter_ids:
        pool.apply_async(run_waiter, (qid,))

    # Give all waiters time to enter the queue
    time.sleep(0.5)

    # Kill blockers to release slots — waiters drain
    for qid in blocker_ids:
        node.query(f"KILL QUERY WHERE query_id = '{qid}' SYNC")

    pool.close()
    pool.join()

    # Flush query_log and verify all queries completed
    node.query("SYSTEM FLUSH LOGS")

    id_list = ", ".join(f"'{qid}'" for qid in waiter_ids)
    result = node.query(
        f"""
        SELECT count()
        FROM system.query_log
        WHERE query_id IN ({id_list})
          AND type = 'QueryFinish'
        """
    ).strip()

    assert int(result) == len(waiter_ids), (
        f"Expected all {len(waiter_ids)} queries to finish, got {result}"
    )


def test_replace_running_query_with_admission_queue(started_cluster):
    """
    Test that replace_running_query works correctly when the replacement query
    has to pass through the admission queue first.

    1. Saturate both slots: one "blocker" + one "victim" (with a known query_id)
    2. Submit a replacement query with the same query_id as the victim and
       replace_running_query=1. This query enters the admission queue.
    3. Kill the blocker to free a slot.
    4. The replacement should get admitted, then cancel the victim, then run.
    """
    prefix = uuid.uuid4().hex[:8]
    blocker_id = f"replace_blocker_{prefix}"
    victim_id = f"replace_victim_{prefix}"

    pool = Pool(4)

    # Start blocker (long sleep, will be killed)
    def run_blocker():
        node.query(
            "SELECT sleep(30) FORMAT Null",
            settings={
                "function_sleep_max_microseconds_per_block": 0,
                "queue_max_wait_ms": 60000,
            },
            query_id=blocker_id,
        )

    # Start victim (long sleep, will be replaced)
    def run_victim():
        try:
            node.query(
                "SELECT sleep(30) FORMAT Null",
                settings={
                    "function_sleep_max_microseconds_per_block": 0,
                    "queue_max_wait_ms": 60000,
                },
                query_id=victim_id,
            )
        except Exception:
            pass  # Expected: victim gets killed

    pool.apply_async(run_blocker)
    pool.apply_async(run_victim)

    wait_for_query_start(node, blocker_id)
    wait_for_query_start(node, victim_id)

    # Both slots saturated. Submit replacement with same query_id as victim.
    # It will enter the admission queue.
    replacement_result = [None]

    def run_replacement():
        try:
            result = node.query(
                "SELECT 'replaced'",
                settings={
                    "replace_running_query": 1,
                    "replace_running_query_max_wait_ms": 30000,
                    "queue_max_wait_ms": 30000,
                },
                query_id=victim_id,
            )
            replacement_result[0] = result.strip()
        except Exception as e:
            replacement_result[0] = f"ERROR: {e}"

    pool.apply_async(run_replacement)

    # Give the replacement time to enter the queue
    time.sleep(0.5)

    # Kill the blocker to free a slot — this should admit the replacement
    node.query(f"KILL QUERY WHERE query_id = '{blocker_id}' SYNC")

    pool.close()
    pool.join()

    assert replacement_result[0] == "replaced", (
        f"Replacement query failed: {replacement_result[0]}"
    )


def test_no_slot_leak_on_timeout(started_cluster):
    """
    Verify that when a queued query times out, its slot is not leaked:
    subsequent queries should still be able to run.

    1. Saturate both slots
    2. Submit a query with short timeout — it times out
    3. Kill blockers
    4. Submit a new query — it should succeed immediately (no leaked slot)
    """
    prefix = uuid.uuid4().hex[:8]
    blocker_ids = [f"leak_blocker_{prefix}_{i}" for i in range(2)]

    pool = Pool(4)

    def run_blocker(qid):
        node.query(
            "SELECT sleep(30) FORMAT Null",
            settings={
                "function_sleep_max_microseconds_per_block": 0,
                "queue_max_wait_ms": 60000,
            },
            query_id=qid,
        )

    for qid in blocker_ids:
        pool.apply_async(run_blocker, (qid,))

    for qid in blocker_ids:
        wait_for_query_start(node, qid)

    # This query should timeout in the queue
    error = node.query_and_get_error(
        "SELECT 1",
        settings={"queue_max_wait_ms": 200},
    )
    assert "TOO_MANY_SIMULTANEOUS_QUERIES" in error

    # Kill blockers to free slots
    for qid in blocker_ids:
        node.query(f"KILL QUERY WHERE query_id = '{qid}' SYNC")

    pool.close()
    pool.join()

    # Now both slots should be free — this must succeed
    result = node.query("SELECT 'no_leak'").strip()
    assert result == "no_leak", f"Expected 'no_leak', got '{result}'"


def test_client_disconnect_while_waiting_in_queue(started_cluster):
    """
    Verify that the server detects a client disconnect while the query is
    waiting in the admission queue and removes the waiter.

    Strategy:
    1. Saturate both slots with blocker queries
    2. Open a raw TCP socket to the HTTP port, send a query that will enter
       the admission queue, then immediately close the socket (RST)
    3. The server's periodic alive check (queue_alive_check_interval_ms=500)
       should detect the broken connection and cancel the waiter
    4. Verify the queue metric goes back to 0
    """
    prefix = uuid.uuid4().hex[:8]
    blocker_ids = [f"disconnect_blocker_{prefix}_{i}" for i in range(2)]

    pool = Pool(4)

    def run_blocker(qid):
        node.query(
            "SELECT sleep(30) FORMAT Null",
            settings={
                "function_sleep_max_microseconds_per_block": 0,
                "queue_max_wait_ms": 60000,
            },
            query_id=qid,
        )

    for qid in blocker_ids:
        pool.apply_async(run_blocker, (qid,))

    for qid in blocker_ids:
        wait_for_query_start(node, qid)

    # Open a raw TCP connection to the HTTP port and send a query that will
    # queue up, then close the connection abruptly.
    params = urllib.parse.urlencode({
        "query": "SELECT 1",
        "queue_max_wait_ms": "30000",
    })
    http_request = (
        f"GET /?{params} HTTP/1.1\r\n"
        f"Host: {node.ip_address}\r\n"
        f"Connection: close\r\n"
        f"\r\n"
    )

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)
    sock.connect((node.ip_address, 8123))
    sock.sendall(http_request.encode())

    # Give the server time to receive the request and enter the queue
    time.sleep(0.5)

    # Verify the query is actually in the queue (metric > 0).
    # Use Prometheus endpoint to avoid consuming a query slot.
    queue_len = get_prometheus_metric(node, "QueryAdmissionQueueLength")
    assert queue_len >= 1, f"Expected queue length >= 1, got {queue_len}"

    # Abruptly close the connection — send RST
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, b'\x01\x00\x00\x00\x00\x00\x00\x00')
    sock.close()

    # Wait for the alive check to detect the disconnect (interval=500ms, give 1.5s)
    time.sleep(1.5)

    # Queue should be empty now
    queue_len = get_prometheus_metric(node, "QueryAdmissionQueueLength")
    assert queue_len == 0, f"Expected queue length 0 after disconnect, got {queue_len}"

    # Clean up: kill blockers
    for qid in blocker_ids:
        node.query(f"KILL QUERY WHERE query_id = '{qid}' SYNC")

    pool.close()
    pool.join()


def test_queue_wait_time_profile_event(started_cluster):
    """
    Verify that QueryAdmissionQueueWaitMicroseconds is recorded both globally
    (system.events) and per-query (system.query_log ProfileEvents map).

    Strategy:
    1. Saturate both slots with long-sleep blockers
    2. Submit a query that enters the admission queue
    3. After ~1s, kill blockers — the waiter accumulates measurable wait time
    4. Check:
       a. system.events has a non-zero global counter
       b. system.query_log has a per-query counter for the queued query
    """
    prefix = uuid.uuid4().hex[:8]
    blocker_ids = [f"waittime_blocker_{prefix}_{i}" for i in range(2)]
    waiter_id = f"waittime_waiter_{prefix}"

    pool = Pool(4)

    def run_blocker(qid):
        node.query(
            "SELECT sleep(30) FORMAT Null",
            settings={
                "function_sleep_max_microseconds_per_block": 0,
                "queue_max_wait_ms": 60000,
            },
            query_id=qid,
        )

    for qid in blocker_ids:
        pool.apply_async(run_blocker, (qid,))

    for qid in blocker_ids:
        wait_for_query_start(node, qid)

    # Submit a query that will queue up and wait for a blocker to finish
    def run_waiter():
        node.query(
            "SELECT 1",
            settings={"queue_max_wait_ms": 60000},
            query_id=waiter_id,
        )

    pool.apply_async(run_waiter)

    # Let the waiter accumulate ~1s of queue wait time, then kill blockers
    time.sleep(1)
    for qid in blocker_ids:
        node.query(f"KILL QUERY WHERE query_id = '{qid}' SYNC")

    pool.close()
    pool.join()

    # All queries finished — slots are now free for diagnostic queries

    # Check global ProfileEvent via Prometheus (doesn't need a slot)
    resp = requests.get(f"http://{node.ip_address}:9363/metrics", timeout=5)
    resp.raise_for_status()
    global_wait_us = 0
    for line in resp.text.splitlines():
        m = re.match(r"^ClickHouseProfileEvents_QueryAdmissionQueueWaitMicroseconds\s+(\d+)", line)
        if m:
            global_wait_us = int(m.group(1))
            break

    assert global_wait_us > 0, (
        f"Expected global QueryAdmissionQueueWaitMicroseconds > 0, got {global_wait_us}"
    )

    # Check per-query ProfileEvent in query_log
    node.query("SYSTEM FLUSH LOGS")

    per_query_wait_us = node.query(
        f"SELECT ProfileEvents['QueryAdmissionQueueWaitMicroseconds'] "
        f"FROM system.query_log "
        f"WHERE query_id = '{waiter_id}' AND type = 'QueryFinish'"
    ).strip()

    assert per_query_wait_us != "", (
        f"Query {waiter_id} not found in query_log"
    )
    assert int(per_query_wait_us) > 0, (
        f"Expected per-query wait time > 0, got {per_query_wait_us}"
    )


def test_max_execution_time_fallback_timeout(started_cluster):
    """
    Verify that when queue_max_wait_ms is 0 (default), the admission queue
    uses max_execution_time as the wait timeout.

    Strategy:
    1. Saturate both slots with blocker queries
    2. Submit a query with queue_max_wait_ms=0 but max_execution_time=1 (1 second)
    3. The query should timeout in the admission queue after ~1 second
    4. Error should be TOO_MANY_SIMULTANEOUS_QUERIES
    """
    prefix = uuid.uuid4().hex[:8]
    blocker_ids = [f"fallback_blocker_{prefix}_{i}" for i in range(2)]

    pool = Pool(4)

    def run_blocker(qid):
        node.query(
            "SELECT sleep(30) FORMAT Null",
            settings={
                "function_sleep_max_microseconds_per_block": 0,
                "queue_max_wait_ms": 60000,
            },
            query_id=qid,
        )

    for qid in blocker_ids:
        pool.apply_async(run_blocker, (qid,))

    for qid in blocker_ids:
        wait_for_query_start(node, qid)

    # queue_max_wait_ms=0 (default), max_execution_time=1s
    # The effective wait timeout should be 1000ms (from max_execution_time)
    start = time.monotonic()
    error = node.query_and_get_error(
        "SELECT 1",
        settings={
            "queue_max_wait_ms": 0,
            "max_execution_time": 1,
        },
    )
    elapsed = time.monotonic() - start

    assert "TOO_MANY_SIMULTANEOUS_QUERIES" in error, (
        f"Expected TOO_MANY_SIMULTANEOUS_QUERIES, got: {error}"
    )
    # Should take ~1s (max_execution_time), not instant
    assert elapsed >= 0.8, (
        f"Expected ~1s wait (max_execution_time fallback), but only waited {elapsed:.2f}s"
    )
    assert elapsed < 5, (
        f"Waited too long ({elapsed:.2f}s), expected ~1s"
    )

    # Clean up
    for qid in blocker_ids:
        node.query(f"KILL QUERY WHERE query_id = '{qid}' SYNC")

    pool.close()
    pool.join()


def test_no_timeout_when_both_zero(started_cluster):
    """
    Verify that when both queue_max_wait_ms=0 and max_execution_time=0,
    the query waits in the admission queue (capped at DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC)
    rather than timing out immediately.

    Strategy:
    1. Saturate both slots with blocker queries
    2. Submit a query with both timeouts at 0 — it should wait (not instant reject)
    3. After 2 seconds, confirm the query is still waiting (not timed out)
    4. Kill a blocker to free a slot — the waiting query should succeed
    """
    prefix = uuid.uuid4().hex[:8]
    blocker_ids = [f"noto_blocker_{prefix}_{i}" for i in range(2)]
    waiter_id = f"noto_waiter_{prefix}"

    pool = Pool(4)

    def run_blocker(qid):
        node.query(
            "SELECT sleep(30) FORMAT Null",
            settings={
                "function_sleep_max_microseconds_per_block": 0,
                "queue_max_wait_ms": 60000,
            },
            query_id=qid,
        )

    for qid in blocker_ids:
        pool.apply_async(run_blocker, (qid,))

    for qid in blocker_ids:
        wait_for_query_start(node, qid)

    # Submit a query with no timeout — should wait indefinitely
    waiter_result = [None]

    def run_waiter():
        try:
            result = node.query(
                "SELECT 'waited_ok'",
                settings={
                    "queue_max_wait_ms": 0,
                    "max_execution_time": 0,
                },
                query_id=waiter_id,
            )
            waiter_result[0] = result.strip()
        except Exception as e:
            waiter_result[0] = f"ERROR: {e}"

    pool.apply_async(run_waiter)

    # Wait 2 seconds — the query should still be waiting (not timed out)
    time.sleep(2)

    # Verify the waiter is in the admission queue (metric > 0)
    queue_len = get_prometheus_metric(node, "QueryAdmissionQueueLength")
    assert queue_len >= 1, (
        f"Expected queue length >= 1 (query should be waiting), got {queue_len}"
    )

    # Kill one blocker to free a slot — the waiting query should get admitted
    node.query(f"KILL QUERY WHERE query_id = '{blocker_ids[0]}' SYNC")

    # Wait for the waiter to finish
    time.sleep(1)

    # Kill the remaining blocker
    node.query(f"KILL QUERY WHERE query_id = '{blocker_ids[1]}' SYNC")

    pool.close()
    pool.join()

    assert waiter_result[0] == "waited_ok", (
        f"Expected 'waited_ok', got: {waiter_result[0]}"
    )
