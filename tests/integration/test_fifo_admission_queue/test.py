"""
Tests for the admission queue in ProcessList.

When max_concurrent_queries is reached, incoming queries wait in a FIFO
admission queue (per-waiter CV with notify_one — no thundering herd). These tests verify:

1. All queued queries are eventually admitted
2. replace_running_query works correctly after passing through the admission queue
3. Queue slot is not leaked when a queued query times out
4. Client disconnect while waiting in queue is detected, and queue length metric is accurate
5. QueryAdmissionQueueWaitMicroseconds is recorded per-query and globally,
   including for waiters that time out
6. `SYSTEM RELOAD CONFIG` bypasses admission, so the limit can be raised while
   it is saturated
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

# The admission queue is opt-in, so the disconnect-aware `replace_running_query`
# wait must work with it turned off as well (see
# `test_client_disconnect_while_replacing_query_without_admission_queue`).
node_without_admission_queue = cluster.add_instance(
    "node_without_admission_queue",
    main_configs=["configs/no_admission_queue.xml"],
)

# main_configs are mounted under /etc/clickhouse-server/config.d/.
SERVER_CONFIG_PATH = "/etc/clickhouse-server/config.d/server.xml"


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


def wait_for_query_cancelled(node, query_id, timeout=30):
    """Wait until a running query is marked as cancelled in `system.processes`,
    or has already left the process list.

    A replacement query sets `is_killed` on its victim before it parks on
    `query_finished`, so observing `is_cancelled = 1` on the still-running
    victim proves the replacement has reached that wait. The cancelled victim
    can exit within a second (`sleep` checks cancellation at 1-second chunk
    boundaries), while each poll here goes through `docker exec` and takes a
    substantial fraction of a second — so the `is_cancelled = 1` window is
    easy to miss entirely. A victim that has disappeared is an equally valid
    signal: the long-running victim only leaves the process list early because
    the replacement cancelled it.
    """
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        result = node.query(
            f"SELECT is_cancelled FROM system.processes WHERE query_id = '{query_id}'"
        ).strip()
        if result == "1" or result == "":
            return
        time.sleep(0.05)
    raise RuntimeError(f"Query {query_id} was not cancelled within {timeout}s")


def wait_for_queue_length(node, expected, timeout=30):
    """Poll the Prometheus QueryAdmissionQueueLength metric until it equals `expected`.

    Uses the Prometheus endpoint so it works even when all query slots are busy.
    """
    start = time.monotonic()
    last = None
    while time.monotonic() - start < timeout:
        last = get_prometheus_metric(node, "QueryAdmissionQueueLength")
        if last == expected:
            return
        time.sleep(0.1)
    raise RuntimeError(
        f"Admission queue length did not reach {expected} within {timeout}s (last={last})"
    )


def set_max_concurrent_queries(node, value):
    """Change `max_concurrent_queries` in the config file at runtime.

    The change is applied by the background `ConfigReloader` thread (see
    `config_reload_interval_ms` in the test config), which calls
    `ProcessList::setMaxSize`. Most tests here let the background reloader apply the
    change instead of issuing `SYSTEM RELOAD CONFIG`, so that they exercise the
    reloader path; `SYSTEM RELOAD CONFIG` itself bypasses admission and works while
    the limit is saturated (see `test_reload_config_bypasses_admission_queue`).
    """
    node.replace_in_config(
        SERVER_CONFIG_PATH,
        "<max_concurrent_queries>[0-9]*</max_concurrent_queries>",
        f"<max_concurrent_queries>{value}</max_concurrent_queries>",
    )


def wait_for_max_concurrent_queries(node, expected, timeout=30):
    """Wait until the background reloader has applied max_concurrent_queries=expected.

    Issues a plain SELECT against system.server_settings, so it must only be
    called when query slots are free (otherwise it would queue in admission).
    """
    start = time.monotonic()
    last = None
    while time.monotonic() - start < timeout:
        last = node.query(
            "SELECT value FROM system.server_settings WHERE name = 'max_concurrent_queries'"
        ).strip()
        if last == str(expected):
            return
        time.sleep(0.1)
    raise RuntimeError(
        f"max_concurrent_queries did not become {expected} within {timeout}s (last={last})"
    )


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


def test_two_replacements_of_the_same_victim(started_cluster):
    """
    Two concurrent `replace_running_query` queries sharing one victim's `query_id` must not both
    register.

    The `replace_running_query` wait happens before a query takes an admission slot, and the
    admission wait releases the `ProcessList` mutex. So both replacements can observe the victim
    gone, both leave the replacement stage, and both queue for admission. If the second one is
    admitted while the first is still running, it must be rejected with
    `QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING` instead of silently failing to register and later
    terminating the server in `~ProcessListEntry`.

    With `max_concurrent_queries = 2`:

    1. A blocker and the victim occupy both slots.
    2. An unrelated query queues for admission, so that when the victim leaves, its slot is handed
       to that query and the limit stays saturated.
    3. Both replacements arrive, cancel the victim, and — once it is gone — queue for admission.
    4. Killing the queued unrelated query admits the first replacement, which registers the shared
       `query_id` and keeps running.
    5. Killing the blocker admits the second replacement, which now finds the `query_id` taken.
    """
    prefix = uuid.uuid4().hex[:8]
    blocker_id = f"dup_blocker_{prefix}"
    queued_id = f"dup_queued_{prefix}"
    victim_id = f"dup_victim_{prefix}"

    long_query_settings = {
        "function_sleep_max_microseconds_per_block": 0,
        "queue_max_wait_ms": 60000,
    }

    pool = Pool(4)

    def run_and_swallow(query_id, settings):
        def run():
            try:
                node.query(
                    "SELECT sleep(30) FORMAT Null",
                    settings=settings,
                    query_id=query_id,
                )
            except Exception:
                pass  # Expected: the query is killed or replaced.

        return run

    pool.apply_async(run_and_swallow(blocker_id, long_query_settings))
    pool.apply_async(run_and_swallow(victim_id, long_query_settings))

    wait_for_query_start(node, blocker_id)
    wait_for_query_start(node, victim_id)

    # Both slots are busy: this one waits in the admission queue and will inherit the victim's slot.
    pool.apply_async(run_and_swallow(queued_id, long_query_settings))
    wait_for_queue_length(node, 1)

    replacement_results = [None, None]

    def run_replacement(index):
        def run():
            try:
                node.query(
                    "SELECT sleep(30) FORMAT Null",
                    settings={
                        "function_sleep_max_microseconds_per_block": 0,
                        "replace_running_query": 1,
                        "replace_running_query_max_wait_ms": 60000,
                        "queue_max_wait_ms": 60000,
                    },
                    query_id=victim_id,
                )
                replacement_results[index] = "OK"
            except Exception as e:
                replacement_results[index] = str(e)

        return run

    pool.apply_async(run_replacement(0))
    pool.apply_async(run_replacement(1))

    # The victim is cancelled by the replacements; its slot goes to the queued query (FIFO), so both
    # replacements have to queue as well.
    wait_for_queue_length(node, 2)

    # Free one slot: the first replacement is admitted and registers the shared query_id.
    node.query(f"KILL QUERY WHERE query_id = '{queued_id}' SYNC")
    wait_for_query_start(node, victim_id)

    # Free another slot: the second replacement is admitted while the first one still runs.
    node.query(f"KILL QUERY WHERE query_id = '{blocker_id}' SYNC")

    rejected = None
    start = time.monotonic()
    while time.monotonic() - start < 60:
        rejected = [r for r in replacement_results if r and "QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING" in r]
        if rejected:
            break
        time.sleep(0.1)

    # Whichever replacement won the race is still sleeping; stop it so the pool can be joined.
    node.query(f"KILL QUERY WHERE query_id = '{victim_id}' SYNC")
    pool.close()
    pool.join()

    assert rejected, (
        f"Expected one replacement to be rejected as a duplicate, got: {replacement_results}"
    )
    assert len(rejected) == 1, (
        f"Expected exactly one replacement to be rejected, got: {replacement_results}"
    )

    # The server must still be alive and the query_id free again.
    assert node.query("SELECT 1").strip() == "1"


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


def check_client_disconnect_while_replacing_query(target):
    """
    Verify that a replacement query whose HTTP client disconnects while the old
    query is leaving the process list is not admitted for execution.

    The replacement cancels the original query and waits on `query_finished`.
    Closing its socket immediately after sending the request exercises the
    post-wakeup liveness check: once the original query disappears, the
    replacement must stop before it creates a second `QueryStart` entry.
    """
    query_id = f"replace_disconnect_{uuid.uuid4().hex[:8]}"
    pool = Pool(2)

    def run_victim():
        try:
            target.query(
                "SELECT sleep(30) FORMAT Null",
                settings={"function_sleep_max_microseconds_per_block": 0},
                query_id=query_id,
            )
        except Exception:
            pass  # Expected: the replacement cancels the victim.

    pool.apply_async(run_victim)
    wait_for_query_start(target, query_id)

    params = urllib.parse.urlencode({
        "query": "SELECT 'replacement ran'",
        "query_id": query_id,
        "replace_running_query": "1",
        "replace_running_query_max_wait_ms": "30000",
    })
    request = (
        f"GET /?{params} HTTP/1.1\r\n"
        f"Host: {target.ip_address}\r\n"
        f"Connection: close\r\n"
        f"\r\n"
    )

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)
    sock.connect((target.ip_address, 8123))
    sock.sendall(request.encode())

    # Half-close right away instead of resetting the connection later. A `SHUT_WR`
    # delivers the request bytes and then a FIN, so the server still runs the request
    # but every liveness check on it reports the peer as closed from the very first
    # one. Waiting for an observable state and only then resetting the connection is
    # racy: the victim leaves the process list within a second of being cancelled, so
    # on a slow build the replacement can wake up, see the victim gone and pass its
    # post-wakeup liveness check before the reset is even sent.
    sock.shutdown(socket.SHUT_WR)

    # The replacement marks the victim as cancelled before it parks on `query_finished`,
    # so `is_cancelled = 1` on the still-running victim means the replacement has reached
    # that wait with an already-disconnected client.
    wait_for_query_cancelled(target, query_id)
    sock.close()

    try:
        wait_for_query_finish(target, query_id)
        target.query("SYSTEM FLUSH LOGS")
        starts = target.query(
            f"SELECT count() FROM system.query_log "
            f"WHERE query_id = '{query_id}' AND type = 'QueryStart'"
        ).strip()
        assert starts == "1", (
            "Disconnected replacement query was admitted for execution "
            f"(QueryStart count: {starts})"
        )
    finally:
        target.query(f"KILL QUERY WHERE query_id = '{query_id}' SYNC")
        pool.close()
        pool.join()


def test_client_disconnect_while_replacing_query(started_cluster):
    check_client_disconnect_while_replacing_query(node)


def test_client_disconnect_while_replacing_query_without_admission_queue(started_cluster):
    """
    The same check with `enable_query_admission_queue = 0` (the default).

    The FIFO admission queue is opt-in, but the connection-liveness check in the
    `replace_running_query` wait must not be: without it a disconnected client
    would still wait out `replace_running_query_max_wait_ms` and then run the
    replacement query against a socket nobody is reading.
    """
    check_client_disconnect_while_replacing_query(node_without_admission_queue)


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


def get_prometheus_profile_event(node, event_name, timeout=5):
    """Read a global ProfileEvent counter from the Prometheus /metrics endpoint.

    Like `get_prometheus_metric`, this bypasses the query pipeline, so it works
    while every execution slot is occupied.
    """
    resp = requests.get(f"http://{node.ip_address}:9363/metrics", timeout=timeout)
    resp.raise_for_status()
    pattern = rf"^ClickHouseProfileEvents_{event_name}\s+(\d+)"
    for line in resp.text.splitlines():
        m = re.match(pattern, line)
        if m:
            return int(m.group(1))
    return 0


def test_timed_out_waiter_records_wait_time(started_cluster):
    """
    Verify that a waiter which times out in the admission queue still contributes
    its waiting time to `QueryAdmissionQueueWaitMicroseconds`.

    The event is documented as the total time spent waiting in the FIFO admission
    queue, and a timed-out waiter is exactly the overload case that spends the most
    time queued, so it must not report zero.

    Strategy:
    1. Read the global counter
    2. Saturate both slots
    3. Submit a query with a short `queue_max_wait_ms` — it times out in the queue
    4. The global counter must have grown by roughly the timeout
    """
    prefix = uuid.uuid4().hex[:8]
    blocker_ids = [f"timedout_wait_blocker_{prefix}_{i}" for i in range(2)]
    timeout_ms = 2000

    before_us = get_prometheus_profile_event(
        node, "QueryAdmissionQueueWaitMicroseconds"
    )

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

    error = node.query_and_get_error(
        "SELECT 1",
        settings={"queue_max_wait_ms": timeout_ms},
    )
    assert "TOO_MANY_SIMULTANEOUS_QUERIES" in error

    after_us = get_prometheus_profile_event(
        node, "QueryAdmissionQueueWaitMicroseconds"
    )

    for qid in blocker_ids:
        node.query(f"KILL QUERY WHERE query_id = '{qid}' SYNC")

    pool.close()
    pool.join()

    # Allow for scheduling slack: require at least half of the configured timeout.
    assert after_us - before_us >= timeout_ms * 1000 / 2, (
        f"Expected the timed-out waiter to add ~{timeout_ms} ms of queue wait time, "
        f"got {after_us - before_us} us (before={before_us}, after={after_us})"
    )


def test_reload_config_bypasses_admission_queue(started_cluster):
    """
    Verify that `SYSTEM RELOAD CONFIG` is not subject to admission control.

    `SYSTEM RELOAD CONFIG` is the way to raise `max_concurrent_queries` at runtime.
    If it queued behind the very limit it raises, the documented relief path would be
    unavailable exactly when it is needed, so it must run while the limit is saturated
    and its effect must reach the already queued waiters.

    Strategy:
    1. Saturate both slots and queue one waiter behind them
    2. Raise `max_concurrent_queries` in the config file and apply it with
       `SYSTEM RELOAD CONFIG` — the command itself must not queue or time out
    3. The queued waiter is admitted on the newly raised limit, while the blockers
       are still running
    4. Restore the original limit
    """
    prefix = uuid.uuid4().hex[:8]
    blocker_ids = [f"reload_blocker_{prefix}_{i}" for i in range(2)]
    waiter_id = f"reload_waiter_{prefix}"

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

    def run_waiter():
        node.query(
            "SELECT sleep(3) FORMAT Null",
            settings={
                "function_sleep_max_microseconds_per_block": 0,
                "queue_max_wait_ms": 60000,
            },
            query_id=waiter_id,
        )

    pool.apply_async(run_waiter)
    wait_for_queue_length(node, 1)

    try:
        set_max_concurrent_queries(node, 4)

        # Both slots are busy: this must not queue behind them, and must not time out.
        node.query("SYSTEM RELOAD CONFIG", settings={"queue_max_wait_ms": 5000})

        # The relief is effective: the queued waiter starts while the blockers still run.
        wait_for_query_start(node, waiter_id)
        assert get_prometheus_metric(node, "QueryAdmissionQueueLength") == 0
    finally:
        for qid in blocker_ids:
            node.query(f"KILL QUERY WHERE query_id = '{qid}' SYNC")
        node.query(f"KILL QUERY WHERE query_id = '{waiter_id}' SYNC")

        pool.close()
        pool.join()

        set_max_concurrent_queries(node, 2)
        node.query("SYSTEM RELOAD CONFIG")
        wait_for_max_concurrent_queries(node, 2)


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


def test_runtime_unlimit_drains_admission_queue(started_cluster):
    """
    Verify that switching `max_concurrent_queries` to 0 (unlimited) at runtime
    drains queued waiters instead of stranding them until `queue_max_wait_ms`.

    Without `setMaxSize` draining the queue on config reload, finishing queries
    decrement `admission_running` (they don't transfer the slot) and new queries
    bypass admission entirely when `max_size == 0`, so existing waiters would be
    stuck until their timeout. Here the blockers keep running, so the only way
    the waiters can finish is by being drained on reload.

    Strategy:
    1. Saturate both slots with long blockers
    2. Submit 3 waiters — they enter the admission queue
    3. Reload config with max_concurrent_queries=0 (unlimited)
    4. All 3 waiters drain and finish quickly, while blockers still run
    """
    prefix = uuid.uuid4().hex[:8]
    blocker_ids = [f"unlimit_blocker_{prefix}_{i}" for i in range(2)]
    waiter_ids = [f"unlimit_waiter_{prefix}_{i}" for i in range(3)]

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

    def run_waiter(qid):
        node.query(
            "SELECT 1 FORMAT Null",
            settings={"queue_max_wait_ms": 60000},
            query_id=qid,
        )

    try:
        # Baseline limit must be active before we saturate it (slots are free here).
        wait_for_max_concurrent_queries(node, 2)

        for qid in blocker_ids:
            pool.apply_async(run_blocker, (qid,))
        for qid in blocker_ids:
            wait_for_query_start(node, qid)

        for qid in waiter_ids:
            pool.apply_async(run_waiter, (qid,))

        # All 3 waiters should be queued (both slots are held by blockers).
        wait_for_queue_length(node, 3)

        # Switch to unlimited at runtime — the background reloader must drain the
        # queue even though the blockers keep holding their slots (the blockers
        # never finish on their own within the test, so the drain is the only path).
        set_max_concurrent_queries(node, 0)

        # Waiters drain and finish; the queue empties.
        wait_for_queue_length(node, 0)
        for qid in waiter_ids:
            wait_for_query_finish(node, qid)

        node.query("SYSTEM FLUSH LOGS")
        id_list = ", ".join(f"'{qid}'" for qid in waiter_ids)
        finished = node.query(
            f"""
            SELECT count()
            FROM system.query_log
            WHERE query_id IN ({id_list}) AND type = 'QueryFinish'
            """
        ).strip()
        assert int(finished) == len(waiter_ids), (
            f"Expected all {len(waiter_ids)} waiters to finish after unlimiting, got {finished}"
        )
    finally:
        # KILL QUERY bypasses admission, so it works even with a full queue.
        for qid in blocker_ids:
            node.query(f"KILL QUERY WHERE query_id = '{qid}' SYNC")
        pool.close()
        pool.join()
        # Restore the original limit and confirm it before the next test runs.
        set_max_concurrent_queries(node, 2)
        wait_for_max_concurrent_queries(node, 2)


def test_runtime_increase_preserves_fifo(started_cluster):
    """
    Verify that raising `max_concurrent_queries` at runtime hands the freed
    slots to the oldest queued waiters first (FIFO is not violated).

    Strategy:
    1. Saturate both slots with long blockers
    2. Submit 3 waiters one at a time, confirming each enters the queue before
       the next, so the FIFO order is deterministically waiter_0 < 1 < 2
    3. Raise max_concurrent_queries from 2 to 3 — exactly one slot opens for the
       queue, so exactly one waiter (the oldest) must be admitted
    4. Verify the oldest waiter is now running and the two younger ones are still
       queued (queue length 2)
    """
    prefix = uuid.uuid4().hex[:8]
    blocker_ids = [f"fifo_blocker_{prefix}_{i}" for i in range(2)]
    waiter_ids = [f"fifo_waiter_{prefix}_{i}" for i in range(3)]

    pool = Pool(10)

    def run_long(qid):
        # Long-running so an admitted waiter keeps holding its slot, which lets
        # us observe exactly which waiter was admitted.
        node.query(
            "SELECT sleep(30) FORMAT Null",
            settings={
                "function_sleep_max_microseconds_per_block": 0,
                "queue_max_wait_ms": 60000,
            },
            query_id=qid,
        )

    try:
        # Baseline limit must be active before we saturate it (slots are free here).
        wait_for_max_concurrent_queries(node, 2)

        for qid in blocker_ids:
            pool.apply_async(run_long, (qid,))
        for qid in blocker_ids:
            wait_for_query_start(node, qid)

        # Submit waiters one at a time, establishing a deterministic FIFO order.
        for i, qid in enumerate(waiter_ids):
            pool.apply_async(run_long, (qid,))
            wait_for_queue_length(node, i + 1)

        # Raise the limit by one: admission_running is 2 (blockers), so exactly
        # one waiter — the oldest — is drained from the front of the queue.
        set_max_concurrent_queries(node, 3)

        # The oldest waiter must now be running.
        wait_for_query_start(node, waiter_ids[0])

        # The two younger waiters must still be queued, not overtaken.
        wait_for_queue_length(node, 2)
        for qid in waiter_ids[1:]:
            running = node.query(
                f"SELECT count() FROM system.processes WHERE query_id = '{qid}'"
            ).strip()
            assert running == "0", (
                f"Waiter {qid} was admitted out of FIFO order (oldest must go first)"
            )
    finally:
        # Drain the queue (unlimited) so every remaining waiter is admitted and
        # therefore appears in system.processes — a queued waiter is not yet
        # killable, so killing before draining would let cleanup block on the
        # 30s sleep.
        set_max_concurrent_queries(node, 0)
        for qid in waiter_ids:
            try:
                wait_for_query_start(node, qid, timeout=10)
            except RuntimeError:
                pass  # already finished or never admitted
        for qid in blocker_ids + waiter_ids:
            node.query(f"KILL QUERY WHERE query_id = '{qid}' SYNC")
        pool.close()
        pool.join()
        # Restore the original limit and confirm it before the next test runs.
        set_max_concurrent_queries(node, 2)
        wait_for_max_concurrent_queries(node, 2)


def test_normal_release_preserves_fifo(started_cluster):
    """
    Verify that releasing a single admission slot on ordinary query completion
    hands it to the oldest queued waiter first (FIFO is not violated).

    `test_runtime_increase_preserves_fifo` only proves FIFO for the runtime
    `setMaxSize` drain path (a `max_concurrent_queries` increase). This test
    covers the normal release path instead: a running query finishes and its
    `ProcessListEntry` teardown calls `releaseAdmissionSlotLocked`, which must
    transfer the freed slot to the *front* of the queue. A regression that
    granted from the back of `admission_queue` would still pass
    `test_all_queued_queries_admitted` (which only checks eventual completion,
    not admission order), so this asserts the ordering of the handoff directly.

    Strategy (server config: max_concurrent_queries = 2):
    1. Saturate both slots with two long blockers.
    2. Submit 3 waiters one at a time, confirming each enters the queue before
       the next, so the FIFO order is deterministically waiter_0 < 1 < 2.
    3. KILL exactly one blocker. Its `ProcessListEntry` teardown releases exactly
       one admission slot through `releaseAdmissionSlotLocked`, transferring it
       to the front waiter.
    4. Verify the oldest waiter is now running and the two younger ones are still
       queued (queue length 2), i.e. not overtaken.
    """
    prefix = uuid.uuid4().hex[:8]
    blocker_ids = [f"normfifo_blocker_{prefix}_{i}" for i in range(2)]
    waiter_ids = [f"normfifo_waiter_{prefix}_{i}" for i in range(3)]

    pool = Pool(10)

    def run_long(qid):
        # Long-running so an admitted waiter keeps holding its slot, which lets
        # us observe exactly which waiter was admitted.
        node.query(
            "SELECT sleep(30) FORMAT Null",
            settings={
                "function_sleep_max_microseconds_per_block": 0,
                "queue_max_wait_ms": 60000,
            },
            query_id=qid,
        )

    try:
        # Baseline limit must be active before we saturate it (slots are free here).
        wait_for_max_concurrent_queries(node, 2)

        for qid in blocker_ids:
            pool.apply_async(run_long, (qid,))
        for qid in blocker_ids:
            wait_for_query_start(node, qid)

        # Submit waiters one at a time, establishing a deterministic FIFO order.
        for i, qid in enumerate(waiter_ids):
            pool.apply_async(run_long, (qid,))
            wait_for_queue_length(node, i + 1)

        # Release exactly one slot via the normal path: kill a single blocker.
        # Its teardown frees one admission slot, which `releaseAdmissionSlotLocked`
        # must hand to the front (oldest) waiter — not to a younger one.
        node.query(f"KILL QUERY WHERE query_id = '{blocker_ids[0]}' SYNC")

        # The oldest waiter must now be running.
        wait_for_query_start(node, waiter_ids[0])

        # Exactly one slot opened, so the two younger waiters must still be
        # queued, not overtaken.
        wait_for_queue_length(node, 2)
        for qid in waiter_ids[1:]:
            running = node.query(
                f"SELECT count() FROM system.processes WHERE query_id = '{qid}'"
            ).strip()
            assert running == "0", (
                f"Waiter {qid} was admitted out of FIFO order (oldest must go first)"
            )
    finally:
        # Drain the queue (unlimited) so every remaining waiter is admitted and
        # therefore appears in system.processes — a queued waiter is not yet
        # killable, so killing before draining would let cleanup block on the
        # 30s sleep.
        set_max_concurrent_queries(node, 0)
        for qid in waiter_ids:
            try:
                wait_for_query_start(node, qid, timeout=10)
            except RuntimeError:
                pass  # already finished or never admitted
        for qid in blocker_ids + waiter_ids:
            node.query(f"KILL QUERY WHERE query_id = '{qid}' SYNC")
        pool.close()
        pool.join()
        # Restore the original limit and confirm it before the next test runs.
        set_max_concurrent_queries(node, 2)
        wait_for_max_concurrent_queries(node, 2)


def test_secondary_limit_not_rejected_on_early_release(started_cluster):
    """
    Regression test for the admission handoff vs secondary concurrency limits.

    The admission slot is released early in `executeQuery` (same timing as the
    resource-scheduler `QuerySlot`), before the finishing query's
    `ProcessListEntry` destructor decrements `non_internal_processes` (and the
    per-user counter). When `max_concurrent_queries_for_all_users` (or
    `max_concurrent_queries_for_user`) equals `max_concurrent_queries`, a waiter
    that just received the transferred admission slot used to reach the secondary
    check while the finishing query was still counted, and was rejected with
    `TOO_MANY_SIMULTANEOUS_QUERIES` ("for all users"/"for user") — whereas the
    legacy `max_size` path would keep it waiting and then run it.

    The fix makes the FIFO path wait on `query_finished` for the in-flight
    teardown to drain (bounded by `queue_max_wait_ms`) instead of throwing. With
    the fix every query completes; without it some intermittently fail at the
    secondary-limit check.

    The window between early release and destructor is short, so we drive a
    steady stream of naturally-finishing queries (not killed — a killed query
    releases its slot only in the destructor, so it never opens the window) and
    assert that none is rejected by the secondary limits.
    """
    # server config: max_concurrent_queries = 2.
    limit = 2

    for setting in ("max_concurrent_queries_for_all_users", "max_concurrent_queries_for_user"):
        prefix = uuid.uuid4().hex[:8]
        num_queries = 60
        pool = Pool(num_queries)

        errors = []

        def run_query(i):
            try:
                # A tiny bit of real work so the queries genuinely contend for the
                # two slots and finish naturally, producing admission handoffs.
                node.query(
                    "SELECT sum(number) FROM numbers(200000) FORMAT Null",
                    settings={
                        setting: limit,
                        "queue_max_wait_ms": 60000,
                    },
                    query_id=f"sec_{setting}_{prefix}_{i}",
                )
            except Exception as e:
                errors.append(str(e))

        for i in range(num_queries):
            pool.apply_async(run_query, (i,))
        pool.close()
        pool.join()

        # No query may be rejected by the secondary limit: the waiter held a valid
        # admission slot and must wait out the early-release window, not fail.
        offending = [e for e in errors if "Too many simultaneous queries" in e]
        assert not offending, (
            f"{setting}: {len(offending)}/{num_queries} queries were rejected by the "
            f"secondary concurrency limit while holding an admission slot; "
            f"first error: {offending[0]}"
        )


def test_secondary_limit_not_rejected_on_early_release_default_wait(started_cluster):
    """
    Same regression as `test_secondary_limit_not_rejected_on_early_release`, but for
    the default `queue_max_wait_ms = 0` path.

    With `queue_max_wait_ms = 0` the FIFO admission wait falls back to
    `max_execution_time` (then 300s). The secondary-limit checks used to gate their
    post-handoff wait on `queue_max_wait_ms` directly, so with the default value of
    `0` they threw `TOO_MANY_SIMULTANEOUS_QUERIES` immediately instead of waiting for
    the finishing query's `ProcessListEntry` destructor to drain the counter — even
    though the waiter already held a valid admission slot.

    The fix makes both secondary checks wait on `query_finished` until the shared
    admission deadline (derived from the effective admission budget) whenever an
    admission slot was transferred, so the default path waits out the early-release
    window too. A generous `max_execution_time` bounds the fallback budget so a genuine
    failure still terminates the test instead of hanging for 300s.
    """
    # server config: max_concurrent_queries = 2.
    limit = 2

    for setting in ("max_concurrent_queries_for_all_users", "max_concurrent_queries_for_user"):
        prefix = uuid.uuid4().hex[:8]
        num_queries = 60
        pool = Pool(num_queries)

        errors = []

        def run_query(i):
            try:
                node.query(
                    "SELECT sum(number) FROM numbers(200000) FORMAT Null",
                    settings={
                        setting: limit,
                        # Default admission wait: queue_max_wait_ms = 0 → falls back to
                        # max_execution_time for the effective budget.
                        "queue_max_wait_ms": 0,
                        "max_execution_time": 60,
                    },
                    query_id=f"secdef_{setting}_{prefix}_{i}",
                )
            except Exception as e:
                errors.append(str(e))

        for i in range(num_queries):
            pool.apply_async(run_query, (i,))
        pool.close()
        pool.join()

        offending = [e for e in errors if "Too many simultaneous queries" in e]
        assert not offending, (
            f"{setting} (default queue_max_wait_ms): {len(offending)}/{num_queries} queries "
            f"were rejected by the secondary concurrency limit while holding an admission "
            f"slot; first error: {offending[0]}"
        )


def test_fast_path_slot_not_hoarded_at_secondary_limit(started_cluster):
    """
    Regression test for admission-slot hoarding at the secondary concurrency limits.

    The secondary-limit checks used to park a slot-holding query on `query_finished`
    until the shared admission deadline whenever the limit was full: a query that hit
    a full `max_concurrent_queries_for_user` (or query-kind) limit waited for up to
    the whole admission budget while still holding a global admission slot — so a
    query of a different user (or kind) arriving meanwhile had to queue even though
    a global execution slot was actually idle.

    The fix bounds that wait by the early-release teardown drain
    (`admission_pending_teardowns`): once no finishing query is still counted toward
    the secondary counters, a full limit is genuinely full of running queries, and
    the query is rejected immediately instead of parking; the rollback guard then
    hands its slot to the next waiter.

    Scenario (server config: max_concurrent_queries = 2):
    1. A (user `default`) runs a long query — 1 of 2 global slots used.
    2. B (user `default`, `max_concurrent_queries_for_user = 1`) arrives: takes the
       fast path, hits the per-user limit, which is full of genuinely running
       queries (no teardown in flight). It must be rejected immediately, releasing
       its slot — not park until the admission deadline.
    3. C (a different user) arrives: it must run immediately on the free global
       slot, not queue behind B.
    """
    prefix = uuid.uuid4().hex[:8]
    a_id = f"hoard_a_{prefix}"
    other_user = f"admission_other_{prefix}"

    node.query(f"CREATE USER {other_user} IDENTIFIED WITH no_password")
    pool = Pool(2)

    try:
        # A: occupies one global slot and the whole per-user budget used by B.
        def run_blocker():
            node.query(
                "SELECT sleep(30) FORMAT Null",
                settings={"function_sleep_max_microseconds_per_block": 0},
                query_id=a_id,
            )

        pool.apply_async(run_blocker)
        wait_for_query_start(node, a_id)

        # B: fast path (1 of 2 slots used), then the per-user limit is full because
        # of A. Without the fix it parks here for up to max_execution_time (60s)
        # holding the second global slot; with the fix it fails immediately.
        b_result = {}

        def run_secondary_limited():
            start = time.monotonic()
            try:
                node.query(
                    "SELECT 1 FORMAT Null",
                    settings={
                        "max_concurrent_queries_for_user": 1,
                        "max_execution_time": 60,
                    },
                    query_id=f"hoard_b_{prefix}",
                )
                b_result["error"] = None
            except Exception as e:
                b_result["error"] = str(e)
            b_result["elapsed"] = time.monotonic() - start

        b_future = pool.apply_async(run_secondary_limited)
        # Give B time to reach the per-user limit (without the fix: to park there).
        time.sleep(2)

        # C: different user, different per-user budget. Must run on the free global
        # slot right away. Without the fix, B holds that slot, so C queues in
        # admission and times out after its effective budget (max_execution_time).
        c_start = time.monotonic()
        node.query(
            "SELECT 1 FORMAT Null",
            settings={"max_execution_time": 10},
            query_id=f"hoard_c_{prefix}",
            user=other_user,
        )
        c_elapsed = time.monotonic() - c_start

        # A must still be running — C ran concurrently, not after A's teardown.
        assert node.query(
            f"SELECT count() FROM system.processes WHERE query_id = '{a_id}'"
        ).strip() == "1", "Blocker query finished too early, the test is inconclusive"

        assert c_elapsed < 8, (
            f"Query of another user took {c_elapsed:.1f}s — it queued behind the "
            f"secondary-limit waiter that hoarded a fast-path admission slot"
        )

        b_future.get(timeout=70)
        assert b_result["error"] is not None and "Too many simultaneous queries for user" in b_result["error"], (
            f"Expected the fast-path query to be rejected at the per-user limit, got: {b_result['error']}"
        )
        assert b_result["elapsed"] < 8, (
            f"Fast-path query waited {b_result['elapsed']:.1f}s at the per-user limit "
            f"instead of failing immediately (legacy behavior)"
        )
    finally:
        node.query(f"KILL QUERY WHERE query_id = '{a_id}' SYNC")
        pool.close()
        pool.join()
        node.query(f"DROP USER IF EXISTS {other_user}")


def test_zero_to_finite_reload_enforces_limit(started_cluster):
    """
    Regression test for a runtime `max_concurrent_queries` reload from `0`
    (unlimited) to a finite value `N`.

    While `max_concurrent_queries == 0` the primary limit is unlimited, so queries
    are admitted without a FIFO wait. They must still be *counted* as holding
    admission slots, so that when the limit is later reloaded to a finite `N` the
    already-running queries are reflected in `admission_running`. Without that, the
    next `N` arrivals would observe `admission_running == 0`, take the fast path,
    and run even though more than `N` queries are already running — so the finite
    limit would not take effect until the pre-limit queries happened to drain.

    With the fix, a tracked (non-internal, non-unlimited) query holds an admission
    slot for its whole lifetime whenever the feature is enabled, even while
    unlimited, so the `0 -> N` reload immediately enforces `N` against the queries
    that were admitted while the limit was unlimited.

    Strategy (server config: max_concurrent_queries = 2):
    1. Switch to unlimited (`max_concurrent_queries = 0`) at runtime.
    2. Start 3 long queries (> N = 2). They all run concurrently (none queues).
    3. Reload back to `max_concurrent_queries = 2`.
    4. Assert a new query is now rejected (queued, then timed out) rather than
       running on the fast path — the limit is enforced against the 3 already
       running queries.
    5. Drain below the limit (kill all but one). A new query must run again, proving
       no slot was leaked across the transition.
    """
    prefix = uuid.uuid4().hex[:8]
    blocker_ids = [f"zerofin_blocker_{prefix}_{i}" for i in range(3)]

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

    try:
        # 1. Go unlimited at runtime. Slots are free here, so this confirming query
        #    is not itself subject to admission.
        set_max_concurrent_queries(node, 0)
        wait_for_max_concurrent_queries(node, 0)

        # 2. Start 3 concurrent queries while unlimited — more than the finite limit
        #    we will reload to. All of them run; none enters the queue.
        for qid in blocker_ids:
            pool.apply_async(run_blocker, (qid,))
        for qid in blocker_ids:
            wait_for_query_start(node, qid)
        assert get_prometheus_metric(node, "QueryAdmissionQueueLength") == 0

        # 3. Reload to a finite limit below the running count.
        set_max_concurrent_queries(node, 2)

        # 4. The new limit must be enforced against the already-running queries: a
        #    fresh query is queued and times out, instead of running on the fast
        #    path. Poll because the background reloader applies the change
        #    asynchronously; before it does, a probe simply runs (still unlimited)
        #    and we retry. Once enforced, every probe is rejected. With the bug the
        #    pre-limit queries are not counted, so the probe always runs and this
        #    never becomes true (the test then fails by timeout).
        deadline = time.monotonic() + 15
        enforced = False
        while time.monotonic() < deadline:
            _, error = node.query_and_get_answer_with_error(
                "SELECT 1",
                settings={"queue_max_wait_ms": 1000},
            )
            if "TOO_MANY_SIMULTANEOUS_QUERIES" in error:
                enforced = True
                break
            time.sleep(0.2)
        assert enforced, (
            "0 -> N reload did not enforce max_concurrent_queries: new queries kept "
            "running on the fast path even though more than N were already running"
        )

        # 5. Drain below the limit: with only one of the three queries left running,
        #    admission_running drops under N and queries are admitted again.
        for qid in blocker_ids[1:]:
            node.query(f"KILL QUERY WHERE query_id = '{qid}' SYNC")
        deadline = time.monotonic() + 15
        admitted = False
        while time.monotonic() < deadline:
            answer, error = node.query_and_get_answer_with_error(
                "SELECT 42",
                settings={"queue_max_wait_ms": 2000},
            )
            if error == "" and answer.strip() == "42":
                admitted = True
                break
            time.sleep(0.2)
        assert admitted, (
            "after draining below the limit, queries were still rejected — a slot was "
            "leaked across the 0 -> N transition"
        )
    finally:
        for qid in blocker_ids:
            node.query(f"KILL QUERY WHERE query_id = '{qid}' SYNC")
        pool.close()
        pool.join()
        # Restore the baseline limit for subsequent tests.
        set_max_concurrent_queries(node, 2)
        wait_for_max_concurrent_queries(node, 2)
