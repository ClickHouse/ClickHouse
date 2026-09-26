#!/usr/bin/env python3
# Regression tests for Coordination::ZooKeeper::sendThread request-window hazards.
#
# In sendThread a popped request is the sole owner of its callback until it is registered in
# `operations`. If anything in that window throws (the OpenTelemetry span finalize, addRootPath,
# the operations map insert), the request unwinds with an unsatisfied callback, and an async
# caller waiting on a std::promise sees a broken-promise future_error that aborts the server.
#
# The window can throw for reasons other than memory pressure: addRootPath raises ZBADARGUMENTS
# for an empty or non-absolute path, and that validation runs after the async wrapper has already
# enqueued the callback. The SCOPE_EXIT guard satisfies the dropped callback with a normal Keeper
# error for ANY throw in the window, so the server stays up and the waiter gets a Keeper error
# rather than std::future_error.
#
# A second hazard lives in the same window: the in-flight CurrentMetrics::ZooKeeperRequest gauge
# must be bumped only for a request that actually lands in `operations`. finalize() subtracts
# operations.size() on teardown, so an increment that happens before a throwing insert would never
# be subtracted and the gauge would leak forever. The increment is therefore gated on a successful
# insert.
#
# Two failpoints exercise the two source positions:
#   zk_send_thread_request_window_throw   - throws at the top of the window (mirrors addRootPath),
#                                           tests the broken-promise guard.
#   zk_send_thread_operations_insert_throw- throws at the operations insert site, tests that the
#                                           in-flight metric does not leak when the insert fails.

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/abort_on_logical_error.xml"],
    with_zookeeper=True,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _settled_in_flight_requests():
    # The in-flight gauge drains as soon as the outstanding requests get their responses. Poll a
    # few times and take the minimum so a request that happens to be in flight during one sample
    # does not skew the reading.
    samples = []
    for _ in range(20):
        samples.append(
            int(
                node.query(
                    "SELECT value FROM system.metrics WHERE metric = 'ZooKeeperRequest'"
                )
            )
        )
    return min(samples)


def test_send_window_throw_does_not_abort_server(started_cluster):
    # Warm up the session so the failpoint lands on a normal request, not the connect handshake.
    node.query("SELECT name FROM system.zookeeper WHERE path = '/' LIMIT 1")

    pid_before = node.get_process_pid("clickhouse server")
    assert pid_before is not None

    # Drive the real sendThread request window through the failpoint. The query may succeed or
    # fail with a Keeper error; either is fine. What must not happen is a broken-promise abort,
    # so each iteration re-arms the ONCE failpoint and checks the server is still the same process.
    for _ in range(20):
        node.query("SYSTEM ENABLE FAILPOINT zk_send_thread_request_window_throw")
        node.query_and_get_answer_with_error(
            "SELECT name FROM system.zookeeper WHERE path = '/' LIMIT 1"
        )

    node.query("SYSTEM DISABLE FAILPOINT zk_send_thread_request_window_throw")

    assert node.get_process_pid("clickhouse server") == pid_before
    assert not node.contains_in_log("The associated promise has been destructed")
    assert node.query("SELECT 1") == "1\n"
    # The session recovers and ZooKeeper is usable again.
    assert int(node.query("SELECT count() FROM system.zookeeper WHERE path = '/'")) > 0


def test_send_window_insert_throw_does_not_leak_metric(started_cluster):
    # Warm up the session so the failpoint lands on a normal request, not the connect handshake.
    node.query("SELECT name FROM system.zookeeper WHERE path = '/' LIMIT 1")

    pid_before = node.get_process_pid("clickhouse server")
    assert pid_before is not None

    baseline = _settled_in_flight_requests()

    # Throw at the operations-insert site many times. Each throw makes the popped request unwind
    # before it owns its slot in `operations`. The completion guard keeps the server up; the point
    # here is that the in-flight gauge is only bumped after a successful insert, so a failed insert
    # must not increment it. Before the fix each iteration leaked one count and the gauge would
    # climb monotonically above the baseline.
    iterations = 20
    for _ in range(iterations):
        node.query("SYSTEM ENABLE FAILPOINT zk_send_thread_operations_insert_throw")
        node.query_and_get_answer_with_error(
            "SELECT name FROM system.zookeeper WHERE path = '/' LIMIT 1"
        )

    node.query("SYSTEM DISABLE FAILPOINT zk_send_thread_operations_insert_throw")

    # Server must still be up (the completion guard handles the dropped callback).
    assert node.get_process_pid("clickhouse server") == pid_before
    assert not node.contains_in_log("The associated promise has been destructed")
    assert node.query("SELECT 1") == "1\n"

    # The in-flight gauge must settle back to the baseline. A leak would leave it at least
    # `iterations` above the baseline; allow a small slack for unrelated in-flight requests.
    after = _settled_in_flight_requests()
    assert after <= baseline + 1, (
        f"ZooKeeperRequest gauge leaked: baseline={baseline}, after={after}, "
        f"iterations={iterations}"
    )

    # The session recovers and ZooKeeper is usable again.
    assert int(node.query("SELECT count() FROM system.zookeeper WHERE path = '/'")) > 0
