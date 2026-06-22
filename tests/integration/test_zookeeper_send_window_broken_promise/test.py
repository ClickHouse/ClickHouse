#!/usr/bin/env python3
# Regression test for a broken-promise server abort in Coordination::ZooKeeper::sendThread.
#
# In sendThread a popped request is the sole owner of its callback until it is registered in
# `operations`. The steps before that (span finalize, addRootPath, the map insert) can throw,
# and without a guard the request unwinds with an unsatisfied callback, so an async caller
# waiting on a std::promise sees a broken-promise future_error that aborts the server.
#
# The zk_send_thread_request_window_throw failpoint throws inside exactly that window. With the
# guard the callback is satisfied with a session error and the server stays up; without it the
# server aborts (caught here because the node enables abort_on_logical_error).

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


def test_send_window_throw_does_not_abort_server(started_cluster):
    # Warm up the session so the failpoint lands on a normal request, not the connect handshake.
    node.query("SELECT name FROM system.zookeeper WHERE path = '/' LIMIT 1")

    pid_before = node.get_process_pid("clickhouse server")
    assert pid_before is not None

    # Drive the real sendThread request window through the failpoint. The query may succeed (the
    # client reconnects after the session is finalized) or fail with a Keeper error; either is
    # fine. What must not happen is a broken-promise abort, so each iteration re-arms the ONCE
    # failpoint and checks the server is still the same process.
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
