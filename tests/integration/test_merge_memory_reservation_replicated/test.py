"""Regression test for the replicated merge memory reservation path.

`MergeFromLogEntryTask::prepare` reserves memory for the merge's input/output IO buffers up front
(unconditionally - this replica is already committed to running the log entry), sized against the
resolved destination disk. This drives a real ZooKeeper-backed merge through that path and observes
the reservation itself: the task is parked on the `rmt_merge_task_pause_after_reserve` failpoint right
after it has reserved, the `MergesMutationsMemoryReservation` metric is read while the merge is held
there, and after the failpoint is released the merge must complete and release the reservation.
"""

import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", with_zookeeper=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def reserved_memory():
    return int(
        node.query(
            "SELECT value FROM system.metrics WHERE metric = 'MergesMutationsMemoryReservation'"
        ).strip()
    )


def active_parts():
    return int(
        node.query(
            "SELECT count() FROM system.parts"
            " WHERE database = 'default' AND table = 't_replicated_merge_reservation' AND active"
        ).strip()
    )


def test_replicated_merge_reserves_memory(started_cluster):
    node.query("""
        CREATE TABLE t_replicated_merge_reservation (k UInt64, v String)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/t_replicated_merge_reservation', 'r1')
        ORDER BY k
        """)

    # Only this table's merges may run during the test, so the process-wide metric below can only
    # reflect the reservation of this one replicated merge.
    node.query("SYSTEM STOP MERGES")

    node.query(
        "INSERT INTO t_replicated_merge_reservation SELECT number, repeat('a', 100) FROM numbers(10000)"
    )
    node.query(
        "INSERT INTO t_replicated_merge_reservation"
        " SELECT number, repeat('b', 100) FROM numbers(10000, 10000)"
    )
    assert active_parts() == 2
    assert reserved_memory() == 0

    node.query("SYSTEM ENABLE FAILPOINT rmt_merge_task_pause_after_reserve")
    try:
        node.query("SYSTEM START MERGES t_replicated_merge_reservation")
        # The OPTIMIZE creates a MERGE_PARTS log entry and returns; the entry is executed in the
        # background by MergeFromLogEntryTask, which reserves and then parks on the failpoint.
        node.query(
            "OPTIMIZE TABLE t_replicated_merge_reservation", settings={"alter_sync": 0}
        )

        assert_eq_with_retry(
            node,
            "SELECT value > 0 FROM system.metrics WHERE metric = 'MergesMutationsMemoryReservation'",
            "1",
        )

        # The reservation is a sustained floor while the task is held on the failpoint - not a
        # transient blip - and the merge has not executed (both source parts are still active).
        for _ in range(5):
            assert reserved_memory() > 0
            assert active_parts() == 2
            time.sleep(1)
    finally:
        node.query("SYSTEM DISABLE FAILPOINT rmt_merge_task_pause_after_reserve")

    # Released from the failpoint, the merge runs to completion and releases its reservation.
    assert_eq_with_retry(
        node,
        "SELECT count() FROM system.parts"
        " WHERE database = 'default' AND table = 't_replicated_merge_reservation' AND active",
        "1",
    )
    assert_eq_with_retry(
        node,
        "SELECT value FROM system.metrics WHERE metric = 'MergesMutationsMemoryReservation'",
        "0",
    )
    assert (
        node.query("SELECT count() FROM t_replicated_merge_reservation").strip()
        == "20000"
    )
