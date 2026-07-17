#!/usr/bin/env python3
"""
Checks that an in-progress part mutation survives a transient ZooKeeper (Keeper)
session re-establishment instead of being cancelled and re-computed from scratch,
when `reuse_precomputed_mutations_after_keeper_reconnect` is enabled.

The mutation is paused mid-computation with a fail point, the ZooKeeper session is
then expired via `SYSTEM RECONNECT ZOOKEEPER`, and after the mutation is resumed we
check that its pre-computed result was kept and reused (via ProfileEvents) and that
the data is correct.
"""

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", with_zookeeper=True, stay_alive=True)

FAILPOINT = "mutate_task_pause_after_first_block"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_event(event):
    value = node.query(
        f"SELECT value FROM system.events WHERE event = '{event}'"
    ).strip()
    return int(value) if value else 0


def wait_for(condition, description, attempts=120, sleep=0.5):
    import time

    for _ in range(attempts):
        if condition():
            return
        time.sleep(sleep)
    raise AssertionError(f"Timed out waiting for: {description}")


def test_mutation_survives_transient_keeper_reconnect(started_cluster):
    node.query("DROP TABLE IF EXISTS t SYNC")
    node.query(
        """
        CREATE TABLE t (k UInt64, v UInt64)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/t', 'r1')
        ORDER BY k
        SETTINGS reuse_precomputed_mutations_after_keeper_reconnect = 1,
                 zookeeper_session_expiration_check_period = 1,
                 index_granularity = 1024
        """
    )

    # A single part with several granules so the mutation read loop runs more than once.
    node.query("INSERT INTO t SELECT number, number FROM numbers(100000)")
    assert node.query("SELECT count() FROM system.parts WHERE table = 't' AND active").strip() == "1"

    survived_before = get_event("MutationsSurvivedKeeperReconnect")
    reused_before = get_event("MutationsReusedPrecomputedParts")
    partial_shutdown_before = get_event("ReplicaPartialShutdown")

    # Pause the mutation right after it has produced its first block of the new part.
    node.query(f"SYSTEM ENABLE FAILPOINT {FAILPOINT}")

    # Start the mutation asynchronously; it will block inside the fail point.
    node.query("ALTER TABLE t UPDATE v = v + 1 WHERE 1 SETTINGS mutations_sync = 0")

    # Wait until the mutation is actually running (present in system.merges).
    wait_for(
        lambda: node.query(
            "SELECT count() FROM system.merges WHERE table = 't' AND is_mutation"
        ).strip()
        != "0",
        "mutation to start and pause mid-computation",
    )

    # Expire the ZooKeeper session. The restarting thread wakes up immediately, does a
    # transient partial shutdown, and the paused mutation detaches to survive it.
    node.query("SYSTEM RECONNECT ZOOKEEPER")

    # Wait until the partial shutdown happened and the replica recovered from readonly.
    wait_for(
        lambda: get_event("ReplicaPartialShutdown") > partial_shutdown_before,
        "the transient partial shutdown to happen",
    )
    wait_for(
        lambda: node.query("SELECT is_readonly FROM system.replicas WHERE table = 't'").strip() == "0",
        "the replica to recover from readonly",
    )

    # Resume the mutation. It now finishes its computation and deposits the result,
    # then a follow-up attempt re-validates and commits it without re-computing.
    node.query(f"SYSTEM DISABLE FAILPOINT {FAILPOINT}")

    # The mutation should complete.
    wait_for(
        lambda: node.query(
            "SELECT is_done FROM system.mutations WHERE table = 't' ORDER BY mutation_id DESC LIMIT 1"
        ).strip()
        == "1",
        "the mutation to complete",
    )

    # The pre-computed result must have been kept across the reconnect and reused.
    assert get_event("MutationsSurvivedKeeperReconnect") > survived_before
    assert get_event("MutationsReusedPrecomputedParts") > reused_before

    # And the data must be correct: v == k + 1 for all rows.
    assert node.query("SELECT count() FROM t WHERE v != k + 1").strip() == "0"
    assert node.query("SELECT count() FROM t").strip() == "100000"

    node.query("DROP TABLE t SYNC")
