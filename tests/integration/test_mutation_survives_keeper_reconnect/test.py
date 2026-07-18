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


def test_survivor_result_discarded_when_target_part_dropped(started_cluster):
    """
    A mutation that detaches to survive a transient reconnect must not orphan its pre-computed
    result if the target part's queue entry is meanwhile removed by a concurrent DROP PARTITION.
    While the survivor is still computing, executing the DROP_RANGE removes its queue entry, so the
    in-progress computation is aborted (and, in the race where it had already deposited, its result
    is discarded). Either way the result must never be reused, and the table must stay consistent and
    usable afterwards.
    """
    node.query("DROP TABLE IF EXISTS t2 SYNC")
    node.query(
        """
        CREATE TABLE t2 (p UInt64, k UInt64, v UInt64)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/t2', 'r1')
        PARTITION BY p
        ORDER BY k
        SETTINGS reuse_precomputed_mutations_after_keeper_reconnect = 1,
                 zookeeper_session_expiration_check_period = 1,
                 index_granularity = 1024
        """
    )

    # A single part in partition 0 with several granules so the mutation read loop runs more than once.
    node.query("INSERT INTO t2 SELECT 0, number, number FROM numbers(100000)")
    assert node.query("SELECT count() FROM system.parts WHERE table = 't2' AND active").strip() == "1"

    survived_before = get_event("MutationsSurvivedKeeperReconnect")
    reused_before = get_event("MutationsReusedPrecomputedParts")
    partial_shutdown_before = get_event("ReplicaPartialShutdown")

    # Pause the mutation right after it has produced its first block of the new part.
    node.query(f"SYSTEM ENABLE FAILPOINT {FAILPOINT}")
    node.query("ALTER TABLE t2 UPDATE v = v + 1 WHERE 1 SETTINGS mutations_sync = 0")
    wait_for(
        lambda: node.query(
            "SELECT count() FROM system.merges WHERE table = 't2' AND is_mutation"
        ).strip()
        != "0",
        "mutation to start and pause mid-computation",
    )

    # Expire the ZooKeeper session so the paused mutation detaches to survive the reconnect.
    node.query("SYSTEM RECONNECT ZOOKEEPER")
    wait_for(
        lambda: get_event("ReplicaPartialShutdown") > partial_shutdown_before,
        "the transient partial shutdown to happen",
    )
    wait_for(
        lambda: node.query("SELECT is_readonly FROM system.replicas WHERE table = 't2'").strip() == "0",
        "the replica to recover from readonly",
    )

    # The (still-paused) mutation detached to survive the reconnect instead of being cancelled.
    wait_for(
        lambda: get_event("MutationsSurvivedKeeperReconnect") > survived_before,
        "the mutation to survive the reconnect",
    )

    # Drop the partition that contains the part the (still-paused) survivor is computing. Executing
    # the resulting DROP_RANGE removes the mutation's queue entry, so its result can never be
    # committed.
    node.query("ALTER TABLE t2 DROP PARTITION 0")
    wait_for(
        lambda: node.query("SELECT count() FROM t2").strip() == "0",
        "the partition to be dropped (DROP_RANGE executed while the survivor is still computing)",
    )

    # Resume the survivor. Its computation is aborted now that the queue entry is gone (in the race
    # where it had already deposited, the deposited result is discarded instead).
    node.query(f"SYSTEM DISABLE FAILPOINT {FAILPOINT}")

    # Wait for the resumed survivor task to finish tearing down, so that if a stale result were
    # (incorrectly) going to be reused, it would have happened by now.
    wait_for(
        lambda: node.query(
            "SELECT count() FROM system.merges WHERE table = 't2' AND is_mutation"
        ).strip()
        == "0",
        "the aborted survivor task to finish",
    )

    # The result must NOT have been reused: the partition (and the target part) was dropped.
    assert get_event("MutationsReusedPrecomputedParts") == reused_before

    # The partition is gone and the table is still consistent and usable.
    assert node.query("SELECT count() FROM t2").strip() == "0"
    node.query("INSERT INTO t2 SELECT 1, number, number FROM numbers(10)")
    node.query("ALTER TABLE t2 UPDATE v = v + 1 WHERE 1 SETTINGS mutations_sync = 2")
    assert node.query("SELECT count() FROM t2 WHERE v = k + 1").strip() == "10"

    # And the table drops cleanly (no orphaned temporary part keeping directories alive).
    node.query("DROP TABLE t2 SYNC")
