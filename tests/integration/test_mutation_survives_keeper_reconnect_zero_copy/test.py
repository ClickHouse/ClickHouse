#!/usr/bin/env python3
"""
Zero-copy variant of test_mutation_survives_keeper_reconnect.

On a zero-copy-replicated table an in-progress mutation holds an exclusive zero-copy
lock while it computes. When such a mutation survives a transient ZooKeeper reconnect
and deposits its result, it must release that lock before the follow-up attempt is
scheduled; otherwise the follow-up fails to acquire the lock (against the survivor's
own still-held one) and drops the pre-computed result instead of reusing it.

This checks that, with `reuse_precomputed_mutations_after_keeper_reconnect` enabled,
the pre-computed result is kept across the reconnect and reused (via ProfileEvents)
even on a zero-copy disk, and that the data is correct.
"""

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/storage_conf.xml"],
    with_zookeeper=True,
    with_minio=True,
    stay_alive=True,
)

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


def test_mutation_survives_transient_keeper_reconnect_zero_copy(started_cluster):
    node.query("DROP TABLE IF EXISTS t SYNC")
    node.query(
        """
        CREATE TABLE t (k UInt64, v UInt64)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/t_zero_copy', 'r1')
        ORDER BY k
        SETTINGS storage_policy = 's3',
                 allow_remote_fs_zero_copy_replication = 1,
                 reuse_precomputed_mutations_after_keeper_reconnect = 1,
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

    # Pause the mutation right after it has produced its first block of the new part. By then
    # prepare() has already acquired the exclusive zero-copy lock.
    node.query(f"SYSTEM ENABLE FAILPOINT {FAILPOINT}")
    node.query("ALTER TABLE t UPDATE v = v + 1 WHERE 1 SETTINGS mutations_sync = 0")
    wait_for(
        lambda: node.query(
            "SELECT count() FROM system.merges WHERE table = 't' AND is_mutation"
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
        lambda: node.query("SELECT is_readonly FROM system.replicas WHERE table = 't'").strip() == "0",
        "the replica to recover from readonly",
    )

    # Resume the mutation. It finishes computing, releases its zero-copy lock, and deposits the
    # result; a follow-up attempt then re-acquires a fresh lock and reuses the result.
    node.query(f"SYSTEM DISABLE FAILPOINT {FAILPOINT}")
    wait_for(
        lambda: node.query(
            "SELECT is_done FROM system.mutations WHERE table = 't' ORDER BY mutation_id DESC LIMIT 1"
        ).strip()
        == "1",
        "the mutation to complete",
    )

    # The pre-computed result must have been kept across the reconnect and reused on the zero-copy disk.
    assert get_event("MutationsSurvivedKeeperReconnect") > survived_before
    assert get_event("MutationsReusedPrecomputedParts") > reused_before

    # And the data must be correct: v == k + 1 for all rows.
    assert node.query("SELECT count() FROM t WHERE v != k + 1").strip() == "0"
    assert node.query("SELECT count() FROM t").strip() == "100000"

    node.query("DROP TABLE t SYNC")
