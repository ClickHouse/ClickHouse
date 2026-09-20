import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml"],
    with_zookeeper=True,
    keeper_required_feature_flags=["multi_read", "create_if_not_exists"],
    macros={"shard": 1, "replica": 1},
)

node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/remote_servers.xml"],
    with_zookeeper=True,
    keeper_required_feature_flags=["multi_read", "create_if_not_exists"],
    macros={"shard": 1, "replica": 2},
)

FAILPOINT = "keeper_fault_on_watch_request"


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def wait_for_status(node, table, expected, timeout=60):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = node.query(
            f"SELECT status FROM system.view_refreshes WHERE view = '{table}'"
        ).strip()
        if last == expected:
            return
        time.sleep(0.2)
    raise AssertionError(
        f"{node.name}: view '{table}' did not reach status '{expected}' within {timeout}s "
        f"(last: '{last}')"
    )


def test_watch_registration_failure_recovers(started_cluster):
    """
    A coordinated RMV relies on keeper watches to wake up when another replica changes the
    shared state - the `RunningOnAnotherReplica` state schedules no timer. The bug: the
    "watch active" flag in `RefreshTask::readZnodesIfNeeded` was set before the watch request
    was sent and not reset on failure, so after one failed request the watch was never re-added
    and the replica stayed blind.

    Repro: break node1's watch requests via the failpoint, start a refresh on node2 (which fires
    node1's watches and makes it try - and fail - to re-add them), then disable the failpoint and
    check node1 still notices when node2 finishes. With the fix node1 re-adds its watches and
    returns to Scheduled; with the bug it stays stuck in RunningOnAnotherReplica.
    """
    # Replicated database is the only flavor that supports coordinated RMVs.
    node1.query("DROP DATABASE IF EXISTS re ON CLUSTER default SYNC")
    node1.query(
        "CREATE DATABASE re ON CLUSTER default "
        "ENGINE = Replicated('/clickhouse/databases/re', '{shard}', '{replica}')"
    )
    node1.query(
        "CREATE TABLE re.tgt (t DateTime, i UInt64) "
        "ENGINE = ReplicatedMergeTree() ORDER BY tuple()"
    )

    # EVERY 1 YEAR so neither replica refreshes on its own (no periodic wakeup to mask the missing
    # watch); EMPTY skips the initial refresh; the ~60s SELECT keeps node2's refresh running long
    # enough for the failpoint dance to finish before node2 completes.
    node1.query(
        """
        CREATE MATERIALIZED VIEW re.a
        REFRESH EVERY 1 YEAR APPEND
        TO re.tgt
        EMPTY
        AS SELECT now() AS t, sum(sleepEachRow(1) + 1) AS i
           FROM numbers(60) SETTINGS max_block_size = 1
        """
    )

    # Both replicas idle in Scheduled, each with its watches registered (failpoint off).
    wait_for_status(node1, "a", "Scheduled")
    wait_for_status(node2, "a", "Scheduled")

    # Break node1's watch requests. Process-local and only affects watch-carrying reads, so node1's
    # session stays healthy. No DDL is issued on node1 while it is enabled.
    # Anchor the log search at the current line so it ignores any matches from a previous run sharing
    # this module-scoped cluster (wait_for_log_line's default scans backwards over old lines).
    log_anchor = node1.count_log_lines()
    node1.query(f"SYSTEM ENABLE FAILPOINT {FAILPOINT}")

    # node2 starts a refresh and creates the 'running' ephemeral child. That fires node1's existing
    # watches, so node1 wakes and tries to re-add them - which now fails.
    node2.query("SYSTEM REFRESH VIEW re.a")
    wait_for_status(node2, "a", "Running", timeout=30)

    # Wait for two watch-add failures: the exists-watch on the first scheduling iteration and the
    # children-watch on the second (5s retry apart). Both flags would be left stuck by the bug.
    node1.wait_for_log_line(
        "RefreshTask.re.a.*Keeper error",
        timeout=60,
        repetitions=2,
        look_behind_lines=f"+{log_anchor}",
    )

    # Stop injecting faults. With the fix node1 re-adds both watches; with the bug the flags are
    # stuck and it never tries again.
    node1.query(f"SYSTEM DISABLE FAILPOINT {FAILPOINT}")

    # node1 has read the state and seen node2's refresh. Both bug and fix reach this; the difference
    # is whether node1 holds a working watch here.
    wait_for_status(node1, "a", "RunningOnAnotherReplica", timeout=60)

    # node2 finishes and clears the 'running' znode - the change node1 must be woken by.
    wait_for_status(node2, "a", "Scheduled", timeout=120)

    # The crux: node1 must notice node2 finished, via its watch, and return to Scheduled. With the
    # bug it has no watch and stays in RunningOnAnotherReplica until this times out.
    wait_for_status(node1, "a", "Scheduled", timeout=60)
