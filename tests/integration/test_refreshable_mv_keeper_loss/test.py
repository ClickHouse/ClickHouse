import time
from datetime import datetime, timedelta

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager

cluster = ClickHouseCluster(__file__, zookeeper_config_path="configs/zookeeper.xml")

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/settings.xml"],
    with_zookeeper=True,
    keeper_required_feature_flags=["multi_read", "create_if_not_exists"],
    macros={"shard": 1, "replica": 1},
)

node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/settings.xml"],
    with_zookeeper=True,
    keeper_required_feature_flags=["multi_read", "create_if_not_exists"],
    macros={"shard": 1, "replica": 2},
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def view_state(table):
    """Per-replica `system.view_refreshes` snapshot, for failure messages."""
    rows = []
    for n in [node1, node2]:
        info = n.query(
            "SELECT status, exception, retry, last_refresh_replica, "
            "toInt64(last_refresh_time) AS last, toInt64(next_refresh_time) AS next, "
            "toInt64(last_success_time) AS succ "
            f"FROM system.view_refreshes WHERE view = '{table}' FORMAT Vertical"
        ).strip()
        rows.append(f"--- {n.name} ---\n{info}")
    return "\n".join(rows)


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
        f"(last: '{last}')\n{view_state(table)}"
    )


def test_keeper_session_loss_during_coordinated_refresh(started_cluster):
    """
    Regression test: when a coordinated RMV refresh is running on replica B and B
    loses its keeper session (long enough for the 'running' ephemeral to expire),
    replica A must NOT start a duplicate refresh for the same timeslot.
    """
    if node1.is_built_with_sanitizer():
        # SELECT below balloons under sanitizers → flaky timing.
        pytest.skip("Disabled for sanitizers (timing-sensitive)")

    # Replicated database is the only flavor that supports coordinated RMVs.
    node1.query("DROP DATABASE IF EXISTS re ON CLUSTER default SYNC")
    node1.query(
        "CREATE DATABASE re ON CLUSTER default "
        "ENGINE = Replicated('/clickhouse/databases/re', '{shard}', '{replica}')"
    )

    # Separate target table so we can SYSTEM SYNC REPLICA on it deterministically
    # before counting rows.
    node1.query(
        "CREATE TABLE re.tgt (t DateTime, i UInt64) "
        "ENGINE = ReplicatedMergeTree() ORDER BY tuple()"
    )

    # APPEND-mode coordinated RMV. The SELECT runs ~30s (sleepEachRow with
    # max_block_size=1), giving a wide window to kill keeper mid-refresh.
    # EVERY 1 YEAR puts the natural schedule far from any clock boundary, so
    # neither replica spontaneously refreshes during setup. EMPTY skips the
    # initial refresh at CREATE time.
    node1.query(
        """
        CREATE MATERIALIZED VIEW re.a
        REFRESH EVERY 1 YEAR APPEND
        TO re.tgt
        EMPTY
        AS SELECT now() AS t, sum(sleepEachRow(1) + 1) AS i
           FROM numbers(30) SETTINGS max_block_size = 1
        """
    )

    wait_for_status(node1, "a", "Scheduled")
    wait_for_status(node2, "a", "Scheduled")

    # Stop the view on node1 so node2 deterministically wins the race for the
    # 'running' ephemeral znode of the next refresh.
    node1.query("SYSTEM STOP VIEW re.a")

    next_refresh = node2.query(
        "SELECT next_refresh_time FROM system.view_refreshes WHERE view = 'a'"
    ).strip()
    assert next_refresh, "Expected node2 to have a scheduled next refresh"
    next_refresh_dt = datetime.strptime(next_refresh, "%Y-%m-%d %H:%M:%S")
    # node1's wake time must land in the same yearly timeslot as node2's,
    # otherwise determineNextRefreshTime would advance to next year and APPEND
    # for a different timeslot, masking the bug. 5 minutes leaves room for
    # retry backoff to grow without crossing the year boundary.
    node1_fake = (next_refresh_dt + timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S")

    # Fake clock is per-replica, so this only triggers node2's refresh.
    node2.query(f"SYSTEM TEST VIEW re.a SET FAKE TIME '{next_refresh}'")

    wait_for_status(node2, "a", "Running", timeout=30)
    wait_for_status(node1, "a", "RunningOnAnotherReplica", timeout=30)

    # Put node1 on the fake clock for node2's timeslot and START its view BEFORE
    # touching keeper. node1's 'running' ephemeral check in RefreshTask runs
    # before the stop_requested check, so node1 is already coordinating
    # (RunningOnAnotherReplica) regardless of START/STOP; what matters is that
    # node1 is now on the fake clock while node2's ephemeral is still present, so
    # it has not yet entered the missing-znode grace path.
    #
    # This ordering is the crux of the fix: node1's grace deadline is computed
    # from currentTime(), which returns the frozen fake clock once set. If node1
    # recorded running_znode_missing_since while still on the real clock (i.e. if
    # the ephemeral disappeared before this SET FAKE TIME) and we then jumped its
    # clock forward, the real-clock deadline would already be in the past and
    # node1 would clear the lock immediately, producing the duplicate this test
    # only wants to see when the coordination fix is absent. Setting the fake
    # clock first makes node1 record grace-start on the frozen clock instead.
    node1.query(f"SYSTEM TEST VIEW re.a SET FAKE TIME '{node1_fake}'")
    node1.query("SYSTEM START VIEW re.a")
    wait_for_status(node1, "a", "RunningOnAnotherReplica", timeout=30)

    # Cut only node2 off from keeper instead of restarting the whole keeper
    # cluster. Keeper stays healthy for node1, so node1's session never expires
    # and its watches keep firing. node2's session expires
    # (>session_timeout_ms=5000, see zookeeper.xml) so its 'running' ephemeral
    # disappears, but its SELECT keeps running because query execution is
    # independent of keeper -- the bug's trigger condition.
    with PartitionManager() as pm:
        pm.drop_instance_zk_connections(node2)

        # Wait until node1 actually notices the missing 'running' ephemeral and
        # enters its crash-detection grace period, then restore node2 at once.
        # Chaining the restore to this log line (instead of a fixed sleep)
        # removes the load-dependent race: node2 reconnects ~1s after node1
        # starts the grace period, always well within the ~6.25s window,
        # regardless of how loaded the runner is. With the fix node1 waits and
        # sees node2 re-register; without it node1 has no grace and APPENDs a
        # duplicate row.
        node1.wait_for_log_line("no corresponding ephemeral znode", timeout=60)
        pm.restore_instance_zk_connections(node2)

    # Wait for the dust to settle: node2's refresh completes (~30s SELECT +
    # post-keeper INSERT) and propagates to keeper, then both replicas idle.
    wait_for_status(node1, "a", "Scheduled", timeout=120)
    wait_for_status(node2, "a", "Scheduled", timeout=120)

    # Pull both replicas' parts before counting -- otherwise the count races
    # against ReplicatedMergeTree replication. Retry: a replica may still be
    # transiently read-only right after re-establishing its keeper session.
    node1.query_with_retry("SYSTEM SYNC REPLICA re.tgt")
    node2.query_with_retry("SYSTEM SYNC REPLICA re.tgt")

    rows = node1.query("SELECT toInt64(t), i, _part FROM re.tgt ORDER BY t")
    final_count = int(node1.query("SELECT count() FROM re.tgt").strip())

    assert final_count == 1, (
        f"Expected 1 row APPENDed for the timeslot, got {final_count}. "
        f"Rows:\n{rows}"
    )
