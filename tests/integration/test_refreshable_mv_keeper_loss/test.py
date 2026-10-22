import time
from datetime import datetime, timedelta

import pytest

from helpers.cluster import ClickHouseCluster

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

    # Restart all keeper nodes. With session_timeout_ms=5000 (see zookeeper.xml)
    # and >5s of total quorum-loss below, every client session expires, so
    # node2's 'running' ephemeral disappears -- but node2's SELECT keeps
    # running because query execution is independent of keeper. This is the
    # bug's trigger condition and mirrors the user's manual repro.
    started_cluster.stop_zookeeper_nodes(["zoo1", "zoo2", "zoo3"])
    started_cluster.start_zookeeper_nodes(["zoo1", "zoo2", "zoo3"])
    started_cluster.wait_zookeeper_nodes_to_start(["zoo1", "zoo2", "zoo3"], timeout=60)

    # Make node1 want to refresh for the same yearly timeslot. With the fix,
    # node1 sees `refresh_running=true` in the root znode and waits for node2
    # to finish; without the fix, node1 races and APPENDs a duplicate row.
    node1.query(f"SYSTEM TEST VIEW re.a SET FAKE TIME '{node1_fake}'")
    node1.query("SYSTEM START VIEW re.a")

    # Wait for the dust to settle: node2's refresh completes (~30s SELECT +
    # post-keeper INSERT) and propagates to keeper, then both replicas idle.
    wait_for_status(node1, "a", "Scheduled", timeout=120)
    wait_for_status(node2, "a", "Scheduled", timeout=120)

    # Pull both replicas' parts before counting -- otherwise the count races
    # against ReplicatedMergeTree replication.
    node1.query("SYSTEM SYNC REPLICA re.tgt")
    node2.query("SYSTEM SYNC REPLICA re.tgt")

    rows = node1.query("SELECT toInt64(t), i, _part FROM re.tgt ORDER BY t")
    final_count = int(node1.query("SELECT count() FROM re.tgt").strip())

    assert final_count == 1, (
        f"Expected 1 row APPENDed for the timeslot, got {final_count}. "
        f"Rows:\n{rows}"
    )
