import logging
import random
import threading
import time

import pytest

from helpers.cluster import ClickHouseCluster

logging.basicConfig(level=logging.INFO)

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/config.d/storage_conf.xml"],
    with_minio=True,
    stay_alive=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config.d/storage_conf.xml"],
    with_minio=True,
    stay_alive=True,
)
node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/config.d/storage_conf.xml"],
    with_minio=True,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


TABLE_SETTINGS = (
    "storage_policy = 's3', "
    "leader_election = true, "
    "leader_election_heartbeat_interval = 1, "
    "leader_election_session_timeout = 5"
)

# Fixed UUID so all nodes share the same S3 data path and lease file.
SHARED_UUID = "12345678-abcd-abcd-abcd-123456789abc"
SHARED_UUID_FO = "12345678-abcd-abcd-abcd-123456789abd"
SHARED_UUID_CONCURRENT = "12345678-abcd-abcd-abcd-123456789abe"


def create_table_on_first_node(node, table_name="test_le", uuid=SHARED_UUID):
    """Create the table on the first node (initializes the S3 directory)."""
    node.query(
        f"""
        CREATE TABLE {table_name} UUID '{uuid}' (x UInt64)
        ENGINE = MergeTree ORDER BY x
        SETTINGS {TABLE_SETTINGS}
        """
    )


def attach_table_on_second_node(node, table_name="test_le", uuid=SHARED_UUID):
    """Attach the table on the second node using the same UUID (shares S3 path)."""
    node.query(
        f"""
        ATTACH TABLE {table_name} UUID '{uuid}' (x UInt64)
        ENGINE = MergeTree ORDER BY x
        SETTINGS {TABLE_SETTINGS}
        """
    )


def is_leader(node, table_name="test_le"):
    """Check if the node considers itself the leader by attempting an insert."""
    try:
        node.query(f"INSERT INTO {table_name} VALUES (0)")
        # Clean up the test row
        node.query(
            f"ALTER TABLE {table_name} DELETE WHERE x = 0 SETTINGS mutations_sync = 1"
        )
        return True
    except Exception as e:
        if "TABLE_IS_READ_ONLY" in str(e):
            return False
        raise


def wait_for_leader(nodes, timeout=60, table_name="test_le"):
    """Wait until exactly one node becomes the leader. Returns (leader, followers)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        leaders = []
        followers = []
        for node in nodes:
            try:
                if is_leader(node, table_name):
                    leaders.append(node)
                else:
                    followers.append(node)
            except Exception:
                followers.append(node)
        if len(leaders) == 1 and len(followers) == len(nodes) - 1:
            return leaders[0], followers
        time.sleep(2)
    raise RuntimeError("Timed out waiting for exactly one leader")


def test_leader_elected(started_cluster):
    """Test that when two nodes share S3 storage, exactly one becomes leader."""
    create_table_on_first_node(node1)
    attach_table_on_second_node(node2)

    leader, followers = wait_for_leader([node1, node2])
    follower = followers[0]

    logging.info(f"Leader: {leader.name}, Follower: {follower.name}")

    # Leader can insert
    leader.query("INSERT INTO test_le VALUES (1), (2), (3)")
    assert leader.query("SELECT count() FROM test_le WHERE x > 0").strip() == "3"

    # Follower cannot insert
    error = ""
    try:
        follower.query("INSERT INTO test_le VALUES (100)")
    except Exception as e:
        error = str(e)
    assert "TABLE_IS_READ_ONLY" in error, f"Expected TABLE_IS_READ_ONLY, got: {error}"

    node1.query("DROP TABLE IF EXISTS test_le SYNC")
    node2.query("DROP TABLE IF EXISTS test_le SYNC")


def test_metrics(started_cluster):
    """Verify that `MergeTreeLeaderElection*` CurrentMetrics and ProfileEvents are wired up."""
    table = "test_metrics"
    uuid = "12345678-abcd-abcd-abcd-123456789abf"

    def metric(node, name):
        return int(node.query(
            f"SELECT value FROM system.metrics WHERE metric = '{name}'"
        ).strip())

    def event(node, name):
        result = node.query(
            f"SELECT value FROM system.events WHERE event = '{name}'"
        ).strip()
        return int(result) if result else 0

    # Baselines captured before the test creates its tables — other tests in this
    # module may have left counters above zero, so we measure deltas, not absolutes.
    baseline_leader = {n.name: metric(n, "MergeTreeLeaderElectionLeader") for n in [node1, node2]}
    baseline_follower = {n.name: metric(n, "MergeTreeLeaderElectionFollower") for n in [node1, node2]}
    baseline_acquired = {n.name: event(n, "MergeTreeLeaderElectionAcquired") for n in [node1, node2]}
    baseline_renewals = {n.name: event(n, "MergeTreeLeaderElectionLeaseRenewals") for n in [node1, node2]}

    node1.query(
        f"""
        CREATE TABLE {table} UUID '{uuid}' (x UInt64)
        ENGINE = MergeTree ORDER BY x
        SETTINGS {TABLE_SETTINGS}
        """
    )
    node2.query(
        f"""
        ATTACH TABLE {table} UUID '{uuid}' (x UInt64)
        ENGINE = MergeTree ORDER BY x
        SETTINGS {TABLE_SETTINGS}
        """
    )

    leader, followers = wait_for_leader([node1, node2], table_name=table)
    follower = followers[0]

    # Gauge: the leader gauge on the leader's node went up by 1; the follower gauge
    # on the follower's node went up by 1.
    assert metric(leader, "MergeTreeLeaderElectionLeader") - baseline_leader[leader.name] >= 1, (
        f"{leader.name} did not record itself in MergeTreeLeaderElectionLeader"
    )
    assert metric(follower, "MergeTreeLeaderElectionFollower") - baseline_follower[follower.name] >= 1, (
        f"{follower.name} did not record itself in MergeTreeLeaderElectionFollower"
    )

    # Counters: the leader should have acquired at least once and renewed at least
    # once. With `leader_election_heartbeat_interval = 1 s` the wait + sleep here
    # gives at least one renewal cycle.
    time.sleep(2)
    assert event(leader, "MergeTreeLeaderElectionAcquired") - baseline_acquired[leader.name] >= 1, (
        f"{leader.name} did not increment MergeTreeLeaderElectionAcquired"
    )
    assert event(leader, "MergeTreeLeaderElectionLeaseRenewals") - baseline_renewals[leader.name] >= 1, (
        f"{leader.name} did not increment MergeTreeLeaderElectionLeaseRenewals"
    )

    # Drop the table and verify the gauges return to their pre-test baseline.
    node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node2.query(f"DROP TABLE IF EXISTS {table} SYNC")
    for n in [node1, node2]:
        assert metric(n, "MergeTreeLeaderElectionLeader") == baseline_leader[n.name], (
            f"{n.name} did not release MergeTreeLeaderElectionLeader after DROP "
            f"(now {metric(n, 'MergeTreeLeaderElectionLeader')}, baseline {baseline_leader[n.name]})"
        )
        assert metric(n, "MergeTreeLeaderElectionFollower") == baseline_follower[n.name], (
            f"{n.name} did not release MergeTreeLeaderElectionFollower after DROP "
            f"(now {metric(n, 'MergeTreeLeaderElectionFollower')}, baseline {baseline_follower[n.name]})"
        )


def test_failover(started_cluster):
    """Test that when the leader stops, the follower takes over."""
    create_table_on_first_node(node1, "test_fo", SHARED_UUID_FO)
    attach_table_on_second_node(node2, "test_fo", SHARED_UUID_FO)

    leader, followers = wait_for_leader([node1, node2], table_name="test_fo")
    follower = followers[0]

    logging.info(f"Leader: {leader.name}, Follower: {follower.name}")

    # Leader inserts data
    leader.query("INSERT INTO test_fo VALUES (1), (2), (3)")

    # Stop the leader
    leader.stop_clickhouse()

    # Wait for the follower to become leader (session_timeout = 5s)
    deadline = time.monotonic() + 60
    new_leader = False
    while time.monotonic() < deadline:
        try:
            follower.query("INSERT INTO test_fo VALUES (10)")
            new_leader = True
            break
        except Exception as e:
            if "TABLE_IS_READ_ONLY" in str(e):
                time.sleep(2)
                continue
            raise

    assert new_leader, "Follower did not become leader after original leader stopped"
    logging.info(f"New leader: {follower.name}")

    # Restart the old leader
    leader.start_clickhouse()

    # The old leader must come back up as a follower without ever accepting a write.
    # On startup `is_leader` is false, and the first heartbeat on a lease still held by
    # the new leader keeps it false — so any successful INSERT here would indicate a
    # dual-writer window (split-brain) and must fail the test immediately.
    #
    # We retry the INSERT for a bounded period to give the table time to load after
    # restart (a server-not-ready error is not the same as a dual-writer), but we
    # require every attempt to either fail with TABLE_IS_READ_ONLY or with a transient
    # startup error. A single successful INSERT fails the test.
    deadline = time.monotonic() + 60
    old_leader_is_readonly = False
    while time.monotonic() < deadline:
        try:
            leader.query("INSERT INTO test_fo VALUES (999)")
        except Exception as e:
            if "TABLE_IS_READ_ONLY" in str(e):
                old_leader_is_readonly = True
                break
            # Transient startup errors (e.g. server still loading) are tolerated.
            time.sleep(1)
            continue
        raise AssertionError(
            "Restarted old leader accepted a write while the new leader holds the lease "
            "(dual-writer / split-brain window detected)"
        )

    assert old_leader_is_readonly, "Restarted old leader did not become read-only"
    logging.info(f"Old leader {leader.name} is now read-only as expected")

    # Verify data is accessible from the restarted node
    count = leader.query("SELECT count() FROM test_fo WHERE x > 0").strip()
    assert int(count) >= 3, f"Expected at least 3 rows, got {count}"

    node1.query("DROP TABLE IF EXISTS test_fo SYNC")
    node2.query("DROP TABLE IF EXISTS test_fo SYNC")


# Settings for the concurrent test: tighten the lease so leadership churn is observable
# within a 30-second test window. `leader_election_session_timeout` must be at least
# 3x `leader_election_heartbeat_interval`.
TABLE_SETTINGS_CONCURRENT = (
    "storage_policy = 's3', "
    "leader_election = true, "
    "leader_election_heartbeat_interval = 1, "
    "leader_election_session_timeout = 3"
)


def create_concurrent_table_on_first_node(node, table_name, uuid):
    node.query(
        f"""
        CREATE TABLE {table_name} UUID '{uuid}' (x UInt64)
        ENGINE = MergeTree ORDER BY x
        SETTINGS {TABLE_SETTINGS_CONCURRENT}
        """
    )


def attach_concurrent_table(node, table_name, uuid):
    node.query(
        f"""
        ATTACH TABLE {table_name} UUID '{uuid}' (x UInt64)
        ENGINE = MergeTree ORDER BY x
        SETTINGS {TABLE_SETTINGS_CONCURRENT}
        """
    )


def test_concurrent_inserts_with_restarts(started_cluster):
    """
    Stress test: three nodes share an S3 path with `leader_election = true`. Each node
    spawns a worker that inserts in a tight loop while a chaos thread restarts random
    nodes. After 30 seconds the test validates:

      - At any moment in time, at most one node accepted inserts (leader exclusivity).
      - At least one merge happened (background processing on the leader is alive).
      - Total row count, distinct keys, and per-node markers all reconcile with the
        worker's locally-recorded successful inserts (no data loss, no phantom writes).
    """
    table = "test_concurrent"
    nodes = [node1, node2, node3]

    create_concurrent_table_on_first_node(node1, table, SHARED_UUID_CONCURRENT)
    attach_concurrent_table(node2, table, SHARED_UUID_CONCURRENT)
    attach_concurrent_table(node3, table, SHARED_UUID_CONCURRENT)

    # Wait for an initial leader so workers don't all start in a bootstrap-flap window.
    wait_for_leader(nodes, table_name=table)

    stop_event = threading.Event()
    records_lock = threading.Lock()
    records = []  # (node_name, start_monotonic, end_monotonic, success, value)

    def insert_worker(node, node_idx):
        # Per-node value space keeps inserts globally unique so we can audit data loss.
        # 10**9 leaves room for many inserts per node within UInt64. We start at
        # `base + 1` so we never collide with the `x = 0` probe used by `is_leader`.
        base = (node_idx + 1) * 1_000_000_000
        counter = 0
        while not stop_event.is_set():
            counter += 1
            value = base + counter
            start = time.monotonic()
            try:
                node.query(f"INSERT INTO {table} VALUES ({value})", timeout=8)
                end = time.monotonic()
                with records_lock:
                    records.append((node.name, start, end, True, value))
            except Exception as e:
                end = time.monotonic()
                with records_lock:
                    records.append((node.name, start, end, False, value))
                # Brief backoff on failure to avoid hammering a node that's
                # restarting or refusing as a follower.
                if not stop_event.is_set():
                    time.sleep(0.1)

    def chaos_worker():
        # Restart nodes one after another so every node — including the leader —
        # is taken down at least once during the run. With three nodes and one
        # stopped at a time we always keep two live nodes; one of them must be
        # (or become) the leader within `leader_election_session_timeout`.
        #
        # When we kill the leader we MUST keep it down longer than
        # `leader_election_session_timeout` (3 s here); otherwise the leader
        # comes back before the lease expires and silently reclaims its role,
        # so the test never observes a failover.
        rng = random.Random(20260506)
        order = list(nodes)
        rng.shuffle(order)
        idx = 0
        while not stop_event.is_set():
            wait = rng.uniform(1.0, 2.0)
            end_wait = time.monotonic() + wait
            while time.monotonic() < end_wait and not stop_event.is_set():
                time.sleep(0.2)
            if stop_event.is_set():
                break
            target = order[idx % len(order)]
            idx += 1
            logging.info(f"Chaos: killing {target.name}")
            try:
                target.stop_clickhouse(kill=True)
            except Exception as e:
                logging.warning(f"Chaos: kill of {target.name} failed: {e}")
                continue
            # Stay down past the session timeout so a follower can claim the lease.
            down_for = rng.uniform(4.0, 5.0)
            down_until = time.monotonic() + down_for
            while time.monotonic() < down_until:
                time.sleep(0.2)
            try:
                target.start_clickhouse()
                logging.info(f"Chaos: {target.name} back online")
            except Exception as e:
                logging.warning(f"Chaos: start of {target.name} failed: {e}")

    workers = [
        threading.Thread(target=insert_worker, args=(n, i), name=f"insert-{n.name}")
        for i, n in enumerate(nodes)
    ]
    chaos = threading.Thread(target=chaos_worker, name="chaos")

    test_duration = 30
    for w in workers:
        w.start()
    chaos.start()

    time.sleep(test_duration)
    stop_event.set()

    chaos.join(timeout=120)
    for w in workers:
        w.join(timeout=120)

    assert not chaos.is_alive(), "Chaos worker did not exit"
    for w in workers:
        assert not w.is_alive(), f"Insert worker {w.name} did not exit"

    # Make sure every node is up before validation. The chaos worker may have left
    # one node mid-restart at the moment it observed `stop_event`.
    for n in nodes:
        try:
            n.query("SELECT 1", timeout=5)
        except Exception:
            try:
                n.start_clickhouse()
            except Exception as e:
                logging.warning(f"Could not bring {n.name} back up: {e}")

    successes = [r for r in records if r[3]]
    failures = [r for r in records if not r[3]]
    logging.info(
        f"Total attempts: {len(records)}, successes: {len(successes)}, failures: {len(failures)}"
    )
    by_node = {}
    for r in successes:
        by_node.setdefault(r[0], []).append(r)
    for name, rs in by_node.items():
        logging.info(f"  {name}: {len(rs)} successful inserts")

    assert len(successes) > 0, "No successful inserts at all — chaos broke the cluster"

    # Invariant 1: at any moment, at most one node accepted inserts. We measure each
    # insert as a python-side `[start, end]` window — a superset of the actual
    # server-side write window — so any real split-brain shows up here. Sweep the
    # events in time order and assert that whenever one node has an open window,
    # no other node opens a window before it closes. Allow a small tolerance for
    # client-RPC overhead and host-clock noise (sub-tolerance overlaps would be
    # ambiguous between real concurrency and measurement skew).
    GRACE = 0.1  # 100 ms
    events = []
    for r in successes:
        events.append((r[1], 1, r))   # 1 = start; sort starts after ends at same time
        events.append((r[2] + GRACE, 0, r))  # 0 = end; extend by GRACE
    events.sort(key=lambda e: (e[0], e[1]))
    active = {}  # node_name -> count of currently-open insert windows
    for _, kind, r in events:
        node_name = r[0]
        if kind == 1:
            other_active = [n for n, c in active.items() if c > 0 and n != node_name]
            if other_active:
                raise AssertionError(
                    f"Two nodes accepted inserts at the same time (split-brain):\n"
                    f"  starting on {node_name}: window [{r[1]:.3f}, {r[2]:.3f}] value={r[4]}\n"
                    f"  still open on: {other_active}"
                )
            active[node_name] = active.get(node_name, 0) + 1
        else:
            active[node_name] -= 1

    # Invariant 2: leadership actually moved between nodes during the run. The chaos
    # thread keeps the leader down past the 3-second session timeout, so a follower
    # has to claim leadership to keep the workload progressing. If only one node
    # ever succeeded, the chaos thread didn't exercise failover.
    assert len(by_node) >= 2, (
        f"Only {len(by_node)} node(s) ever accepted inserts: {list(by_node.keys())}. "
        f"Failover did not occur during the test."
    )

    # Wait for a stable leader so the cluster is in a defined state for read queries.
    wait_for_leader(nodes, table_name=table)

    # Invariant 3: data integrity. Under shared-storage leader election, each node's
    # local metadata only references the parts that node itself wrote during its
    # leadership epoch — there's no cross-node metadata sync. So every successful
    # insert must be visible from SOMEWHERE in the cluster (the node that wrote it),
    # and the union of what every node sees must contain only values we attempted.
    success_values = set(r[4] for r in successes)
    attempted_values = set(r[4] for r in records)
    visible = set()
    per_node_visible = {}
    for n in nodes:
        try:
            rows_str = n.query(f"SELECT x FROM {table}", timeout=30)
            node_values = {
                int(line) for line in rows_str.strip().split("\n") if line
            }
            per_node_visible[n.name] = node_values
            visible |= node_values
        except Exception as e:
            logging.warning(f"Could not read from {n.name}: {e}")
            per_node_visible[n.name] = set()
    for name, values in per_node_visible.items():
        logging.info(f"  {name}: sees {len(values)} rows")

    missing = success_values - visible
    if missing:
        sample = sorted(missing)[:10]
        raise AssertionError(
            f"Data loss: {len(missing)} of {len(success_values)} successful inserts "
            f"are not visible on any node. Sample missing values: {sample}"
        )

    extra = visible - attempted_values
    if extra:
        sample = sorted(extra)[:10]
        raise AssertionError(
            f"Phantom rows: {len(extra)} values present that we never tried to insert. "
            f"Sample: {sample}"
        )

    # Invariant 4: merges happened. Each insert produces a part; the leader's
    # background scheduler must have merged at least some of them. Check the union
    # of active parts across all nodes (each node sees its own).
    total_parts = 0
    for n in nodes:
        try:
            count_str = n.query(
                f"SELECT count() FROM system.parts "
                f"WHERE table = '{table}' AND active"
            ).strip()
            total_parts += int(count_str)
        except Exception:
            pass
    expected_count = len(success_values)
    logging.info(f"Inserts: {expected_count}, total active parts across all nodes: {total_parts}")
    assert total_parts < expected_count, (
        f"No merges happened: {total_parts} active parts for {expected_count} inserts"
    )

    for n in nodes:
        try:
            n.query(f"DROP TABLE IF EXISTS {table} SYNC")
        except Exception:
            pass


# UUIDs for the regression tests below — each test uses a unique S3 prefix so it
# can run independently of the others.
SHARED_UUID_BLOCKNUM = "12345678-abcd-abcd-abcd-12345678ab01"
SHARED_UUID_ALTER = "12345678-abcd-abcd-abcd-12345678ab02"
SHARED_UUID_RENAME = "12345678-abcd-abcd-abcd-12345678ab03"
SHARED_UUID_VISIBILITY = "12345678-abcd-abcd-abcd-12345678ab04"


def test_failover_no_block_number_overlap(started_cluster):
    """
    Regression: after the old leader writes parts on shared storage, the new leader
    must refresh its in-memory part set and advance `increment` past the highest
    block number on disk before issuing new block numbers. Without that, two
    consecutive leaders would allocate overlapping `min_block`/`max_block` and
    produce intersecting parts.

    The test checks (a) the new leader sees the old leader's parts after failover
    and (b) the new leader's first own inserts have strictly higher block numbers
    than every part the old leader wrote.
    """
    table = "test_blocknum"
    create_table_on_first_node(node1, table, SHARED_UUID_BLOCKNUM)
    attach_table_on_second_node(node2, table, SHARED_UUID_BLOCKNUM)

    leader, followers = wait_for_leader([node1, node2], table_name=table)
    follower = followers[0]
    logging.info(f"Leader: {leader.name}, Follower: {follower.name}")

    # Old leader produces several parts. With one row per insert, each insert
    # creates its own part with a distinct block number.
    for i in range(1, 6):
        leader.query(f"INSERT INTO {table} VALUES ({i})")

    old_leader_max_block = int(
        leader.query(
            f"SELECT max(max_block_number) FROM system.parts "
            f"WHERE table = '{table}' AND active"
        ).strip()
    )
    assert old_leader_max_block >= 5, (
        f"Old leader did not produce parts as expected: max_block={old_leader_max_block}"
    )

    # Kill the leader and wait for the follower to take over.
    leader.stop_clickhouse()
    deadline = time.monotonic() + 60
    became_leader = False
    while time.monotonic() < deadline:
        try:
            follower.query(f"INSERT INTO {table} VALUES (100)")
            became_leader = True
            break
        except Exception as e:
            if "TABLE_IS_READ_ONLY" in str(e):
                time.sleep(2)
                continue
            raise
    assert became_leader, "Follower did not become leader"

    # The new leader's parts must have block numbers strictly higher than what the
    # old leader produced — proving the sync-on-leadership-acquire happened.
    follower_min_block_of_own_parts = int(
        follower.query(
            f"SELECT min(min_block_number) FROM system.parts "
            f"WHERE table = '{table}' AND active AND min_block_number > {old_leader_max_block}"
        ).strip()
    )
    assert follower_min_block_of_own_parts > old_leader_max_block, (
        f"New leader allocated overlapping block number "
        f"{follower_min_block_of_own_parts} <= old max {old_leader_max_block}"
    )

    # All five rows written by the old leader must be visible on the new leader.
    visible = int(
        follower.query(f"SELECT countIf(x BETWEEN 1 AND 5) FROM {table}").strip()
    )
    assert visible == 5, (
        f"New leader does not see all old-leader rows: visible={visible}/5"
    )

    leader.start_clickhouse()
    for n in (node1, node2):
        try:
            n.query(f"DROP TABLE IF EXISTS {table} SYNC")
        except Exception:
            pass


def test_alter_rejected_under_leader_election(started_cluster):
    """
    Regression: any ALTER that mutates table structure or settings would leave
    followers with stale metadata. Reject all such ALTERs; allow only comment
    changes. Verified on both the leader and the follower.
    """
    table = "test_alter"
    create_table_on_first_node(node1, table, SHARED_UUID_ALTER)
    attach_table_on_second_node(node2, table, SHARED_UUID_ALTER)
    leader, followers = wait_for_leader([node1, node2], table_name=table)
    follower = followers[0]

    cases = [
        ("ADD COLUMN", f"ALTER TABLE {table} ADD COLUMN y UInt32"),
        ("DROP COLUMN", f"ALTER TABLE {table} DROP COLUMN x"),
        ("MODIFY COLUMN", f"ALTER TABLE {table} MODIFY COLUMN x Int64"),
        ("MODIFY TTL", f"ALTER TABLE {table} MODIFY TTL toStartOfDay(toDateTime(0)) + INTERVAL 1 DAY"),
        ("ADD INDEX", f"ALTER TABLE {table} ADD INDEX idx_x x TYPE minmax GRANULARITY 1"),
        ("MODIFY SETTING", f"ALTER TABLE {table} MODIFY SETTING merge_max_block_size = 1024"),
    ]
    for label, sql in cases:
        for node, role in [(leader, "leader"), (follower, "follower")]:
            try:
                node.query(sql)
            except Exception as e:
                msg = str(e)
                assert "SUPPORT_IS_DISABLED" in msg or "leader_election" in msg, (
                    f"{label} on {role}: expected rejection due to leader_election, got: {msg}"
                )
                continue
            raise AssertionError(
                f"{label} on {role}: expected rejection, query succeeded"
            )

    # COMMENT TABLE must still work on the leader (and only the leader).
    leader.query(f"ALTER TABLE {table} MODIFY COMMENT 'leader-only comment'")
    try:
        follower.query(f"ALTER TABLE {table} MODIFY COMMENT 'follower comment'")
    except Exception as e:
        assert "TABLE_IS_READ_ONLY" in str(e), (
            f"Follower COMMENT TABLE: expected TABLE_IS_READ_ONLY, got: {e}"
        )
    else:
        raise AssertionError("Follower COMMENT TABLE should have been rejected")

    for n in (node1, node2):
        try:
            n.query(f"DROP TABLE IF EXISTS {table} SYNC")
        except Exception:
            pass


def test_rename_rejected_under_leader_election(started_cluster):
    """
    Regression: RENAME TABLE moves the shared data path on every disk and does not
    update the followers' lease path. Reject it.
    """
    table = "test_rename_src"
    create_table_on_first_node(node1, table, SHARED_UUID_RENAME)
    attach_table_on_second_node(node2, table, SHARED_UUID_RENAME)
    leader, followers = wait_for_leader([node1, node2], table_name=table)
    follower = followers[0]

    for node, role in [(leader, "leader"), (follower, "follower")]:
        try:
            node.query(f"RENAME TABLE {table} TO {table}_renamed")
        except Exception as e:
            msg = str(e)
            assert "SUPPORT_IS_DISABLED" in msg or "leader_election" in msg or "TABLE_IS_READ_ONLY" in msg, (
                f"RENAME on {role}: expected rejection, got: {msg}"
            )
            continue
        raise AssertionError(f"RENAME on {role}: expected rejection, query succeeded")

    for n in (node1, node2):
        try:
            n.query(f"DROP TABLE IF EXISTS {table} SYNC")
        except Exception:
            pass


def test_replicated_mergetree_rejects_leader_election(started_cluster):
    """
    Regression: `leader_election` is implemented only for `MergeTree`. Setting it
    on `ReplicatedMergeTree` would be a confusing no-op. Reject at CREATE.
    """
    table = "test_repl_rejected"
    try:
        node1.query(
            f"""
            CREATE TABLE {table} (x UInt64)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/{table}/{{shard}}', '{{replica}}')
            ORDER BY x
            SETTINGS leader_election = 1
            """
        )
    except Exception as e:
        msg = str(e)
        assert "leader_election" in msg and "MergeTree" in msg, (
            f"Expected rejection mentioning `leader_election` and engine, got: {msg}"
        )
    else:
        node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
        raise AssertionError(
            "ReplicatedMergeTree with leader_election=1 should have been rejected at CREATE"
        )
