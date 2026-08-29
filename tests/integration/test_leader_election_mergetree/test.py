import io
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
    main_configs=[
        "configs/config.d/storage_conf.xml",
        "configs/config.d/transactions.xml",
    ],
    with_minio=True,
    with_zookeeper=True,
    stay_alive=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=[
        "configs/config.d/storage_conf.xml",
        "configs/config.d/transactions.xml",
    ],
    with_minio=True,
    with_zookeeper=True,
    stay_alive=True,
)
node3 = cluster.add_instance(
    "node3",
    main_configs=[
        "configs/config.d/storage_conf.xml",
        "configs/config.d/transactions.xml",
    ],
    with_minio=True,
    with_zookeeper=True,
    stay_alive=True,
)
# The feature contract is active/standby failover WITHOUT ClickHouse Keeper, so at
# least one multi-node scenario must run on nodes that have no Keeper configured at
# all — otherwise a hidden dependency on Keeper-backed startup or cleanup paths would
# go unnoticed (every node above starts with `with_zookeeper=True`). No
# `transactions.xml` here either: the experimental transaction log requires Keeper.
node4_no_keeper = cluster.add_instance(
    "node4_no_keeper",
    main_configs=["configs/config.d/storage_conf.xml"],
    with_minio=True,
    with_zookeeper=False,
    stay_alive=True,
)
node5_no_keeper = cluster.add_instance(
    "node5_no_keeper",
    main_configs=["configs/config.d/storage_conf.xml"],
    with_minio=True,
    with_zookeeper=False,
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
    """Check if the node considers itself the leader by attempting an insert.

    The probe value is `x = 0` and every count-based assertion in the test suite
    filters with `x > 0`, so the probe rows are not cleaned up — `s3_plain_rewritable`
    (the shared-metadata disk these tests require) does not support mutations, and
    we would not be able to issue an `ALTER ... DELETE` here even on the leader.
    """
    try:
        node.query(f"INSERT INTO {table_name} VALUES (0)")
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


def test_async_insert_rejected_before_queueing_on_follower(started_cluster):
    """A follower must reject `async_insert` before the query enters its queue."""
    table = "test_le_async_insert"
    uuid = "12345678-abcd-abcd-abcd-123456789ad0"
    stopped_leader = None

    try:
        create_table_on_first_node(node1, table, uuid)
        attach_table_on_second_node(node2, table, uuid)
        leader, followers = wait_for_leader([node1, node2], table_name=table)
        follower = followers[0]

        for value, wait_for_async_insert in [(101, 0), (102, 1)]:
            error = ""
            try:
                follower.query(
                    f"INSERT INTO {table} SETTINGS async_insert = 1, "
                    f"wait_for_async_insert = {wait_for_async_insert} VALUES ({value})"
                )
            except Exception as e:
                error = str(e)
            assert "TABLE_IS_READ_ONLY" in error, (
                "Expected follower async insert to be rejected before queueing, "
                f"got: {error}"
            )

        leader.stop_clickhouse()
        stopped_leader = leader

        new_leader, _ = wait_for_leader([follower], table_name=table)
        new_leader.query(f"INSERT INTO {table} VALUES (1)")
        assert new_leader.query(
            f"SELECT count() FROM {table} WHERE x IN (101, 102)"
        ).strip() == "0", "A follower-side async insert was flushed after takeover"
    finally:
        if stopped_leader:
            stopped_leader.start_clickhouse()
        node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
        node2.query(f"DROP TABLE IF EXISTS {table} SYNC")


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

    # Failover leg: these are the counters operators use to diagnose lease churn, so
    # exercise them through one real leadership change. Detaching the table on the
    # leader stops its election (recording `MergeTreeLeaderElectionLost` on that node)
    # and stops lease renewal, so the lease expires and the follower claims it
    # (recording `MergeTreeLeaderElectionLeaseTakeovers` on the follower's node).
    baseline_lost = {n.name: event(n, "MergeTreeLeaderElectionLost") for n in [node1, node2]}
    baseline_takeovers = {
        n.name: event(n, "MergeTreeLeaderElectionLeaseTakeovers") for n in [node1, node2]
    }

    leader.query(f"DETACH TABLE {table}")
    assert event(leader, "MergeTreeLeaderElectionLost") - baseline_lost[leader.name] >= 1, (
        f"{leader.name} did not increment MergeTreeLeaderElectionLost after losing leadership"
    )

    new_leader, _ = wait_for_leader([follower], table_name=table)
    assert new_leader is follower
    assert (
        event(follower, "MergeTreeLeaderElectionLeaseTakeovers")
        - baseline_takeovers[follower.name]
        >= 1
    ), f"{follower.name} did not increment MergeTreeLeaderElectionLeaseTakeovers after takeover"

    # Re-attach on the old leader (it rejoins as a follower) so the drop-time gauge
    # checks below cover both nodes again.
    leader.query(f"ATTACH TABLE {table}")

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

    # The restarted old leader is now a follower. The failover contract requires it to
    # refresh and observe the row the new leader committed after takeover (x = 10), not
    # only the rows it wrote before failover (1, 2, 3). Follower part refresh is
    # asynchronous, so poll for a bounded period until the post-takeover write appears,
    # then assert the exact positive row set (the rejected x = 999 probes must be absent).
    expected = "1\n2\n3\n10"
    deadline = time.monotonic() + 60
    rows = ""
    while time.monotonic() < deadline:
        rows = leader.query("SELECT x FROM test_fo WHERE x > 0 ORDER BY x").strip()
        if rows == expected:
            break
        time.sleep(1)
    assert rows == expected, (
        "Restarted old leader (now follower) did not refresh to the full post-failover "
        f"row set; expected x in {{1, 2, 3, 10}}, got: {rows!r}"
    )

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
    initial_leader, _ = wait_for_leader(nodes, table_name=table)

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
            except Exception:
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
        #
        # Take down the current leader first. Only killing the leader forces a
        # failover (followers are already read-only), so if it lands late in the
        # kill order it may never be reached within the test window: on slow
        # builds a single kill-and-restart cycle can take ~15 s, so only two of
        # three cycles fit in 30 s. Killing the leader up front guarantees a
        # failover early in the run regardless of build speed.
        rng = random.Random(20260506)
        order = list(nodes)
        rng.shuffle(order)
        order.remove(initial_leader)
        order.insert(0, initial_leader)
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

    # Invariant 3: data integrity on failover. The active/standby contract is that
    # whoever holds the lease can serve the full history — not just the rows that
    # node happened to write. After leadership stabilises post-chaos, the elected
    # leader must see every successful insert from every previous epoch, and only
    # values we attempted.
    success_values = set(r[4] for r in successes)
    attempted_values = set(r[4] for r in records)
    leader, _followers = wait_for_leader(nodes, table_name=table)
    # `wait_for_leader` writes `x = 0` probes via `is_leader`; the table cannot
    # delete them on `s3_plain_rewritable`, so exclude them here. Worker inserts
    # always use `x >= base + 1` (base >= 10**9), so no real row is filtered out.
    rows_str = leader.query(f"SELECT x FROM {table} WHERE x > 0", timeout=30)
    leader_visible = {int(line) for line in rows_str.strip().split("\n") if line}
    logging.info(f"  elected leader {leader.name}: sees {len(leader_visible)} rows")

    missing = success_values - leader_visible
    if missing:
        sample = sorted(missing)[:10]
        raise AssertionError(
            f"Failover data loss: {len(missing)} of {len(success_values)} successful "
            f"inserts not visible on the elected leader {leader.name}. "
            f"Sample missing values: {sample}"
        )

    extra = leader_visible - attempted_values
    if extra:
        sample = sorted(extra)[:10]
        raise AssertionError(
            f"Phantom rows: {len(extra)} values on the elected leader that we never "
            f"tried to insert. Sample: {sample}"
        )

    # Invariant 4: merges happened. Each insert produces a part; the leader's
    # background scheduler must have merged at least some of them. With shared
    # metadata every node observes the same active-part set, so reading from the
    # leader is sufficient.
    count_str = leader.query(
        f"SELECT count() FROM system.parts "
        f"WHERE table = '{table}' AND active"
    ).strip()
    active_parts = int(count_str)
    expected_count = len(success_values)
    logging.info(f"Inserts: {expected_count}, active parts on leader {leader.name}: {active_parts}")
    assert active_parts < expected_count, (
        f"No merges happened: {active_parts} active parts for {expected_count} inserts"
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
SHARED_UUID_FOLLOWER_REFRESH = "12345678-abcd-abcd-abcd-12345678ab05"
SHARED_UUID_EPOCH = "12345678-abcd-abcd-abcd-12345678ab06"


def ensure_node_up(node, timeout=60):
    """Bring `node` back up if it was left stopped by a previous failed test."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            node.query("SELECT 1", timeout=5)
            return
        except Exception:
            try:
                node.start_clickhouse()
            except Exception:
                time.sleep(1)
                continue
            time.sleep(1)
    raise RuntimeError(f"Could not bring {node.name} back up within {timeout}s")


# Note on block-number safety on failover:
#
# The leader-election callback advances `increment` to
#   `max(local_max_block_number, getMaxBlockNumberFromObjectStorage())`
# before background writes are enabled, so a freshly elected leader can never
# allocate a block number that collides with a part the previous leader
# committed on shared storage. The chaos test above (`test_chaos_failover`)
# exercises this path on `s3_plain_rewritable`, where the object key IS the
# part path and a colliding block number would produce a real conflict.


def test_alter_rejected_under_leader_election(started_cluster):
    """
    Regression: any ALTER that mutates table structure or settings would leave
    followers with stale metadata. Reject all such ALTERs; allow only comment
    changes. Verified on both the leader and the follower.
    """
    for n in (node1, node2):
        ensure_node_up(n)
    table = "test_alter"
    try:
        create_table_on_first_node(node1, table, SHARED_UUID_ALTER)
        attach_table_on_second_node(node2, table, SHARED_UUID_ALTER)
        leader, followers = wait_for_leader([node1, node2], table_name=table)
        follower = followers[0]

        # `DROP COLUMN x` is intentionally omitted: the table has only `x`, so the
        # alter interpreter rejects it with `Cannot DROP all columns` before
        # reaching `StorageMergeTree::alter` — that rejection is correct but
        # tests a different code path than the leader-election guard.
        cases = [
            ("ADD COLUMN", f"ALTER TABLE {table} ADD COLUMN y UInt32"),
            ("MODIFY COLUMN", f"ALTER TABLE {table} MODIFY COLUMN x Int64"),
            ("MODIFY TTL", f"ALTER TABLE {table} MODIFY TTL toStartOfDay(toDateTime(0)) + INTERVAL 1 DAY"),
            ("ADD INDEX", f"ALTER TABLE {table} ADD INDEX idx_x x TYPE minmax GRANULARITY 1"),
            ("MODIFY SETTING", f"ALTER TABLE {table} MODIFY SETTING merge_max_block_size = 1024"),
        ]
        for label, sql in cases:
            # `StorageMergeTree::checkAlterIsPossible` runs on every node from
            # `InterpreterAlterQuery::executeToTable`, before any read-only gate,
            # so a non-comment `ALTER` is rejected with `SUPPORT_IS_DISABLED` on
            # the follower exactly like on the leader — a follower must NOT
            # report `TABLE_IS_READ_ONLY` here (that is reserved for writes).
            for node, role, accepted in [
                (leader, "leader", ("SUPPORT_IS_DISABLED", "leader_election")),
                (follower, "follower", ("SUPPORT_IS_DISABLED", "leader_election")),
            ]:
                try:
                    node.query(sql)
                except Exception as e:
                    msg = str(e)
                    if any(s in msg for s in accepted):
                        continue
                    raise AssertionError(
                        f"{label} on {role}: expected one of {accepted}, got: {msg}"
                    )
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
    finally:
        for n in (node1, node2):
            try:
                n.query(f"DROP TABLE IF EXISTS {table} SYNC")
            except Exception:
                pass


def test_rename_rejected_under_leader_election(started_cluster):
    """
    Regression: `RENAME TABLE` must be rejected under `leader_election` on both
    the leader and a follower. The rejection lives in
    `StorageMergeTree::checkTableCanBeRenamed` (not only in `rename`), because
    the default `Atomic` database renames a table through
    `checkTableCanBeRenamed` + `renameInMemory` and never calls
    `StorageMergeTree::rename` — only the deprecated on-disk/`Ordinary` path does.
    """
    for n in (node1, node2):
        ensure_node_up(n)
    table = "test_rename"
    new_table = "test_rename_new"
    try:
        create_table_on_first_node(node1, table, SHARED_UUID_RENAME)
        attach_table_on_second_node(node2, table, SHARED_UUID_RENAME)
        leader, followers = wait_for_leader([node1, node2], table_name=table)
        follower = followers[0]

        # `StorageMergeTree::checkTableCanBeRenamed` rejects the rename on every
        # node before any read-only gate runs, so both the leader and the
        # follower must report `SUPPORT_IS_DISABLED`, never `TABLE_IS_READ_ONLY`.
        for node, role, accepted in [
            (leader, "leader", ("SUPPORT_IS_DISABLED", "leader_election")),
            (follower, "follower", ("SUPPORT_IS_DISABLED", "leader_election")),
        ]:
            try:
                node.query(f"RENAME TABLE {table} TO {new_table}")
            except Exception as e:
                msg = str(e)
                if any(s in msg for s in accepted):
                    continue
                raise AssertionError(
                    f"RENAME on {role}: expected one of {accepted}, got: {msg}"
                )
            raise AssertionError(
                f"RENAME on {role}: expected rejection, query succeeded"
            )
    finally:
        for n in (node1, node2):
            for t in (table, new_table):
                try:
                    n.query(f"DROP TABLE IF EXISTS {t} SYNC")
                except Exception:
                    pass


def test_local_disk_rejects_leader_election(started_cluster):
    """
    Regression: `leader_election` on the default local-disk policy must be
    rejected at create. The local disk is not an `S3`/`Azure` object storage
    backend, so the active/standby contract cannot be satisfied — followers
    have no way to see parts the leader wrote.
    """
    ensure_node_up(node1)
    table = "test_local_disk_rejected"
    try:
        node1.query(
            f"""
            CREATE TABLE {table} (x UInt64)
            ENGINE = MergeTree ORDER BY x
            SETTINGS leader_election = 1
            """
        )
    except Exception as e:
        msg = str(e)
        assert "leader_election" in msg and "backend" in msg, (
            f"Expected rejection mentioning `leader_election` and the unsupported backend, got: {msg}"
        )
        return
    finally:
        try:
            node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
        except Exception:
            pass
    raise AssertionError(
        "MergeTree with leader_election=1 on the default local disk policy "
        "should have been rejected at CREATE"
    )


def test_local_metadata_rejects_leader_election(started_cluster):
    """
    Regression: `leader_election` on a plain `s3` disk with `metadata_type = local`
    must be rejected at create. The object storage is shared, but the metadata is
    per-replica — after a failover, the new leader would not see the previous
    leader's parts in its local metadata. This is the second rejection path in
    `StorageMergeTree`'s constructor, distinct from the unsupported-backend path
    exercised by `test_local_disk_rejects_leader_election`.
    """
    ensure_node_up(node1)
    table = "test_local_md_rejected"
    try:
        node1.query(
            f"""
            CREATE TABLE {table} (x UInt64)
            ENGINE = MergeTree ORDER BY x
            SETTINGS storage_policy = 's3_local_md', leader_election = 1
            """
        )
    except Exception as e:
        msg = str(e)
        assert "leader_election" in msg and "metadata" in msg, (
            f"Expected rejection mentioning `leader_election` and metadata, got: {msg}"
        )
        return
    finally:
        try:
            node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
        except Exception:
            pass
    raise AssertionError(
        "MergeTree with leader_election=1 on an S3 disk with local metadata "
        "should have been rejected at CREATE"
    )


def test_mixed_policy_rejects_leader_election(started_cluster):
    """
    Regression: `leader_election` must validate **every** disk in the storage policy,
    not only the first volume. A policy whose first volume is a valid shared
    `plain_rewritable` `S3` disk but whose second volume is invalid (a local disk, or
    an `S3` disk with `metadata_type = local`) could still strand parts on a
    node-invisible disk via `TTL`-driven moves or a volume overflow, so it must be
    rejected at create. A regression back to checking only the primary volume would
    accept both policies below.
    """
    ensure_node_up(node1)
    for policy, table, offending_disk, expected_word in [
        ("s3_mixed_local_md", "test_mixed_local_md_rejected", "s3_local_md", "metadata"),
        ("s3_mixed_local_disk", "test_mixed_local_disk_rejected", "default", "backend"),
    ]:
        rejected = False
        try:
            node1.query(
                f"""
                CREATE TABLE {table} (x UInt64)
                ENGINE = MergeTree ORDER BY x
                SETTINGS storage_policy = '{policy}', leader_election = 1
                """
            )
        except Exception as e:
            msg = str(e)
            assert "leader_election" in msg and expected_word in msg, (
                f"Expected rejection of policy `{policy}` mentioning `leader_election` "
                f"and {expected_word}, got: {msg}"
            )
            assert f"'{offending_disk}'" in msg, (
                f"Expected the rejection of policy `{policy}` to name the offending "
                f"second-volume disk `{offending_disk}` (proving validation went past "
                f"the first volume), got: {msg}"
            )
            rejected = True
        finally:
            try:
                node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
            except Exception:
                pass
        assert rejected, (
            f"MergeTree with leader_election=1 on mixed policy `{policy}` (valid shared "
            "first volume, invalid second volume) should have been rejected at CREATE"
        )


def test_follower_sees_leader_writes(started_cluster):
    """
    Regression: a follower must periodically re-scan shared object storage so that
    `SELECT` observes parts the current leader has committed since the follower
    started up. Without the follower-side refresh, follower reads would lag
    indefinitely until takeover or restart.
    """
    for n in (node1, node2):
        ensure_node_up(n)
    table = "test_follower_refresh"
    try:
        create_table_on_first_node(node1, table, SHARED_UUID_FOLLOWER_REFRESH)
        attach_table_on_second_node(node2, table, SHARED_UUID_FOLLOWER_REFRESH)

        leader, followers = wait_for_leader([node1, node2], table_name=table)
        follower = followers[0]
        logging.info(f"Leader: {leader.name}, Follower: {follower.name}")

        leader.query(f"INSERT INTO {table} VALUES (1), (2), (3)")

        # The follower's refresh task runs at the heartbeat cadence (1 s in
        # `TABLE_SETTINGS`). Allow several cycles before failing.
        deadline = time.monotonic() + 60
        follower_count = 0
        while time.monotonic() < deadline:
            follower_count = int(
                follower.query(f"SELECT count() FROM {table} WHERE x > 0").strip()
            )
            if follower_count >= 3:
                break
            time.sleep(1)

        assert follower_count >= 3, (
            f"Follower did not observe leader's parts after refresh interval "
            f"(saw {follower_count} rows, expected at least 3)"
        )

        # A second batch from the leader must also become visible on the follower.
        leader.query(f"INSERT INTO {table} VALUES (4), (5)")
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            follower_count = int(
                follower.query(f"SELECT count() FROM {table} WHERE x > 0").strip()
            )
            if follower_count >= 5:
                break
            time.sleep(1)

        assert follower_count >= 5, (
            f"Follower did not observe the leader's second batch after refresh "
            f"(saw {follower_count} rows, expected at least 5)"
        )
    finally:
        for n in (node1, node2):
            try:
                n.query(f"DROP TABLE IF EXISTS {table} SYNC")
            except Exception:
                pass


def test_replicated_mergetree_rejects_leader_election(started_cluster):
    """
    Regression: `leader_election` is implemented only for `MergeTree`. Setting it
    on `ReplicatedMergeTree` would be a confusing no-op. Reject at CREATE,
    *before* any ZooKeeper interaction, so the test does not require ZooKeeper.
    """
    ensure_node_up(node1)
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
        return
    finally:
        try:
            node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
        except Exception:
            pass
    raise AssertionError(
        "ReplicatedMergeTree with leader_election=1 should have been rejected at CREATE"
    )


def test_readonly_table_rejects_leader_election(started_cluster):
    """
    Regression: a `table_readonly` table skips `StorageMergeTree::startup`, which is where the
    leader-election task is created. Allowing it to be attached would let a later setting change
    turn it into a writer without a lease, so reject the unsafe configuration at CREATE.
    """
    ensure_node_up(node1)
    table = "test_readonly_leader_election_rejected"
    try:
        node1.query(
            f"""
            CREATE TABLE {table} (x UInt64)
            ENGINE = MergeTree ORDER BY x
            SETTINGS {TABLE_SETTINGS}, table_readonly = 1
            """
        )
    except Exception as e:
        msg = str(e)
        assert "table_readonly" in msg and "leader_election" in msg, (
            f"Expected rejection mentioning both settings, got: {msg}"
        )
        return
    finally:
        try:
            node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
        except Exception:
            pass
    raise AssertionError(
        "MergeTree with table_readonly=1 and leader_election=1 should have been rejected at CREATE"
    )


def test_commit_rejected_on_stale_leadership_epoch(started_cluster):
    """
    Regression for the commit-time epoch fence (`assertWritableLeaderAtEpoch`): a write must not
    publish a part if leadership was lost (and possibly reacquired) between the write's admission
    and its commit. A non-refresh `Transaction::commit` that raced a leadership change could
    otherwise rename a part onto shared storage that a new leader then activates.

    The `merge_tree_leader_election_stale_epoch_before_commit` failpoint deterministically makes
    `MergeTreeSink::commitPart` present an admission epoch older than the current one, so the real
    pre-rename guard rejects the INSERT. Because the guard runs BEFORE the rename that publishes
    the part, the rejected INSERT must leave no new part behind (no orphan a new leader could later
    activate) — this is what the test asserts via the unchanged row count.
    """
    ensure_node_up(node1)
    failpoint = "merge_tree_leader_election_stale_epoch_before_commit"
    table = "test_epoch_fence"
    try:
        create_table_on_first_node(node1, table, SHARED_UUID_EPOCH)
        wait_for_leader([node1], table_name=table)

        node1.query(f"INSERT INTO {table} VALUES (1), (2), (3)")
        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 3

        node1.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
        try:
            rejected = False
            try:
                node1.query(f"INSERT INTO {table} VALUES (10)")
            except Exception as e:
                msg = str(e)
                assert "Leadership epoch" in msg or "stale lease" in msg, (
                    f"INSERT was rejected, but not by the leadership-epoch fence: {msg}"
                )
                rejected = True
            assert rejected, "INSERT under a stale leadership epoch should have been rejected"

            # The rejection must happen before the publishing rename: no new part is committed.
            assert (
                int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 3
            ), "A part was published despite the stale-epoch rejection (orphan part)"
        finally:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")

        # With the failpoint cleared the same INSERT succeeds and becomes visible.
        node1.query(f"INSERT INTO {table} VALUES (10)")
        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 4
    finally:
        try:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        except Exception:
            pass
        try:
            node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
        except Exception:
            pass


SHARED_UUID_DDL_EPOCH = "12345678-abcd-abcd-abcd-12345678ab07"
SHARED_UUID_DDL_CLEANUP = "12345678-abcd-abcd-abcd-12345678ab08"


def test_partition_ddl_rejected_on_stale_leadership_epoch(started_cluster):
    """
    Regression for the admission-epoch fence on the partition-DDL publish paths: `TRUNCATE`,
    `DROP PARTITION` (via `renameAndCommitEmptyParts`) and `ATTACH PARTITION` (via
    `renameTempPartAndAdd`) must not publish parts into the shared prefix if leadership was lost
    (and possibly reacquired) between the DDL's admission and the first non-temporary rename.

    The `merge_tree_leader_election_stale_epoch_before_commit` failpoint (which now fires inside
    `assertWritableLeaderAtEpoch`, covering every fenced path) deterministically presents a stale
    admission epoch, so each DDL must be rejected BEFORE anything becomes visible.
    """
    ensure_node_up(node1)
    failpoint = "merge_tree_leader_election_stale_epoch_before_commit"
    table = "test_ddl_epoch_fence"
    try:
        node1.query(
            f"""
            CREATE TABLE {table} UUID '{SHARED_UUID_DDL_EPOCH}' (x UInt64)
            ENGINE = MergeTree PARTITION BY x % 2 ORDER BY x
            SETTINGS {TABLE_SETTINGS}
            """
        )
        wait_for_leader([node1], table_name=table)

        node1.query(f"INSERT INTO {table} VALUES (1), (2), (3), (4)")
        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 4

        node1.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
        try:
            # TRUNCATE: the covering empty parts must not be published under a stale epoch.
            with pytest.raises(Exception, match="Leadership epoch"):
                node1.query(f"TRUNCATE TABLE {table}")
            assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 4, (
                "TRUNCATE was rejected but the covering empty parts were still published"
            )

            # DROP PARTITION: same fence, same invariant.
            with pytest.raises(Exception, match="Leadership epoch"):
                node1.query(f"ALTER TABLE {table} DROP PARTITION 1")
            assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 4, (
                "DROP PARTITION was rejected but the covering empty parts were still published"
            )
        finally:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")

        # ATTACH PARTITION: detach with the failpoint cleared, then attach under a stale epoch.
        node1.query(f"ALTER TABLE {table} DETACH PARTITION 1")
        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 2

        node1.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
        try:
            with pytest.raises(Exception, match="Leadership epoch"):
                node1.query(f"ALTER TABLE {table} ATTACH PARTITION 1")
            assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 2, (
                "ATTACH PARTITION was rejected but the part was still published"
            )
        finally:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")

        # With the failpoint cleared the same DDLs succeed.
        node1.query(f"ALTER TABLE {table} ATTACH PARTITION 1")
        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 4
        node1.query(f"TRUNCATE TABLE {table}")
        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 0
    finally:
        try:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        except Exception:
            pass
        try:
            node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
        except Exception:
            pass


def test_cleanup_skipped_on_stale_lease_after_partition_ddl(started_cluster):
    """
    Regression for the post-DDL cleanup gate: if the lease goes stale between a non-transactional
    `TRUNCATE` / `DROP PARTITION` committing its covering empty parts and the synchronous cleanup,
    the stale node must skip `clearEmptyParts` (and the rest of the destructive cleanup) — else it
    would drop the covering empty parts while the covered old parts remain on disk, letting the old
    data reappear later.

    The `merge_tree_leader_election_stale_lease_cleanup` failpoint makes `canRunDestructiveCleanup`
    report a stale lease, so the DDL itself succeeds but the cleanup is skipped: the covered parts
    must remain on disk (as `Outdated`) together with their covering empty parts.
    """
    ensure_node_up(node1)
    failpoint = "merge_tree_leader_election_stale_lease_cleanup"
    table = "test_ddl_cleanup_gate"
    try:
        node1.query(
            f"""
            CREATE TABLE {table} UUID '{SHARED_UUID_DDL_CLEANUP}' (x UInt64)
            ENGINE = MergeTree ORDER BY x
            SETTINGS {TABLE_SETTINGS}, old_parts_lifetime = 1
            """
        )
        wait_for_leader([node1], table_name=table)

        node1.query(f"INSERT INTO {table} VALUES (1), (2), (3)")
        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 3

        node1.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
        try:
            # The DDL itself succeeds (the empty parts commit before the lease goes stale) ...
            node1.query(f"TRUNCATE TABLE {table}")
            assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 0

            # ... but the destructive cleanup is skipped: the covered old parts must still be on
            # disk (Outdated), not removed by a node whose lease is stale.
            outdated = int(
                node1.query(
                    f"SELECT count() FROM system.parts WHERE database = currentDatabase()"
                    f" AND table = '{table}' AND NOT active"
                ).strip()
            )
            assert outdated > 0, (
                "The stale node removed the covered parts despite the stale lease"
            )
        finally:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")

        # The table stays truncated after the lease is fresh again.
        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 0
    finally:
        try:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        except Exception:
            pass
        try:
            node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
        except Exception:
            pass


SHARED_UUID_BROKEN = "12345678-abcd-abcd-abcd-12345678ab09"


def find_part_object_key_prefix(uuid, part_name):
    """Locate the S3 key prefix of a part directory on the `plain_rewritable` disk.

    The disk stores each logical directory under a random remapped prefix:
    `data/__meta/<prefix>/prefix.path` holds the logical directory path, and the
    directory's files live flat under `data/<prefix>/`.
    """
    for obj in cluster.minio_client.list_objects(
        cluster.minio_bucket, "data/__meta/", recursive=True
    ):
        if not obj.object_name.endswith("/prefix.path"):
            continue
        response = cluster.minio_client.get_object(
            cluster.minio_bucket, obj.object_name
        )
        try:
            content = response.read().decode().strip()
        finally:
            response.close()
            response.release_conn()
        if uuid in content and content.rstrip("/").endswith("/" + part_name):
            return obj.object_name.replace("__meta/", "", 1)[: -len("prefix.path")]
    return None


def test_broken_part_detached_on_takeover(started_cluster):
    """
    Regression test: a broken part whose cleanup was skipped by the read-only pre-lease
    startup load must not livelock leadership acquisition.

    Under `leader_election`, the startup part loaders run before the lease is acquired
    and therefore load read-only (only the lease-holding leader may mutate shared
    storage), so a broken part is left in place under its active name instead of being
    detached. The takeover scan (`loadNewlyAppearedParts` with `strict_takeover`) then
    rediscovers it; if it aborted the takeover unconditionally, every heartbeat retry
    would hit the same part and the table would stay read-only forever. The new leader
    must instead replay the skipped cleanup: detach the broken part as
    `broken-on-start`, keep the intact parts, and enable writes.
    """
    ensure_node_up(node1)
    table = "test_broken_takeover"
    node1.query(
        f"""
        CREATE TABLE {table} UUID '{SHARED_UUID_BROKEN}' (x UInt64)
        ENGINE = MergeTree ORDER BY x
        SETTINGS {TABLE_SETTINGS}
        """
    )
    try:
        wait_for_leader([node1], table_name=table)

        # Two separate parts; merges are stopped so the part we corrupt survives as-is
        # until the server is stopped (`STOP MERGES` does not persist across restarts,
        # but by then the part is already corrupted and skipped by loading).
        node1.query(f"SYSTEM STOP MERGES {table}")
        node1.query(f"INSERT INTO {table} VALUES (1), (2), (3)")
        node1.query(f"INSERT INTO {table} VALUES (4), (5)")

        part_name = node1.query(
            f"SELECT name FROM system.parts WHERE database = currentDatabase()"
            f" AND table = '{table}' AND active AND rows = 2"
        ).strip()
        assert part_name, "Could not find the two-row part to corrupt"

        node1.stop_clickhouse()

        # Corrupt the part on shared storage while the server is down, so the next
        # startup's pre-lease part loading classifies it as broken.
        key_prefix = find_part_object_key_prefix(SHARED_UUID_BROKEN, part_name)
        assert key_prefix, f"Could not locate part {part_name} on the object storage"
        payload = b"broken by test_broken_part_detached_on_takeover"
        started_cluster.minio_client.put_object(
            started_cluster.minio_bucket,
            key_prefix + "checksums.txt",
            io.BytesIO(payload),
            len(payload),
        )

        node1.start_clickhouse()

        # Without the takeover-time cleanup this times out: every heartbeat rediscovers
        # the broken part, aborts the takeover, and the table stays read-only.
        wait_for_leader([node1], table_name=table)

        # The intact part survived and the corrupted one is out of the active set ...
        assert (
            node1.query(f"SELECT x FROM {table} WHERE x > 0 ORDER BY x").strip()
            == "1\n2\n3"
        )

        # ... and was detached rather than silently dropped.
        detached = node1.query(
            f"SELECT name FROM system.detached_parts WHERE database = currentDatabase()"
            f" AND table = '{table}'"
        ).strip()
        assert "broken-on-start" in detached, (
            f"Expected a broken-on-start detached part, got: {detached!r}"
        )
    finally:
        try:
            node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
        except Exception:
            pass


SHARED_UUID_TXN_DDL = "12345678-abcd-abcd-abcd-12345678ab10"


def test_transactional_partition_ddl_rejected_under_leader_election(started_cluster):
    """
    Regression for the transactional admission check on partition DDL: on a table with
    `leader_election` the final `COMMIT TRANSACTION` is not fenced by the leadership lease, so
    partition commands must be rejected up front (`NOT_IMPLEMENTED`) when they run inside an
    explicit transaction, instead of failing deep inside version-metadata writes or, worse,
    letting a leader that lost its lease finalize the operation after failover.

    Commands whose target table is the `leader_election` table are already rejected by the
    interpreter (`supportsTransactions` is false on `plain_rewritable`), even with
    `throw_on_unsupported_query_inside_transaction = 0`. The interesting path is
    `MOVE PARTITION TO TABLE` from a plain source into a `leader_election` destination: the
    interpreter checks only the source table, so the destination-side storage check added in
    `movePartitionToTable` is what rejects it.
    """
    ensure_node_up(node1)
    table = "test_txn_ddl"
    plain = "test_txn_ddl_plain"
    try:
        node1.query(
            f"""
            CREATE TABLE {table} UUID '{SHARED_UUID_TXN_DDL}' (x UInt64)
            ENGINE = MergeTree PARTITION BY x % 2 ORDER BY x
            SETTINGS {TABLE_SETTINGS}
            """
        )
        wait_for_leader([node1], table_name=table)
        node1.query(f"INSERT INTO {table} VALUES (1), (2), (3), (4)")

        node1.query(
            f"CREATE TABLE {plain} (x UInt64) ENGINE = MergeTree PARTITION BY x % 2 ORDER BY x"
        )
        node1.query(f"INSERT INTO {plain} VALUES (5), (6)")

        # Detach a partition non-transactionally so the transactional ATTACH below would have
        # something to publish if it were (incorrectly) admitted.
        node1.query(f"ALTER TABLE {table} DETACH PARTITION 1")
        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 2

        rejected = [
            # Interpreter-level rejection: the target table does not support transactions.
            (f"ALTER TABLE {table} ATTACH PARTITION 1", "does not support transactions"),
            (f"ALTER TABLE {table} ATTACH PARTITION 1 FROM {plain}", "does not support transactions"),
            (f"ALTER TABLE {table} REPLACE PARTITION 1 FROM {plain}", "does not support transactions"),
            (f"ALTER TABLE {table} MOVE PARTITION 0 TO TABLE {plain}", "does not support transactions"),
            # Destination-side storage rejection: the plain source passes the interpreter check,
            # so only the check inside `movePartitionToTable` fences the destination.
            (
                f"ALTER TABLE {plain} MOVE PARTITION 1 TO TABLE {table}",
                "not supported for tables with the leader_election setting",
            ),
        ]
        for ddl, expected in rejected:
            error = node1.query_and_get_error(
                f"BEGIN TRANSACTION; {ddl}; COMMIT;",
                settings={"throw_on_unsupported_query_inside_transaction": 0},
            )
            assert "NOT_IMPLEMENTED" in error and expected in error, (
                f"{ddl} inside a transaction must be rejected at admission, got: {error!r}"
            )

        # Nothing was published or moved by the rejected commands.
        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 2
        assert int(node1.query(f"SELECT count() FROM {plain}").strip()) == 2

        # Outside a transaction the same commands are admitted.
        node1.query(f"ALTER TABLE {table} ATTACH PARTITION 1")
        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 4
    finally:
        for t in (table, plain):
            try:
                node1.query(f"DROP TABLE IF EXISTS {t} SYNC")
            except Exception:
                pass


SHARED_UUID_DDL_CLEANUP_MID = "12345678-abcd-abcd-abcd-12345678ab11"


def test_cleanup_stops_when_lease_goes_stale_mid_sequence(started_cluster):
    """
    Regression for the second freshness check inside the post-DDL cleanup: the lease may still be
    fresh when the cleanup sequence starts but go stale inside `clearOldMutations` /
    `clearOldPartsFromFilesystem`. `clearEmptyParts` must then be skipped — else the stale node
    would outdate the just-committed covering empty parts while the covered parts remain on disk,
    letting the dropped data reappear later (e.g. if this node reacquires the lease and deletes
    the empty parts, or crashes mid-cleanup).

    The `merge_tree_leader_election_stale_lease_before_clear_empty` failpoint simulates exactly
    that: the first check passes, the helpers run, and staleness is detected only before
    `clearEmptyParts`. The DDL itself succeeds, and the covering empty parts must stay active.
    """
    ensure_node_up(node1)
    failpoint = "merge_tree_leader_election_stale_lease_before_clear_empty"
    table = "test_ddl_cleanup_mid_sequence"
    try:
        node1.query(
            f"""
            CREATE TABLE {table} UUID '{SHARED_UUID_DDL_CLEANUP_MID}' (x UInt64)
            ENGINE = MergeTree ORDER BY x
            SETTINGS {TABLE_SETTINGS}, old_parts_lifetime = 1,
                     cleanup_delay_period = 60, cleanup_delay_period_random_add = 0
            """
        )
        wait_for_leader([node1], table_name=table)

        node1.query(f"INSERT INTO {table} VALUES (1), (2), (3)")
        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 3

        node1.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
        try:
            # The DDL succeeds: the first freshness check passes and the empty parts commit.
            node1.query(f"TRUNCATE TABLE {table}")
            assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 0

            # But `clearEmptyParts` must have been skipped: the covering empty parts stay active
            # instead of being outdated by a node whose lease went stale mid-cleanup.
            active_empty = int(
                node1.query(
                    f"SELECT count() FROM system.parts WHERE database = currentDatabase()"
                    f" AND table = '{table}' AND active AND rows = 0"
                ).strip()
            )
            assert active_empty > 0, (
                "The covering empty parts were dropped despite the lease going stale mid-cleanup"
            )
        finally:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")

        # The table stays truncated after the lease is fresh again.
        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 0
    finally:
        try:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        except Exception:
            pass
        try:
            node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
        except Exception:
            pass


SHARED_UUID_DETACH_FENCE = "12345678-abcd-abcd-abcd-12345678ab12"


def test_detach_rejected_before_writing_detached_copy(started_cluster):
    """
    Regression for the epoch fence on `DETACH PART` / `DETACH PARTITION`: the detached copy is an
    irreversible shared-storage side effect written BEFORE the empty-part publish, so a stale
    leader must be rejected before `makeCloneInDetached` runs. Otherwise a rejected `DETACH`
    would leave a persistent detached copy behind, and a later `ATTACH PARTITION` could re-import
    data from a DDL that supposedly failed.

    The `merge_tree_leader_election_stale_epoch_before_commit` failpoint presents a stale
    admission epoch, so the `DETACH` must fail with no detached copy created.
    """
    ensure_node_up(node1)
    failpoint = "merge_tree_leader_election_stale_epoch_before_commit"
    table = "test_detach_epoch_fence"
    try:
        node1.query(
            f"""
            CREATE TABLE {table} UUID '{SHARED_UUID_DETACH_FENCE}' (x UInt64)
            ENGINE = MergeTree PARTITION BY x % 2 ORDER BY x
            SETTINGS {TABLE_SETTINGS}
            """
        )
        wait_for_leader([node1], table_name=table)

        node1.query(f"INSERT INTO {table} VALUES (1), (2), (3), (4)")
        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 4

        node1.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
        try:
            with pytest.raises(Exception, match="Leadership epoch"):
                node1.query(f"ALTER TABLE {table} DETACH PARTITION 1")
            assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 4, (
                "DETACH PARTITION was rejected but the parts were still hidden"
            )
            detached = int(
                node1.query(
                    f"SELECT count() FROM system.detached_parts"
                    f" WHERE database = currentDatabase() AND table = '{table}'"
                ).strip()
            )
            assert detached == 0, (
                "The rejected DETACH left a detached copy behind on shared storage"
            )
        finally:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")

        # With the failpoint cleared the same DETACH succeeds and creates the detached copy.
        node1.query(f"ALTER TABLE {table} DETACH PARTITION 1")
        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 2
        detached = int(
            node1.query(
                f"SELECT count() FROM system.detached_parts"
                f" WHERE database = currentDatabase() AND table = '{table}'"
            ).strip()
        )
        assert detached > 0
    finally:
        try:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        except Exception:
            pass
        try:
            node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
        except Exception:
            pass


SHARED_UUID_TAKEOVER_SCAN = "12345678-abcd-abcd-abcd-12345678ab13"


def test_takeover_scan_aborts_on_stale_lease_before_cleanup(started_cluster):
    """
    Regression for the lease fence inside the strict takeover scan: the replayed cleanup of a
    broken part (`renameToDetached` in `loadNewlyAppearedParts` with `strict_takeover`) runs on
    the heartbeat thread, which cannot renew the lease meanwhile, so scanning/loading earlier
    parts can outlast `leader_election_session_timeout`. A stale leader must abort the takeover
    before mutating shared storage instead of detaching/removing parts another node may already
    own.

    The `merge_tree_leader_election_stale_lease_during_takeover_scan` failpoint simulates
    exactly that: the lease check right before the replayed detach/remove reports stale. The
    takeover must fail (the table stays read-only) and the broken part must stay untouched
    under its active name; once the failpoint is cleared, the next takeover attempt must
    detach it and enable writes.
    """
    ensure_node_up(node1)
    ensure_node_up(node2)
    failpoint = "merge_tree_leader_election_stale_lease_during_takeover_scan"
    table = "test_takeover_scan_fence"
    node1.query(
        f"""
        CREATE TABLE {table} UUID '{SHARED_UUID_TAKEOVER_SCAN}' (x UInt64)
        ENGINE = MergeTree ORDER BY x
        SETTINGS {TABLE_SETTINGS}
        """
    )
    try:
        wait_for_leader([node1], table_name=table)

        # Two separate parts; merges are stopped so the part we corrupt survives as-is.
        node1.query(f"SYSTEM STOP MERGES {table}")
        node1.query(f"INSERT INTO {table} VALUES (1), (2), (3)")
        node1.query(f"INSERT INTO {table} VALUES (4), (5)")

        part_name = node1.query(
            f"SELECT name FROM system.parts WHERE database = currentDatabase()"
            f" AND table = '{table}' AND active AND rows = 2"
        ).strip()
        assert part_name, "Could not find the two-row part to corrupt"

        # Release the table on node1 so node2 can take over the lease.
        node1.query(f"DETACH TABLE {table} PERMANENTLY")

        # Corrupt the part on shared storage, so node2's read-only pre-lease loading skips it
        # and its takeover scan is the first point that would detach it.
        key_prefix = find_part_object_key_prefix(SHARED_UUID_TAKEOVER_SCAN, part_name)
        assert key_prefix, f"Could not locate part {part_name} on the object storage"
        payload = b"broken by test_takeover_scan_aborts_on_stale_lease_before_cleanup"
        started_cluster.minio_client.put_object(
            started_cluster.minio_bucket,
            key_prefix + "checksums.txt",
            io.BytesIO(payload),
            len(payload),
        )

        # Enable the failpoint BEFORE the table exists on node2, so every takeover attempt
        # observes a stale lease at the cleanup fence — no race with the election task.
        node2.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
        try:
            node2.query(
                f"""
                ATTACH TABLE {table} UUID '{SHARED_UUID_TAKEOVER_SCAN}' (x UInt64)
                ENGINE = MergeTree ORDER BY x
                SETTINGS {TABLE_SETTINGS}
                """
            )

            # Every takeover attempt must abort at the fence: the table stays read-only and
            # reads keep working (the broken part is simply not loaded).
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                assert not is_leader(node2, table_name=table), (
                    "node2 enabled writes despite the stale lease at the takeover-scan fence"
                )
                assert (
                    node2.query(f"SELECT x FROM {table} WHERE x > 0 ORDER BY x").strip()
                    == "1\n2\n3"
                )
                time.sleep(2)

            # The aborted takeovers must not have mutated shared storage: no detached copy,
            # and the broken part still sits under its active name.
            detached = node2.query(
                f"SELECT name FROM system.detached_parts WHERE database = currentDatabase()"
                f" AND table = '{table}'"
            ).strip()
            assert detached == "", (
                f"The aborted takeover still detached parts on shared storage: {detached!r}"
            )
            assert find_part_object_key_prefix(SHARED_UUID_TAKEOVER_SCAN, part_name), (
                "The aborted takeover removed or renamed the broken part on shared storage"
            )
        finally:
            node2.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")

        # With the failpoint cleared, the next takeover attempt replays the skipped cleanup:
        # the broken part is detached and writes are enabled.
        wait_for_leader([node2], table_name=table)
        assert (
            node2.query(f"SELECT x FROM {table} WHERE x > 0 ORDER BY x").strip()
            == "1\n2\n3"
        )
        detached = node2.query(
            f"SELECT name FROM system.detached_parts WHERE database = currentDatabase()"
            f" AND table = '{table}'"
        ).strip()
        assert "broken-on-start" in detached, (
            f"Expected a broken-on-start detached part, got: {detached!r}"
        )
    finally:
        try:
            node2.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        except Exception:
            pass
        try:
            node2.query(f"DROP TABLE IF EXISTS {table} SYNC")
        except Exception:
            pass
        try:
            node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
        except Exception:
            pass


SHARED_UUID_DETACHED_DDL = "12345678-abcd-abcd-abcd-12345678ab14"


def test_detached_ddl_rejected_on_stale_lease(started_cluster):
    """
    Regression for per-operation lease fencing inside the detached-namespace helpers: the
    dispatcher-level admission check in `alterPartition` only fences command entry, but
    `DROP DETACHED` deletes shared `detached/` directories and `ATTACH PARTITION` renames them
    (`attaching_`/`ignored_`/`inactive_`) and strips `txn_version.txt*` BEFORE the commit-time
    epoch fence. A node whose lease goes stale mid-command must stop mutating the shared
    `detached/` namespace and roll temporary renames back.

    The `merge_tree_leader_election_stale_lease_detached_ddl` failpoint makes the per-operation
    lease check report a stale lease, so both commands must fail leaving the detached set intact.
    """
    ensure_node_up(node1)
    failpoint = "merge_tree_leader_election_stale_lease_detached_ddl"
    table = "test_detached_ddl_fence"
    try:
        node1.query(
            f"""
            CREATE TABLE {table} UUID '{SHARED_UUID_DETACHED_DDL}' (x UInt64)
            ENGINE = MergeTree PARTITION BY x % 2 ORDER BY x
            SETTINGS {TABLE_SETTINGS}
            """
        )
        wait_for_leader([node1], table_name=table)

        node1.query(f"INSERT INTO {table} VALUES (1), (2), (3), (4)")
        node1.query(f"ALTER TABLE {table} DETACH PARTITION 1")
        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 2

        def detached_names():
            return sorted(
                node1.query(
                    f"SELECT name FROM system.detached_parts"
                    f" WHERE database = currentDatabase() AND table = '{table}'"
                )
                .strip()
                .splitlines()
            )

        detached_before = detached_names()
        assert detached_before, "DETACH PARTITION did not produce detached parts"

        node1.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
        try:
            with pytest.raises(Exception, match="possibly stale leader"):
                node1.query(
                    f"ALTER TABLE {table} DROP DETACHED PARTITION 1",
                    settings={"allow_drop_detached": 1},
                )
            assert detached_names() == detached_before, (
                "The rejected DROP DETACHED did not leave the detached set intact"
                " (a temporary deleting_ rename was not rolled back, or a directory was deleted)"
            )

            with pytest.raises(Exception, match="possibly stale leader"):
                node1.query(f"ALTER TABLE {table} ATTACH PARTITION 1")
            assert detached_names() == detached_before, (
                "The rejected ATTACH did not leave the detached set intact"
                " (a temporary attaching_ rename was not rolled back)"
            )
            assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 2
        finally:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")

        # With the failpoint cleared the same commands succeed.
        node1.query(f"ALTER TABLE {table} ATTACH PARTITION 1")
        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 4
        node1.query(f"ALTER TABLE {table} DETACH PARTITION 1")
        node1.query(
            f"ALTER TABLE {table} DROP DETACHED PARTITION 1",
            settings={"allow_drop_detached": 1},
        )
        assert detached_names() == []
    finally:
        try:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        except Exception:
            pass
        try:
            node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
        except Exception:
            pass


SHARED_UUID_MID_BATCH_RENAME = "12345678-abcd-abcd-abcd-12345678ab15"


def test_batch_publish_undone_when_lease_goes_stale_mid_rename(started_cluster):
    """
    Regression for the per-rename fence inside `MergeTreeData::Transaction::renameParts`:
    partition DDL and `MOVE PARTITION TO TABLE` publish a whole BATCH of parts, one rename at a
    time. Fencing only the start of the batch is not enough — if the lease expires after the
    first rename, the remaining parts are still published under their persistent names and only
    `commit` notices, by which time the batch can no longer be undone. A new leader would then
    load the covering empty parts of a `TRUNCATE` that failed, and the data would silently
    disappear.

    The `merge_tree_leader_election_stale_lease_mid_batch_rename` failpoint rejects the batch
    exactly after its first part has been published, so the undo of the already published
    renames is exercised. Reloading the table from shared storage afterwards must still see all
    the data: nothing of the aborted `TRUNCATE` may be left under a persistent name.
    """
    ensure_node_up(node1)
    failpoint = "merge_tree_leader_election_stale_lease_mid_batch_rename"
    table = "test_mid_batch_rename_fence"
    try:
        node1.query(
            f"""
            CREATE TABLE {table} UUID '{SHARED_UUID_MID_BATCH_RENAME}' (x UInt64)
            ENGINE = MergeTree PARTITION BY x % 2 ORDER BY x
            SETTINGS {TABLE_SETTINGS}
            """
        )
        wait_for_leader([node1], table_name=table)

        # Two partitions, so `TRUNCATE` publishes a batch of two covering empty parts.
        node1.query(f"INSERT INTO {table} VALUES (1), (2), (3), (4)")
        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 4

        node1.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
        try:
            with pytest.raises(Exception, match="middle of publishing a batch"):
                node1.query(f"TRUNCATE TABLE {table}")

            assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 4, (
                "TRUNCATE was rejected but part of the batch still took effect"
            )
            active_empty = int(
                node1.query(
                    f"SELECT count() FROM system.parts WHERE database = currentDatabase()"
                    f" AND table = '{table}' AND active AND rows = 0"
                ).strip()
            )
            assert active_empty == 0, (
                "A covering empty part of the aborted TRUNCATE stayed active"
            )
        finally:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")

        # The decisive check: reload the part set from shared storage. If the first rename of the
        # aborted batch had not been undone, the covering empty part would be picked up here and
        # would outdate the data of its partition.
        node1.query(f"DETACH TABLE {table}")
        node1.query(f"ATTACH TABLE {table}")
        wait_for_leader([node1], table_name=table)
        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 4, (
            "A covering empty part of the aborted TRUNCATE was left on shared storage"
        )

        # With the failpoint cleared the same DDL succeeds.
        node1.query(f"TRUNCATE TABLE {table}")
        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 0
    finally:
        try:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        except Exception:
            pass
        try:
            node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
        except Exception:
            pass


SHARED_UUID_DETACHED_EPOCH = "12345678-abcd-abcd-abcd-12345678ab16"


def test_detached_ddl_rejected_on_stale_epoch(started_cluster):
    """
    Regression for epoch fencing (not just lease freshness) of the permanent `detached/`
    mutations. `DROP DETACHED` deletes shared directories and `ATTACH PARTITION` renames them
    (`ignored_`/`inactive_`) and strips `txn_version.txt*` before the commit-time epoch fence.
    Checking only `mayMutateSharedStorage()` there is not enough: a node that lost leadership and
    reacquired it as a NEW epoch has a fresh lease again, so the freshness check passes, the
    permanent detached-namespace changes are made, and only the later publish fence rejects the
    command — leaving the next leader with a detached set the operator never asked to change.

    The `merge_tree_leader_election_stale_epoch_before_commit` failpoint presents a stale
    admission epoch while the lease itself stays fresh, so it exercises exactly this window: both
    commands must be rejected with the epoch error and leave the detached set byte-identical.
    """
    ensure_node_up(node1)
    failpoint = "merge_tree_leader_election_stale_epoch_before_commit"
    table = "test_detached_epoch_fence"
    try:
        node1.query(
            f"""
            CREATE TABLE {table} UUID '{SHARED_UUID_DETACHED_EPOCH}' (x UInt64)
            ENGINE = MergeTree PARTITION BY x % 2 ORDER BY x
            SETTINGS {TABLE_SETTINGS}
            """
        )
        wait_for_leader([node1], table_name=table)

        node1.query(f"INSERT INTO {table} VALUES (1), (2), (3), (4)")
        node1.query(f"ALTER TABLE {table} DETACH PARTITION 1")
        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 2

        def detached_names():
            return sorted(
                node1.query(
                    f"SELECT name FROM system.detached_parts"
                    f" WHERE database = currentDatabase() AND table = '{table}'"
                )
                .strip()
                .splitlines()
            )

        detached_before = detached_names()
        assert detached_before, "DETACH PARTITION did not produce detached parts"

        node1.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
        try:
            with pytest.raises(Exception, match="Leadership epoch"):
                node1.query(
                    f"ALTER TABLE {table} DROP DETACHED PARTITION 1",
                    settings={"allow_drop_detached": 1},
                )
            assert detached_names() == detached_before, (
                "DROP DETACHED was admitted under an older leadership epoch and still deleted"
                " directories of the shared detached namespace"
            )

            with pytest.raises(Exception, match="Leadership epoch"):
                node1.query(f"ALTER TABLE {table} ATTACH PARTITION 1")
            assert detached_names() == detached_before, (
                "ATTACH PARTITION was admitted under an older leadership epoch and still"
                " renamed directories of the shared detached namespace"
            )
            assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 2
        finally:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")

        # With the failpoint cleared the same commands succeed on the current epoch.
        node1.query(f"ALTER TABLE {table} ATTACH PARTITION 1")
        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 4
        node1.query(f"ALTER TABLE {table} DETACH PARTITION 1")
        node1.query(
            f"ALTER TABLE {table} DROP DETACHED PARTITION 1",
            settings={"allow_drop_detached": 1},
        )
        assert detached_names() == []
    finally:
        try:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        except Exception:
            pass
        try:
            node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
        except Exception:
            pass


SHARED_UUID_ATTACH_FROM_BATCH = "12345678-abcd-abcd-abcd-12345678ab18"


def test_attach_partition_from_undone_when_lease_goes_stale_mid_rename(started_cluster):
    """
    Regression for the per-rename fence in `ATTACH`/`REPLACE PARTITION FROM`
    (`StorageMergeTree::replacePartitionFrom`): unlike the other partition commands, it used to
    publish every cloned part with `rename_in_transaction = false`, i.e. rename it to its
    persistent name immediately inside the loop, with the epoch fence checked only once before
    the loop. A lease lost after the first rename therefore left already published parts under
    persistent names, and the rollback of the rejected command could not take them back: it only
    sets `creation_csn = RolledBackCSN` in memory, which is deliberately not persisted, while
    `cloneAndLoadDataPart` had already written the non-transactional creation metadata on disk.
    The next leader would then load parts of a command that returned an exception to the client.

    The command now publishes through `Transaction::renameParts` with the fence armed, so the
    `merge_tree_leader_election_stale_lease_mid_batch_rename` failpoint (which rejects the batch
    right after its first part was published) must leave nothing behind — even after the part set
    is reloaded from shared storage.
    """
    ensure_node_up(node1)
    failpoint = "merge_tree_leader_election_stale_lease_mid_batch_rename"
    table = "test_attach_from_batch_fence"
    plain = "test_attach_from_batch_fence_src"
    try:
        node1.query(
            f"""
            CREATE TABLE {table} UUID '{SHARED_UUID_ATTACH_FROM_BATCH}' (x UInt64)
            ENGINE = MergeTree PARTITION BY x % 2 ORDER BY x
            SETTINGS {TABLE_SETTINGS}
            """
        )
        wait_for_leader([node1], table_name=table)
        node1.query(f"INSERT INTO {table} VALUES (1), (3)")

        # The source is a plain table on another disk; two inserts into the same partition give
        # the batch of two parts that the fence has to publish one rename at a time.
        node1.query(
            f"CREATE TABLE {plain} (x UInt64) ENGINE = MergeTree PARTITION BY x % 2 ORDER BY x"
            " SETTINGS index_granularity = 8192"
        )
        node1.query(f"INSERT INTO {plain} VALUES (11)")
        node1.query(f"INSERT INTO {plain} VALUES (13)")
        assert int(node1.query(f"SELECT count() FROM system.parts WHERE database = currentDatabase()"
                               f" AND table = '{plain}' AND active").strip()) == 2

        node1.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
        try:
            with pytest.raises(Exception, match="middle of publishing a batch"):
                node1.query(f"ALTER TABLE {table} ATTACH PARTITION 1 FROM {plain}")
            # `WHERE x > 0` excludes the `x = 0` probe rows inserted by `wait_for_leader`.
            assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 2, (
                "ATTACH PARTITION FROM was rejected but part of the batch still took effect"
            )
        finally:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")

        # The decisive check: reload the part set from shared storage. A part of the aborted
        # command left under its persistent name would be picked up here.
        node1.query(f"DETACH TABLE {table}")
        node1.query(f"ATTACH TABLE {table}")
        wait_for_leader([node1], table_name=table)
        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 2, (
            "A part of the aborted ATTACH PARTITION FROM was left on shared storage"
        )

        # With the failpoint cleared the same command succeeds and publishes the whole batch.
        node1.query(f"ALTER TABLE {table} ATTACH PARTITION 1 FROM {plain}")
        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 4
    finally:
        try:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        except Exception:
            pass
        for name in (table, plain):
            try:
                node1.query(f"DROP TABLE IF EXISTS {name} SYNC")
            except Exception:
                pass


SHARED_UUID_ATTACH_DETACHED_BATCH = "12345678-abcd-abcd-abcd-12345678ab19"


def test_attach_partition_from_detached_undone_when_lease_goes_stale_mid_rename(started_cluster):
    """
    Regression for the per-rename fence in `ATTACH PARTITION` from the detached namespace
    (`StorageMergeTree::attachPartition`): it used to publish every loaded part through its own
    transaction, committing it immediately, with the epoch fence checked once per part. A lease
    lost after the first part committed left the command partially applied: the earlier parts
    stayed visible (and their detached sources consumed) even though the command returned an
    exception to the client.

    The command now stages the whole batch in a single transaction and publishes it through
    `Transaction::renameParts` with the fence armed, so the
    `merge_tree_leader_election_stale_lease_mid_batch_rename` failpoint (which rejects the batch
    right after its first part was published) must leave the table AND the detached namespace
    exactly as they were.
    """
    ensure_node_up(node1)
    failpoint = "merge_tree_leader_election_stale_lease_mid_batch_rename"
    table = "test_attach_detached_batch_fence"
    try:
        node1.query(
            f"""
            CREATE TABLE {table} UUID '{SHARED_UUID_ATTACH_DETACHED_BATCH}' (x UInt64)
            ENGINE = MergeTree PARTITION BY x % 2 ORDER BY x
            SETTINGS {TABLE_SETTINGS}
            """
        )
        wait_for_leader([node1], table_name=table)

        # Two parts in partition 1, then detach them: the ATTACH below is a batch of two.
        node1.query(f"INSERT INTO {table} VALUES (11)")
        node1.query(f"INSERT INTO {table} VALUES (13)")
        node1.query(f"ALTER TABLE {table} DETACH PARTITION 1")
        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 0

        def detached_names():
            return sorted(
                node1.query(
                    f"SELECT name FROM system.detached_parts"
                    f" WHERE database = currentDatabase() AND table = '{table}'"
                )
                .strip()
                .splitlines()
            )

        detached_before = detached_names()
        assert len(detached_before) == 2

        node1.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
        try:
            with pytest.raises(Exception, match="middle of publishing a batch"):
                node1.query(f"ALTER TABLE {table} ATTACH PARTITION 1")
            # `WHERE x > 0` excludes the `x = 0` probe rows inserted by `wait_for_leader`.
            assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 0, (
                "ATTACH PARTITION was rejected but part of the batch still took effect"
            )
            assert detached_names() == detached_before, (
                "A rejected ATTACH PARTITION consumed part of the detached namespace"
            )
        finally:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")

        # The decisive check: reload the part set from shared storage. A part of the aborted
        # command left under its persistent name would be picked up here.
        node1.query(f"DETACH TABLE {table}")
        node1.query(f"ATTACH TABLE {table}")
        wait_for_leader([node1], table_name=table)
        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 0, (
            "A part of the aborted ATTACH PARTITION was left on shared storage"
        )

        # With the failpoint cleared the same command succeeds and publishes the whole batch.
        node1.query(f"ALTER TABLE {table} ATTACH PARTITION 1")
        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 2
        assert detached_names() == []
    finally:
        try:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        except Exception:
            pass
        try:
            node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
        except Exception:
            pass


SHARED_UUID_MID_CLEANUP = "12345678-abcd-abcd-abcd-12345678ab20"


def test_cleanup_stops_when_lease_goes_stale_mid_removal(started_cluster):
    """
    Regression for the per-part freshness re-check in the filesystem cleanup
    (`clearPartsFromFilesystemImplMaybeInParallel`): the lease used to be checked once before
    handing the whole batch to the removal loops, so a lease that expired after the first few
    deletions let the stale leader keep deleting the tail of the batch from shared storage.

    The `merge_tree_leader_election_stale_lease_mid_cleanup` failpoint simulates the lease going
    stale right after the first part of the batch was removed: the removal must stop there and
    the remaining parts must be rolled back to the `Outdated` state (left to the current leader)
    instead of being deleted.
    """
    ensure_node_up(node1)
    failpoint = "merge_tree_leader_election_stale_lease_mid_cleanup"
    table = "test_mid_cleanup_fence"
    try:
        node1.query(
            f"""
            CREATE TABLE {table} UUID '{SHARED_UUID_MID_CLEANUP}' (x UInt64)
            ENGINE = MergeTree PARTITION BY x % 2 ORDER BY x
            SETTINGS {TABLE_SETTINGS}, old_parts_lifetime = 0,
                merge_tree_clear_old_parts_interval_seconds = 60
            """
        )
        wait_for_leader([node1], table_name=table)

        # Two parts in partition 1: `DROP PARTITION` marks both for immediate removal, so the
        # synchronous post-DDL cleanup gets a batch of two.
        node1.query(f"INSERT INTO {table} VALUES (11)")
        node1.query(f"INSERT INTO {table} VALUES (13)")

        node1.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
        try:
            # The DDL succeeds — the cleanup after it is best-effort — but its synchronous
            # cleanup must stop after the first removal and roll the rest back to Outdated.
            node1.query(f"ALTER TABLE {table} DROP PARTITION 1")
            assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 0

            assert node1.contains_in_log(
                "Simulated leadership loss in the middle of removing a batch of old parts"
            ), "The per-part freshness re-check never fired inside the removal batch"
            assert node1.contains_in_log(
                f"{SHARED_UUID_MID_CLEANUP}.*Failed to remove all parts, all count 2, removed 1"
            ), "The removal batch was not stopped after the first part"
        finally:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")

        # With the failpoint cleared the rolled-back parts are removed by a later pass.
        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 0
    finally:
        try:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        except Exception:
            pass
        try:
            node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
        except Exception:
            pass


SHARED_UUID_DETACHED_MUTATION = "12345678-abcd-abcd-abcd-12345678ab21"


def test_attach_partition_undoes_detached_changes_when_lease_goes_stale(started_cluster):
    """
    Regression for the rollback journal of the shared `detached/` namespace
    (`MergeTreeData::DetachedNamespaceRollback`): `ATTACH PARTITION` permanently renames
    directories to `ignored_` / `inactive_` and strips `txn_version.txt*` from the parts it is
    about to attach, all of it before the batch is published. Those changes are not staged in
    `PartsTemporaryRename`, so a lease that went stale in the middle of them used to leave the
    detached namespace altered by a command that returned an exception to the client.

    The `merge_tree_leader_election_stale_lease_mid_detached_mutation` failpoint rejects the
    command right after its first permanent change to `detached/`, so the journal must restore
    everything it recorded.

    A recordable change needs a detached part that is *covered* by another detached part (the
    `inactive_` rename): `txn_version.txt` never exists on these tables (the disks accepted by
    `leader_election` do not support transactions, so non-transactional version metadata is
    never persisted). A covered pair is built by detaching a merged part 1_a_b_1 and restarting
    the server: the restart resets the block-number counter to the maximum among *committed*
    parts (detached ones do not count), so the next insert into the partition reuses block `a`
    and produces 1_a_a_0, which 1_a_b_1 covers once both sit in `detached/`.
    """
    ensure_node_up(node1)
    failpoint = "merge_tree_leader_election_stale_lease_mid_detached_mutation"
    table = "test_detached_mutation_rollback"
    try:
        # `old_parts_lifetime = 0`: the source parts of the merge below must be gone from the
        # working set before the restart, or they would keep the block-number counter past the
        # detached merged part's range.
        node1.query(
            f"""
            CREATE TABLE {table} UUID '{SHARED_UUID_DETACHED_MUTATION}' (x UInt64)
            ENGINE = MergeTree PARTITION BY x % 2 ORDER BY x
            SETTINGS {TABLE_SETTINGS}, old_parts_lifetime = 0
            """
        )
        wait_for_leader([node1], table_name=table)

        node1.query(f"INSERT INTO {table} VALUES (11)")
        node1.query(f"INSERT INTO {table} VALUES (13)")
        node1.query(f"OPTIMIZE TABLE {table} PARTITION 1 FINAL")
        node1.query(f"ALTER TABLE {table} DETACH PARTITION 1")

        def detached_names():
            return sorted(
                node1.query(
                    f"SELECT name FROM system.detached_parts"
                    f" WHERE database = currentDatabase() AND table = '{table}'"
                )
                .strip()
                .splitlines()
            )

        detached_merged = detached_names()
        assert len(detached_merged) == 1 and not detached_merged[0].endswith("_0"), (
            f"Expected a single merged detached part, got {detached_merged}"
        )

        # Wait for the merge's source parts to be removed, then restart: the freshly started
        # server initializes the block-number counter from the parts that are still committed.
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            inactive = int(
                node1.query(
                    f"SELECT count() FROM system.parts"
                    f" WHERE database = currentDatabase() AND table = '{table}' AND NOT active"
                ).strip()
            )
            if inactive == 0:
                break
            time.sleep(0.5)
        assert inactive == 0, "Outdated parts were not cleaned up before the restart"

        node1.stop_clickhouse()
        node1.start_clickhouse()
        ensure_node_up(node1)

        # Retry the insert directly (instead of `wait_for_leader`, whose probe inserts would
        # consume the block number the covered part must reuse) until leadership is re-acquired.
        deadline = time.monotonic() + 60
        while True:
            try:
                node1.query(f"INSERT INTO {table} VALUES (15)")
                break
            except Exception:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(1)

        node1.query(f"ALTER TABLE {table} DETACH PARTITION 1")
        detached_before = detached_names()
        assert len(detached_before) == 2, f"Expected two detached parts, got {detached_before}"

        def block_range(name):
            parts = name.split("_")
            return int(parts[1]), int(parts[2])

        covered = [n for n in detached_before if n != detached_merged[0]]
        assert len(covered) == 1
        merged_min, merged_max = block_range(detached_merged[0])
        covered_min, covered_max = block_range(covered[0])
        assert merged_min <= covered_min and covered_max <= merged_max, (
            f"The new part {covered[0]} is not covered by {detached_merged[0]};"
            f" the restart did not reset the block-number counter as expected"
        )

        node1.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
        try:
            # The covered part gets its permanent `inactive_` rename first, arming the journal;
            # the fence in front of the next permanent change then rejects the command.
            with pytest.raises(Exception, match="middle of changing the detached namespace"):
                node1.query(f"ALTER TABLE {table} ATTACH PARTITION 1")
            # `WHERE x > 0` excludes the `x = 0` probe rows inserted by `wait_for_leader`.
            assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 0, (
                "ATTACH PARTITION was rejected but part of the batch still took effect"
            )
            assert detached_names() == detached_before, (
                "A rejected ATTACH PARTITION consumed part of the detached namespace"
            )
            assert node1.contains_in_log(
                "Renaming detached directory .* back to .* after a rejected command"
            ), "The inactive_ rename of the covered detached part was not undone"
        finally:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")

        # The restored detached parts are still attachable: the merged part carries the rows,
        # the covered part is shelved as `inactive_` (this time for good).
        node1.query(f"ALTER TABLE {table} ATTACH PARTITION 1")
        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 2
        assert detached_names() == ["inactive_" + covered[0]]
    finally:
        try:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        except Exception:
            pass
        try:
            node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
        except Exception:
            pass


def test_move_partition_to_disk_rejected_on_stale_leadership_epoch(started_cluster):
    """`MOVE PARTITION TO DISK` must fence its publish to the admission epoch."""
    ensure_node_up(node1)
    failpoint = "merge_tree_leader_election_stale_epoch_before_commit"
    table = "test_move_disk_epoch_fence"
    uuid = "12345678-abcd-abcd-abcd-12345678ab24"
    try:
        node1.query(
            f"""
            CREATE TABLE {table} UUID '{uuid}' (x UInt64)
            ENGINE = MergeTree PARTITION BY x % 2 ORDER BY x
            SETTINGS {TABLE_SETTINGS}
            """
        )
        wait_for_leader([node1], table_name=table)
        node1.query(f"INSERT INTO {table} VALUES (1), (3)")

        node1.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
        try:
            with pytest.raises(Exception, match="Leadership epoch"):
                node1.query(f"ALTER TABLE {table} MOVE PARTITION 1 TO DISK 's3_move'")
        finally:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")

        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 2
        assert node1.query(
            f"SELECT disk_name FROM system.parts WHERE database = currentDatabase() AND table = '{table}' AND active AND partition = '1'"
        ).strip() == "s3"
    finally:
        try:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        except Exception:
            pass
        try:
            node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
        except Exception:
            pass


SHARED_UUID_MOVE_SRC = "12345678-abcd-abcd-abcd-12345678ab22"
SHARED_UUID_MOVE_DEST = "12345678-abcd-abcd-abcd-12345678ab23"


def test_move_partition_dest_publish_undone_when_source_publish_fails(started_cluster):
    """
    Regression for the cross-transaction window of `MOVE PARTITION TO TABLE`: the command
    publishes through TWO transactions — the destination parts first, then the source-side
    covering empty parts. Each `renameParts` batch undoes its own renames when it fails in the
    middle, but when the destination batch has already fully published and the SOURCE side then
    fails, that completed batch used to stay visible under persistent names on shared storage:
    `rollbackPartsToTemporaryState` does not move directories back, and a "temporary" part whose
    directory no longer starts with `tmp` is never deleted. After a failover the next leader of
    the destination table would load parts from a command that returned an exception.

    The `merge_tree_leader_election_stale_lease_between_move_publishes` failpoint aborts the
    command exactly in that window, and `Transaction::undoPublishedRenames` must rename the
    published destination parts back to their temporary directories (where the regular rollback
    then deletes them).
    """
    ensure_node_up(node1)
    failpoint = "merge_tree_leader_election_stale_lease_between_move_publishes"
    src = "test_move_publish_src"
    dest = "test_move_publish_dest"
    try:
        for table, uuid in ((src, SHARED_UUID_MOVE_SRC), (dest, SHARED_UUID_MOVE_DEST)):
            node1.query(
                f"""
                CREATE TABLE {table} UUID '{uuid}' (x UInt64)
                ENGINE = MergeTree PARTITION BY x % 2 ORDER BY x
                SETTINGS {TABLE_SETTINGS}
                """
            )
            wait_for_leader([node1], table_name=table)

        # Two parts in the moved partition, so the destination publishes a batch of two.
        node1.query(f"INSERT INTO {src} VALUES (11)")
        node1.query(f"INSERT INTO {src} VALUES (13)")

        node1.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
        try:
            with pytest.raises(Exception, match="between the two publishes"):
                node1.query(f"ALTER TABLE {src} MOVE PARTITION 1 TO TABLE {dest}")

            # `WHERE x > 0` excludes the `x = 0` probe rows inserted by `wait_for_leader`.
            assert int(node1.query(f"SELECT count() FROM {src} WHERE x > 0").strip()) == 2, (
                "MOVE PARTITION was rejected but the source lost rows"
            )
            assert int(node1.query(f"SELECT count() FROM {dest} WHERE x > 0").strip()) == 0, (
                "MOVE PARTITION was rejected but rows are visible on the destination"
            )
        finally:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")

        # The decisive check: reload both part sets from shared storage. If the published
        # destination parts had not been renamed back and deleted, they would be picked up here.
        for table in (src, dest):
            node1.query(f"DETACH TABLE {table}")
            node1.query(f"ATTACH TABLE {table}")
            wait_for_leader([node1], table_name=table)
        assert int(node1.query(f"SELECT count() FROM {src} WHERE x > 0").strip()) == 2, (
            "The aborted MOVE PARTITION corrupted the source on shared storage"
        )
        assert int(node1.query(f"SELECT count() FROM {dest} WHERE x > 0").strip()) == 0, (
            "A published destination part of the aborted MOVE PARTITION was left on shared storage"
        )

        # With the failpoint cleared the same command succeeds.
        node1.query(f"ALTER TABLE {src} MOVE PARTITION 1 TO TABLE {dest}")
        assert int(node1.query(f"SELECT count() FROM {src} WHERE x > 0").strip()) == 0
        assert int(node1.query(f"SELECT count() FROM {dest} WHERE x > 0").strip()) == 2
    finally:
        try:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        except Exception:
            pass
        for table in (src, dest):
            try:
                node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
            except Exception:
                pass


SHARED_UUID_COMMIT_UNDO = "12345678-abcd-abcd-abcd-12345678ab24"


def test_insert_publish_undone_when_lease_goes_stale_before_commit(started_cluster):
    """
    Regression for the commit-time publish fence of `MergeTreeData::Transaction::commit`: the
    insert/merge/mutation paths rename the part to its persistent name BEFORE `commit`
    (`rename_in_transaction = false`), so a lease lost — or lost and reacquired — between the
    rename and the commit used to leave the part on shared storage while the client got an
    exception; the next leader would then load and activate a part of a failed `INSERT`. With
    the fence armed, `commit` re-checks the admission epoch and renames the published part back
    to its temporary directory when the check fails.

    The `merge_tree_leader_election_stale_lease_before_commit` failpoint deterministically fails
    the commit-time check after the publishing rename has happened, so the undo is exercised.
    Reloading the table from shared storage afterwards must not see the rejected part.
    """
    ensure_node_up(node1)
    failpoint = "merge_tree_leader_election_stale_lease_before_commit"
    table = "test_commit_undo_fence"
    try:
        create_table_on_first_node(node1, table, SHARED_UUID_COMMIT_UNDO)
        wait_for_leader([node1], table_name=table)

        node1.query(f"INSERT INTO {table} VALUES (1), (2), (3)")
        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 3

        node1.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
        try:
            with pytest.raises(Exception, match="between the publishing renames and the commit"):
                node1.query(f"INSERT INTO {table} VALUES (10)")

            assert (
                int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 3
            ), "The rejected INSERT still took effect locally"
        finally:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")

        # The decisive check: reload the part set from shared storage. If the publishing rename
        # of the rejected INSERT had not been undone, the part would be picked up here.
        node1.query(f"DETACH TABLE {table}")
        node1.query(f"ATTACH TABLE {table}")
        wait_for_leader([node1], table_name=table)
        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 3, (
            "A part of the rejected INSERT was left on shared storage"
        )

        # With the failpoint cleared the same INSERT succeeds and becomes visible.
        node1.query(f"INSERT INTO {table} VALUES (10)")
        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 4
    finally:
        try:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        except Exception:
            pass
        try:
            node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
        except Exception:
            pass


SHARED_UUID_REPLACE_RETIRE = "12345678-abcd-abcd-abcd-12345678ab25"


def test_replace_partition_old_parts_retired_durably(started_cluster):
    """
    Regression for the unfenced second phase of `REPLACE PARTITION FROM`
    (`StorageMergeTree::replacePartitionFrom`): the cloned parts were published through the fenced
    transaction, but the old destination partition was then retired via
    `removePartsInRangeFromWorkingSet`, which records the removal only in local memory — the
    non-transactional removal metadata write is deferred or skipped on these disks, so nothing on
    shared storage said the old parts were gone. Reloading the part set (restart, failover) before
    the lazy cleanup deleted the directories resurrected the replaced partition alongside the
    replacement, and a lease flip right after `transaction.commit` left the command reported as
    successful with the old partition still fully in place.

    The retirement is now published as covering empty parts in the SAME fenced transaction as the
    cloned parts, so it is (a) durable on shared storage and (b) undone together with the cloned
    parts when the lease goes stale in the middle of the batch.
    """
    ensure_node_up(node1)
    failpoint = "merge_tree_leader_election_stale_lease_mid_batch_rename"
    table = "test_replace_retire"
    plain = "test_replace_retire_src"
    try:
        node1.query(
            f"""
            CREATE TABLE {table} UUID '{SHARED_UUID_REPLACE_RETIRE}' (x UInt64)
            ENGINE = MergeTree PARTITION BY x % 2 ORDER BY x
            SETTINGS {TABLE_SETTINGS}
            """
        )
        wait_for_leader([node1], table_name=table)
        node1.query(f"INSERT INTO {table} VALUES (1), (3)")

        # Unlike ATTACH, REPLACE clones on the same disk, so the source must be on the same
        # storage policy (a regular table there, without `leader_election`).
        node1.query(
            f"CREATE TABLE {plain} (x UInt64) ENGINE = MergeTree PARTITION BY x % 2 ORDER BY x"
            " SETTINGS storage_policy = 's3'"
        )
        node1.query(f"INSERT INTO {plain} VALUES (11)")
        node1.query(f"INSERT INTO {plain} VALUES (13)")

        # A lease lost in the middle of the publish batch must undo BOTH halves of the command:
        # the cloned parts and the covering empty parts that retire the old partition.
        node1.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
        try:
            with pytest.raises(Exception, match="middle of publishing a batch"):
                node1.query(f"ALTER TABLE {table} REPLACE PARTITION 1 FROM {plain}")
            assert node1.query(
                f"SELECT x FROM {table} WHERE x > 0 ORDER BY x"
            ).split() == ["1", "3"], (
                "REPLACE PARTITION was rejected but the destination partition changed"
            )
        finally:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")

        # Reload the part set from shared storage: nothing of the rejected command may survive.
        node1.query(f"DETACH TABLE {table}")
        node1.query(f"ATTACH TABLE {table}")
        wait_for_leader([node1], table_name=table)
        assert node1.query(
            f"SELECT x FROM {table} WHERE x > 0 ORDER BY x"
        ).split() == ["1", "3"], (
            "A part of the rejected REPLACE PARTITION was left on shared storage"
        )

        # With the failpoint cleared the same command succeeds and replaces the partition.
        node1.query(f"ALTER TABLE {table} REPLACE PARTITION 1 FROM {plain}")
        assert node1.query(
            f"SELECT x FROM {table} WHERE x > 0 ORDER BY x"
        ).split() == ["11", "13"]

        # The decisive check: reload the part set from shared storage. Before the fix the removal
        # of the old parts existed only in this server's memory, so the replaced partition
        # resurrected here and the query returned 1, 3, 11, 13.
        node1.query(f"DETACH TABLE {table}")
        node1.query(f"ATTACH TABLE {table}")
        wait_for_leader([node1], table_name=table)
        assert node1.query(
            f"SELECT x FROM {table} WHERE x > 0 ORDER BY x"
        ).split() == ["11", "13"], (
            "The replaced partition resurrected after reloading from shared storage"
        )
    finally:
        try:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        except Exception:
            pass
        for name in (table, plain):
            try:
                node1.query(f"DROP TABLE IF EXISTS {name} SYNC")
            except Exception:
                pass


SHARED_UUID_CLEAR_EMPTY_MID_LOOP = "12345678-abcd-abcd-abcd-12345678ab26"


def test_clear_empty_parts_stops_when_lease_goes_stale_mid_loop(started_cluster):
    """
    Regression for the per-part freshness re-check inside `clearEmptyParts` (and
    `clearUnusedPatchParts`): the lease used to be checked only before entering the helper, so a
    lease that expired after the first empty part was dropped let the stale leader keep
    persisting removal metadata and dedup-log `DROP` records for the rest of the list while the
    new leader already owned the shared storage.

    The `merge_tree_leader_election_stale_lease_mid_clear_empty_parts` failpoint simulates the
    lease going stale right after the first empty part of the batch was dropped: the loop must
    stop there and leave the remaining empty parts to the current leader.
    """
    ensure_node_up(node1)
    failpoint = "merge_tree_leader_election_stale_lease_mid_clear_empty_parts"
    table = "test_clear_empty_mid_loop"
    try:
        node1.query(
            f"""
            CREATE TABLE {table} UUID '{SHARED_UUID_CLEAR_EMPTY_MID_LOOP}' (x UInt64)
            ENGINE = MergeTree ORDER BY x
            SETTINGS {TABLE_SETTINGS}, old_parts_lifetime = 0,
                     merge_tree_clear_old_parts_interval_seconds = 60,
                     cleanup_delay_period = 60, cleanup_delay_period_random_add = 0
            """
        )
        wait_for_leader([node1], table_name=table)

        node1.query(f"INSERT INTO {table} VALUES (1)")
        node1.query(f"INSERT INTO {table} VALUES (2)")

        # One covering empty part is committed per active part (the leader probe above may have
        # added parts of its own, so count them instead of hardcoding).
        parts_before = int(
            node1.query(
                f"SELECT count() FROM system.parts WHERE database = currentDatabase()"
                f" AND table = '{table}' AND active"
            ).strip()
        )
        assert parts_before >= 2

        node1.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
        try:
            # The DDL succeeds — the synchronous cleanup after it is best-effort — but its
            # `clearEmptyParts` pass must stop after the first drop.
            node1.query(f"TRUNCATE TABLE {table}")
            assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 0

            assert node1.contains_in_log(
                f"Stopping the removal of empty parts after 1 of {parts_before}"
            ), "The per-part freshness re-check never fired inside the empty-parts loop"

            # Exactly one empty part was dropped; the rest stay active for the current leader.
            active_empty = int(
                node1.query(
                    f"SELECT count() FROM system.parts WHERE database = currentDatabase()"
                    f" AND table = '{table}' AND active AND rows = 0"
                ).strip()
            )
            assert active_empty == parts_before - 1, (
                "The empty-parts loop kept dropping parts after the lease went stale"
            )
        finally:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")

        # The table stays truncated after the lease is fresh again.
        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 0
    finally:
        try:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        except Exception:
            pass
        try:
            node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
        except Exception:
            pass


SHARED_UUID_MERGE_TMP_COLLISION = "12345678-abcd-abcd-abcd-12345678ab27"


def test_merge_after_failover_does_not_collide_with_stale_tmp_dir(started_cluster):
    """
    Regression for the per-node scoping of the background-merge temporary directory names: the
    merged part name is deterministic, so `tmp_merge_<part>` used to be the same path on every
    node sharing the data path. A leader that died (or lost its lease) mid-merge left that
    directory on shared storage, and the new leader, selecting the same merge, collided with it
    (`DIRECTORY_ALREADY_EXISTS`) instead of making progress.

    The merge temp directory is now scoped with the same per-node token as the insert / partition
    DDL temp names (`tmp_merge_<token>_<part>`), so the new leader's merge must succeed. The
    `merge_task_pause_after_temporary_directory_created` failpoint holds the leader's merge at
    the point where its temporary directory exists on shared storage, and a hard kill leaves the
    directory behind.
    """
    ensure_node_up(node1)
    ensure_node_up(node2)
    failpoint = "merge_task_pause_after_temporary_directory_created"
    table = "test_merge_tmp_collision"
    try:
        node1.query(
            f"""
            CREATE TABLE {table} UUID '{SHARED_UUID_MERGE_TMP_COLLISION}' (x UInt64)
            ENGINE = MergeTree ORDER BY x
            SETTINGS {TABLE_SETTINGS}
            """
        )
        attach_table_on_second_node(node2, table_name=table, uuid=SHARED_UUID_MERGE_TMP_COLLISION)
        leader, followers = wait_for_leader([node1, node2], table_name=table)
        follower = followers[0]

        leader.query(f"INSERT INTO {table} VALUES (1)")
        leader.query(f"INSERT INTO {table} VALUES (2)")

        # Hold the merge on the leader right after its temporary directory was created on the
        # shared storage, then kill the node so the directory is left behind (a graceful failure
        # would remove it: temporary parts are deleted on destruction).
        leader.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
        optimize_error = []

        def run_optimize():
            try:
                leader.query(f"OPTIMIZE TABLE {table} FINAL")
            except Exception as e:
                optimize_error.append(e)

        optimize_thread = threading.Thread(target=run_optimize)
        optimize_thread.start()
        try:
            leader.query(f"SYSTEM WAIT FAILPOINT {failpoint} PAUSE", timeout=60)
            # The failpoint is global on this node, so a system-table merge could have been the
            # first to pause. Make sure OUR merge is the one holding a temporary directory (its
            # pause point is reached within moments of the merge registering).
            deadline = time.monotonic() + 60
            while (
                leader.query(
                    f"SELECT count() FROM system.merges WHERE table = '{table}'"
                ).strip()
                == "0"
            ):
                assert time.monotonic() < deadline, "The merge never started on the leader"
                time.sleep(0.5)
            time.sleep(2)
        finally:
            leader.stop_clickhouse(kill=True)
            optimize_thread.join()

        # The follower must take over (session_timeout = 5s) and be able to run the SAME merge —
        # its temporary directory name is scoped by its own node token, so the dead leader's
        # leftover directory does not collide with it.
        deadline = time.monotonic() + 60
        while True:
            try:
                follower.query(f"OPTIMIZE TABLE {table} FINAL", timeout=60)
                break
            except Exception as e:
                if "TABLE_IS_READ_ONLY" in str(e) and time.monotonic() < deadline:
                    time.sleep(1)
                    continue
                raise

        assert follower.query(
            f"SELECT x FROM {table} WHERE x > 0 ORDER BY x"
        ).split() == ["1", "2"]
        assert int(
            follower.query(
                f"SELECT count() FROM system.parts WHERE database = currentDatabase()"
                f" AND table = '{table}' AND active"
            ).strip()
        ) == 1, "OPTIMIZE FINAL on the new leader did not produce a single merged part"
    finally:
        for node in (node1, node2):
            try:
                ensure_node_up(node)
            except Exception:
                pass
        for node in (node1, node2):
            try:
                node.query(f"DROP TABLE IF EXISTS {table} SYNC")
            except Exception:
                pass


SHARED_UUID_DEDUP_FENCE = "12345678-abcd-abcd-abcd-12345678ab28"


def test_insert_dedup_no_stale_block_ids_when_lease_goes_stale_before_commit(
    started_cluster,
):
    """
    Regression for the shared deduplication log vs. the commit-time publish fence: the insert
    path used to persist the `ADD` records (`MergeTreeDeduplicationLog::addPart`) BEFORE the
    fenced `transaction.commit`, so an `INSERT` rejected by the fence left durable block ids on
    shared storage while its part was renamed back to a temporary directory. The next leader
    would load those block ids and silently deduplicate — i.e. drop — the client's retry of the
    failed `INSERT`. Now the insert path only checks for duplicates before publishing and writes
    the `ADD` records after the fenced commit succeeded, so a rejected publish leaves no block
    ids behind.
    """
    ensure_node_up(node1)
    failpoint = "merge_tree_leader_election_stale_lease_before_commit"
    table = "test_dedup_commit_fence"
    try:
        node1.query(
            f"""
            CREATE TABLE {table} UUID '{SHARED_UUID_DEDUP_FENCE}' (x UInt64)
            ENGINE = MergeTree ORDER BY x
            SETTINGS {TABLE_SETTINGS}, non_replicated_deduplication_window = 100
            """
        )
        wait_for_leader([node1], table_name=table)

        node1.query(f"INSERT INTO {table} VALUES (1), (2), (3)")
        # Sanity check that deduplication is active: the identical retry is dropped.
        node1.query(f"INSERT INTO {table} VALUES (1), (2), (3)")
        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 3

        node1.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
        try:
            with pytest.raises(Exception, match="between the publishing renames and the commit"):
                node1.query(f"INSERT INTO {table} VALUES (10)")
        finally:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")

        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 3

        # The decisive check: reload the dedup log and the part set from shared storage, as the
        # next leader would after a failover. The retry of the rejected INSERT must be accepted;
        # with the old order the block id of the failed INSERT was already durable and the retry
        # was silently dropped.
        node1.query(f"DETACH TABLE {table}")
        node1.query(f"ATTACH TABLE {table}")
        wait_for_leader([node1], table_name=table)

        node1.query(f"INSERT INTO {table} VALUES (10)")
        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 4, (
            "The retry of the fence-rejected INSERT was silently deduplicated: "
            "the failed INSERT left durable block ids in the shared deduplication log"
        )

        # And the successful insert's own ADD records must be durable: after another reload the
        # identical statement is deduplicated, not inserted twice.
        node1.query(f"DETACH TABLE {table}")
        node1.query(f"ATTACH TABLE {table}")
        wait_for_leader([node1], table_name=table)
        node1.query(f"INSERT INTO {table} VALUES (10)")
        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 4, (
            "The ADD records of the successful INSERT were not durable on shared storage"
        )
    finally:
        try:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        except Exception:
            pass
        try:
            node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
        except Exception:
            pass


SHARED_UUID_DEDUP_DROP = "12345678-abcd-abcd-abcd-12345678ab29"


def test_dedup_log_reconciled_when_lease_goes_stale_during_drop_records(started_cluster):
    """
    Regression for the shared deduplication log lagging behind a committed partition retirement:
    `TRUNCATE` (and `DROP`/`DETACH`/`REPLACE PARTITION`) retires partitions by committing
    covering empty parts and only then appends the matching `DROP` records to the deduplication
    log. `MergeTreeDeduplicationLog::dropPart` now re-checks the lease immediately before
    writing (a stale leader finalizing/rotating shared log files would clobber the next
    leader's log), so a lease that goes stale in that window leaves durable block ids for data
    that is already dropped — and an `INSERT` retrying that data would be silently deduplicated.
    The takeover reload now reconciles the loaded log against the part set: block ids covered by
    an active empty (covering) part belong to dropped data and are dropped from the log under
    the fresh lease.

    The `merge_tree_leader_election_stale_lease_dedup_log_write` failpoint deterministically
    fails the lease re-check inside the deduplication-log write, exactly the AI-review scenario
    of a heartbeat stalling between the caller's entry-point lease check and the log mutation.
    """
    ensure_node_up(node1)
    failpoint = "merge_tree_leader_election_stale_lease_dedup_log_write"
    table = "test_dedup_drop_reconcile"
    try:
        node1.query(
            f"""
            CREATE TABLE {table} UUID '{SHARED_UUID_DEDUP_DROP}' (x UInt64)
            ENGINE = MergeTree ORDER BY x
            SETTINGS {TABLE_SETTINGS}, non_replicated_deduplication_window = 100
            """
        )
        wait_for_leader([node1], table_name=table)

        node1.query(f"INSERT INTO {table} VALUES (1), (2), (3)")
        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 3

        # The retirement itself (covering empty parts) commits, then the deduplication-log
        # update fails closed on the simulated stale lease: the client gets an error while the
        # data is dropped and the block ids survive in the shared log.
        node1.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
        try:
            with pytest.raises(Exception, match="Refusing to write the deduplication log"):
                node1.query(f"TRUNCATE TABLE {table}")
        finally:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")

        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 0, (
            "TRUNCATE did not retire the partition"
        )

        # Reload from shared storage, as the next leader would after a failover. The takeover
        # reconciliation must drop the block ids covered by the active empty covering parts, so
        # re-inserting the truncated data is accepted instead of being silently deduplicated.
        node1.query(f"DETACH TABLE {table}")
        node1.query(f"ATTACH TABLE {table}")
        wait_for_leader([node1], table_name=table)

        node1.query(f"INSERT INTO {table} VALUES (1), (2), (3)")
        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 3, (
            "Re-inserting truncated data was silently deduplicated: the shared deduplication "
            "log still held block ids of dropped data after the takeover reload"
        )
    finally:
        try:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        except Exception:
            pass
        try:
            node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
        except Exception:
            pass


SHARED_UUID_CLEAR_EMPTY_COVERED = "12345678-abcd-abcd-abcd-12345678ab30"


def test_clear_empty_parts_keeps_tombstones_while_covered_parts_remain(started_cluster):
    """
    Regression for the covered-outdated re-check inside `clearEmptyParts`: an active empty
    covering part is the only durable tombstone for the outdated parts it covers, so it must
    not be retired while any covered part is still present (a part still held by a long-running
    reader, or a delete that rolled back after an I/O failure). `clearEmptyParts` used to
    snapshot all active empty parts and drop them unconditionally; after a restart or failover
    the surviving old parts would be loaded again, undoing the `TRUNCATE` that produced the
    empty parts.

    The `merge_tree_grab_old_parts_skip` failpoint simulates the cleanup round that cannot
    remove any covered outdated part yet (`TRUNCATE` marks them for immediate removal, so
    without the failpoint the synchronous post-DDL cleanup deletes them before
    `clearEmptyParts` runs and the covering parts legitimately stop covering).
    """
    ensure_node_up(node1)
    failpoint = "merge_tree_grab_old_parts_skip"
    table = "test_clear_empty_covered"
    try:
        # Long background-cleanup intervals: only the synchronous post-DDL cleanup runs
        # while the failpoint is enabled, so the state checked below is deterministic.
        node1.query(
            f"""
            CREATE TABLE {table} UUID '{SHARED_UUID_CLEAR_EMPTY_COVERED}' (x UInt64)
            ENGINE = MergeTree ORDER BY x
            SETTINGS {TABLE_SETTINGS},
                     merge_tree_clear_old_parts_interval_seconds = 60,
                     cleanup_delay_period = 60, cleanup_delay_period_random_add = 0
            """
        )
        wait_for_leader([node1], table_name=table)

        node1.query(f"INSERT INTO {table} VALUES (1)")
        node1.query(f"INSERT INTO {table} VALUES (2)")

        # A background merge of the empty covering parts would change the active-part count
        # this test asserts on; the tombstone invariant itself does not depend on merges.
        node1.query(f"SYSTEM STOP MERGES {table}")

        # One covering empty part is committed per active part (the leader probe above may
        # have added parts of its own, so count them instead of hardcoding).
        parts_before = int(
            node1.query(
                f"SELECT count() FROM system.parts WHERE database = currentDatabase()"
                f" AND table = '{table}' AND active"
            ).strip()
        )
        assert parts_before >= 2

        node1.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
        node1.query(f"TRUNCATE TABLE {table}")
        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 0

        # Every empty covering part must survive the synchronous post-DDL `clearEmptyParts`:
        # the covered outdated parts are still on disk, so retiring the tombstones now would
        # let the covered parts resurrect on the next reload.
        active_empty = int(
            node1.query(
                f"SELECT count() FROM system.parts WHERE database = currentDatabase()"
                f" AND table = '{table}' AND active AND rows = 0"
            ).strip()
        )
        assert active_empty == parts_before, (
            "clearEmptyParts retired an empty covering part while its covered outdated "
            "parts were still present"
        )
        assert node1.contains_in_log(
            "Not dropping empty part"
        ), "The covered-outdated re-check never fired inside clearEmptyParts"

        node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")

        # The tombstones are intact, so a full reload must still see the table truncated
        # (with the bug, the covered parts of a retired tombstone would be loaded again).
        node1.query(f"DETACH TABLE {table}")
        node1.query(f"ATTACH TABLE {table}")
        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 0, (
            "TRUNCATE was undone after a reload: covered outdated parts were resurrected"
        )
    finally:
        try:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        except Exception:
            pass
        try:
            node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
        except Exception:
            pass


SHARED_UUID_NO_KEEPER = "12345678-abcd-abcd-abcd-12345678ab31"


def test_leader_election_without_keeper(started_cluster):
    """
    The headline contract is active/standby failover WITHOUT ClickHouse Keeper: coordination
    happens through conditional writes on the S3 lease file only. Every other multi-node test
    in this suite runs on nodes that also have Keeper configured, so a hidden dependency on a
    Keeper-backed startup or cleanup path would go unnoticed there. This scenario runs the
    happy path and a failover on two nodes with no Keeper configured at all.
    """
    ensure_node_up(node4_no_keeper)
    ensure_node_up(node5_no_keeper)
    table = "test_no_keeper"
    try:
        create_table_on_first_node(node4_no_keeper, table, SHARED_UUID_NO_KEEPER)
        attach_table_on_second_node(node5_no_keeper, table, SHARED_UUID_NO_KEEPER)

        leader, followers = wait_for_leader(
            [node4_no_keeper, node5_no_keeper], table_name=table
        )
        follower = followers[0]

        leader.query(f"INSERT INTO {table} VALUES (1), (2), (3)")

        # The follower refreshes the parts list from shared storage (no Keeper involved).
        deadline = time.monotonic() + 60
        rows = ""
        while time.monotonic() < deadline:
            rows = follower.query(f"SELECT x FROM {table} WHERE x > 0 ORDER BY x").strip()
            if rows == "1\n2\n3":
                break
            time.sleep(1)
        assert rows == "1\n2\n3", (
            f"Follower without Keeper did not observe the leader's writes, got: {rows!r}"
        )

        # Failover: stop the leader, the follower must take over on its own.
        leader.stop_clickhouse()
        deadline = time.monotonic() + 60
        took_over = False
        while time.monotonic() < deadline:
            try:
                follower.query(f"INSERT INTO {table} VALUES (10)")
                took_over = True
                break
            except Exception as e:
                if "TABLE_IS_READ_ONLY" in str(e):
                    time.sleep(2)
                    continue
                raise
        assert took_over, "Follower without Keeper did not take over leadership"

        rows = follower.query(f"SELECT x FROM {table} WHERE x > 0 ORDER BY x").strip()
        assert rows == "1\n2\n3\n10", (
            f"New leader without Keeper lost rows across the failover, got: {rows!r}"
        )

        leader.start_clickhouse()

        # The restarted original leader must rejoin as a follower. Retry until its
        # table is loaded, but reject a single successful write because the new
        # leader still owns the lease.
        deadline = time.monotonic() + 60
        old_leader_is_readonly = False
        while time.monotonic() < deadline:
            try:
                leader.query(f"INSERT INTO {table} VALUES (999)")
            except Exception as e:
                if "TABLE_IS_READ_ONLY" in str(e):
                    old_leader_is_readonly = True
                    break
                time.sleep(1)
                continue
            raise AssertionError(
                "Restarted old leader without Keeper accepted a write while the "
                "new leader holds the lease"
            )

        assert old_leader_is_readonly, (
            "Restarted old leader without Keeper did not become read-only"
        )

        # Rejoining as a follower must also refresh the row written after
        # failover, without relying on ClickHouse Keeper.
        expected = "1\n2\n3\n10"
        deadline = time.monotonic() + 60
        rows = ""
        while time.monotonic() < deadline:
            rows = leader.query(
                f"SELECT x FROM {table} WHERE x > 0 ORDER BY x"
            ).strip()
            if rows == expected:
                break
            time.sleep(1)
        assert rows == expected, (
            "Restarted old leader without Keeper did not refresh the full "
            f"post-failover row set, got: {rows!r}"
        )
    finally:
        ensure_node_up(node4_no_keeper)
        for node in (node4_no_keeper, node5_no_keeper):
            try:
                node.query(f"DROP TABLE IF EXISTS {table} SYNC")
            except Exception:
                pass


SHARED_UUID_FOLLOWER_DROP = "12345678-abcd-abcd-abcd-12345678ab32"


def test_follower_drop_table_keeps_shared_data(started_cluster):
    """
    The documented contract is that `DROP TABLE` on a follower is a local-metadata-only
    operation: it must never touch the shared object-storage prefix, because the follower
    cannot prove the data is not owned by a live leader. This exercises the destructive
    path directly: the follower drops the table, then the leader must still be able to read
    and write the data, the part objects must still be present in S3, and a re-attached
    follower must see the same rows.
    """
    ensure_node_up(node1)
    ensure_node_up(node2)
    table = "test_follower_drop"
    try:
        create_table_on_first_node(node1, table, SHARED_UUID_FOLLOWER_DROP)
        attach_table_on_second_node(node2, table, SHARED_UUID_FOLLOWER_DROP)

        leader, followers = wait_for_leader([node1, node2], table_name=table)
        follower = followers[0]

        leader.query(f"INSERT INTO {table} VALUES (1), (2), (3)")

        # Pick a real committed part and locate its S3 prefix so the check below is
        # anchored to the shared data, not only to query results served from caches.
        part_name = leader.query(
            f"SELECT name FROM system.parts WHERE database = currentDatabase()"
            f" AND table = '{table}' AND active AND rows > 1 LIMIT 1"
        ).strip()
        assert part_name
        part_prefix = find_part_object_key_prefix(SHARED_UUID_FOLLOWER_DROP, part_name)
        assert part_prefix, "Could not locate the part's S3 prefix before the drop"

        follower.query(f"DROP TABLE {table} SYNC")

        # The shared part objects must still be in the bucket after the follower's drop.
        objects_after_drop = list(
            cluster.minio_client.list_objects(
                cluster.minio_bucket, part_prefix, recursive=True
            )
        )
        assert objects_after_drop, (
            "DROP TABLE on the follower removed shared S3 data owned by the leader"
        )

        # The leader is untouched: it still reads the data and accepts writes.
        rows = leader.query(f"SELECT x FROM {table} WHERE x > 0 ORDER BY x").strip()
        assert rows == "1\n2\n3", (
            f"Leader lost data after a follower-side DROP TABLE, got: {rows!r}"
        )
        leader.query(f"INSERT INTO {table} VALUES (10)")

        # A re-attached follower sees the same (and the new) rows.
        attach_table_on_second_node(follower, table, SHARED_UUID_FOLLOWER_DROP)
        expected = "1\n2\n3\n10"
        deadline = time.monotonic() + 60
        rows = ""
        while time.monotonic() < deadline:
            rows = follower.query(f"SELECT x FROM {table} WHERE x > 0 ORDER BY x").strip()
            if rows == expected:
                break
            time.sleep(1)
        assert rows == expected, (
            f"Re-attached follower does not see the shared data, got: {rows!r}"
        )
    finally:
        for node in (node1, node2):
            try:
                node.query(f"DROP TABLE IF EXISTS {table} SYNC")
            except Exception:
                pass


SHARED_UUID_LEADER_DROP = "12345678-abcd-abcd-abcd-12345678ab33"


def test_leader_drop_table_keeps_shared_data(started_cluster):
    """
    Mirror of `test_follower_drop_table_keeps_shared_data` for the leader side: the documented
    contract is that `DROP TABLE` is a local-metadata-only operation on the leader too — the
    shared prefix may still be attached on other nodes, and there is no shared refcount to
    prove otherwise. This PR rewires both `DatabaseOnDisk::dropTable` and
    `DatabaseCatalog::dropTableFinally` for that behavior, so a leader-side cleanup regression
    must not merge green: the elected leader drops the table, the shared `S3` prefix must
    survive, the surviving follower must take over leadership and read/write the data, and a
    freshly re-attached peer must see the original rows.
    """
    ensure_node_up(node1)
    ensure_node_up(node2)
    table = "test_leader_drop"
    try:
        create_table_on_first_node(node1, table, SHARED_UUID_LEADER_DROP)
        attach_table_on_second_node(node2, table, SHARED_UUID_LEADER_DROP)

        leader, followers = wait_for_leader([node1, node2], table_name=table)
        follower = followers[0]

        leader.query(f"INSERT INTO {table} VALUES (1), (2), (3)")

        # Pick a real committed part and locate its S3 prefix so the check below is
        # anchored to the shared data, not only to query results served from caches.
        part_name = leader.query(
            f"SELECT name FROM system.parts WHERE database = currentDatabase()"
            f" AND table = '{table}' AND active AND rows > 1 LIMIT 1"
        ).strip()
        assert part_name
        part_prefix = find_part_object_key_prefix(SHARED_UUID_LEADER_DROP, part_name)
        assert part_prefix, "Could not locate the part's S3 prefix before the drop"

        leader.query(f"DROP TABLE {table} SYNC")

        # The shared part objects must still be in the bucket after the leader's drop.
        objects_after_drop = list(
            cluster.minio_client.list_objects(
                cluster.minio_bucket, part_prefix, recursive=True
            )
        )
        assert objects_after_drop, (
            "DROP TABLE on the leader removed shared S3 data still attached on a follower"
        )

        # The follower takes over leadership once the dropped leader's lease expires,
        # then it must read the shared data and accept writes.
        new_leader, _ = wait_for_leader([follower], table_name=table)
        rows = new_leader.query(f"SELECT x FROM {table} WHERE x > 0 ORDER BY x").strip()
        assert rows == "1\n2\n3", (
            f"Follower lost data after a leader-side DROP TABLE, got: {rows!r}"
        )
        new_leader.query(f"INSERT INTO {table} VALUES (10)")

        # A freshly attached peer (the former leader) sees the same (and the new) rows.
        attach_table_on_second_node(leader, table, SHARED_UUID_LEADER_DROP)
        expected = "1\n2\n3\n10"
        deadline = time.monotonic() + 60
        rows = ""
        while time.monotonic() < deadline:
            rows = leader.query(f"SELECT x FROM {table} WHERE x > 0 ORDER BY x").strip()
            if rows == expected:
                break
            time.sleep(1)
        assert rows == expected, (
            f"Re-attached peer does not see the shared data after a leader-side drop, got: {rows!r}"
        )
    finally:
        for node in (node1, node2):
            try:
                node.query(f"DROP TABLE IF EXISTS {table} SYNC")
            except Exception:
                pass


SHARED_UUID_DEDUP_MID_BATCH = "12345678-abcd-abcd-abcd-12345678ab34"


def test_dedup_log_write_fenced_per_record_mid_batch(started_cluster):
    """
    Regression for the per-record lease fence in `MergeTreeDeduplicationLog::dropPart`: the
    lease used to be checked only once per call, but on the `S3` path (no append support)
    `rotateAndDropIfNeeded` finalizes/rotates a whole log file after every record, so dropping
    a covering part with many covered block ids can run past `leader_election_session_timeout`
    and keep rewriting the shared log after another node has taken over.

    The `merge_tree_leader_election_stale_lease_dedup_log_mid_batch` failpoint fails the lease
    re-check only once at least one record of the batch has been written: with the old
    once-per-call fence the `TRUNCATE` below would succeed, with the per-record fence it must
    stop mid-batch. The takeover reload then reconciles the partially written log against the
    part set, so re-inserting the truncated data is accepted.
    """
    ensure_node_up(node1)
    failpoint = "merge_tree_leader_election_stale_lease_dedup_log_mid_batch"
    table = "test_dedup_mid_batch"
    try:
        node1.query(
            f"""
            CREATE TABLE {table} UUID '{SHARED_UUID_DEDUP_MID_BATCH}' (x UInt64)
            ENGINE = MergeTree ORDER BY x
            SETTINGS {TABLE_SETTINGS}, non_replicated_deduplication_window = 100
            """
        )
        wait_for_leader([node1], table_name=table)

        # Three separate inserts produce three distinct block ids in the deduplication log
        # (plus the leader probe's block); merging them into one covering part makes the
        # later `dropPart` call write several records in a single batch.
        node1.query(f"INSERT INTO {table} VALUES (1)")
        node1.query(f"INSERT INTO {table} VALUES (2)")
        node1.query(f"INSERT INTO {table} VALUES (3)")
        node1.query(f"OPTIMIZE TABLE {table} FINAL")
        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 3

        # The retirement itself (covering empty parts) commits, then the deduplication-log
        # batch stops on the simulated mid-batch stale lease: the client gets an error while
        # the data is dropped and some block ids survive in the shared log.
        node1.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
        try:
            with pytest.raises(Exception, match="Refusing to write the deduplication log"):
                node1.query(f"TRUNCATE TABLE {table}")
        finally:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")

        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 0, (
            "TRUNCATE did not retire the partition"
        )

        # Reload from shared storage, as the next leader would after a failover. The takeover
        # reconciliation must drop the block ids of the partially processed batch, so
        # re-inserting the truncated data is accepted instead of being silently deduplicated.
        node1.query(f"DETACH TABLE {table}")
        node1.query(f"ATTACH TABLE {table}")
        wait_for_leader([node1], table_name=table)

        node1.query(f"INSERT INTO {table} VALUES (1), (2), (3)")
        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 3, (
            "Re-inserting truncated data was silently deduplicated: the shared deduplication "
            "log still held block ids of dropped data after the mid-batch fence stop"
        )
    finally:
        try:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        except Exception:
            pass
        try:
            node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
        except Exception:
            pass


SHARED_UUID_DEDUP_BEFORE_ROTATE = "12345678-abcd-abcd-abcd-12345678ab35"


def test_dedup_log_rotation_fenced_after_record(started_cluster):
    """
    Regression for the lease fence in front of the per-record rotation in
    `MergeTreeDeduplicationLog::dropPart`. Writing the `DROP` record and rotating the log are two
    separate mutations of the shared state: on the `S3` path (no append support)
    `rotateAndDropIfNeeded` finalizes the current numbered log file and opens the next one with
    `WriteMode::Rewrite`, so a lease that expires in the gap between the two used to leave a stale
    leader finalizing and creating log files of a sequence the next leader already owns.

    The `merge_tree_leader_election_stale_lease_dedup_log_before_rotate` failpoint fails the lease
    re-check only in that gap — after a record of the batch has been written and before the
    rotation that follows it. Without the fence in front of the rotation the `TRUNCATE` below
    succeeds (the failpoint is never consulted); with it the command must be rejected. The
    takeover reload then reconciles the partially written log against the part set, so
    re-inserting the truncated data is accepted instead of being silently deduplicated.
    """
    ensure_node_up(node1)
    failpoint = "merge_tree_leader_election_stale_lease_dedup_log_before_rotate"
    table = "test_dedup_before_rotate"
    try:
        node1.query(
            f"""
            CREATE TABLE {table} UUID '{SHARED_UUID_DEDUP_BEFORE_ROTATE}' (x UInt64)
            ENGINE = MergeTree ORDER BY x
            SETTINGS {TABLE_SETTINGS}, non_replicated_deduplication_window = 100
            """
        )
        wait_for_leader([node1], table_name=table)

        node1.query(f"INSERT INTO {table} VALUES (1)")
        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 1

        # The retirement itself (covering empty parts) commits, then the deduplication-log batch
        # stops on the simulated stale lease in the record-to-rotation gap: the client gets an
        # error while the data is dropped and the block id survives in the shared log.
        node1.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
        try:
            with pytest.raises(Exception, match="Refusing to write the deduplication log"):
                node1.query(f"TRUNCATE TABLE {table}")
        finally:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")

        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 0, (
            "TRUNCATE did not retire the partition"
        )

        # Reload from shared storage, as the next leader would after a failover. The takeover
        # reconciliation must drop the surviving block id, so re-inserting the truncated data is
        # accepted.
        node1.query(f"DETACH TABLE {table}")
        node1.query(f"ATTACH TABLE {table}")
        wait_for_leader([node1], table_name=table)

        node1.query(f"INSERT INTO {table} VALUES (1)")
        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 1, (
            "Re-inserting truncated data was silently deduplicated: the shared deduplication log "
            "still held the block id of dropped data after the pre-rotation fence stop"
        )
    finally:
        try:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        except Exception:
            pass
        try:
            node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
        except Exception:
            pass


SHARED_UUID_DETACH_CLONE_ROLLBACK = "12345678-abcd-abcd-abcd-12345678ab36"


def test_detach_clone_removed_when_covering_part_creation_fails(started_cluster):
    """
    Regression for the rollback scope of a rejected `DETACH PART` / `DETACH PARTITION`: the
    detached clone is written to shared `detached/` BEFORE the covering empty part is created and
    committed, and the rollback used to cover only the commit (`renameAndCommitEmptyParts`). A
    failure inside `createEmptyDataParts` (or `initCoverageWithNewEmptyParts`) therefore returned
    an error while leaving a durable, attachable `detached/<part>` copy behind — once the live
    part was later retired for real, a normal `ATTACH PARTITION` could re-import data from a
    `DETACH` that never committed.

    The `merge_tree_create_empty_part_inject_failure` failpoint fails exactly that window: after
    the clone is durable and before the covering empty part exists. The rollback must remove the
    clone in both entrypoints.
    """
    ensure_node_up(node1)
    failpoint = "merge_tree_create_empty_part_inject_failure"
    table = "test_detach_clone_rollback"

    def detached_count():
        return int(
            node1.query(
                f"SELECT count() FROM system.detached_parts"
                f" WHERE database = currentDatabase() AND table = '{table}'"
            ).strip()
        )

    try:
        node1.query(
            f"""
            CREATE TABLE {table} UUID '{SHARED_UUID_DETACH_CLONE_ROLLBACK}' (x UInt64)
            ENGINE = MergeTree PARTITION BY x % 2 ORDER BY x
            SETTINGS {TABLE_SETTINGS}
            """
        )
        wait_for_leader([node1], table_name=table)

        node1.query(f"INSERT INTO {table} VALUES (1), (2), (3), (4)")
        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 4

        part_name = node1.query(
            f"SELECT name FROM system.parts WHERE database = currentDatabase()"
            f" AND table = '{table}' AND active AND partition = '1' AND rows > 0 LIMIT 1"
        ).strip()
        assert part_name

        # `DETACH PART`: the clone is durable when the injected failure fires, so the rollback
        # must remove it and the part must stay active.
        node1.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
        try:
            with pytest.raises(Exception, match="Injected failure into MergeTreeData::createEmptyPart"):
                node1.query(f"ALTER TABLE {table} DETACH PART '{part_name}'")
            assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 4, (
                "DETACH PART failed before its commit but the part was still hidden"
            )
            assert detached_count() == 0, (
                "A DETACH PART rejected after writing its clone left the attachable copy behind"
            )

            # `DETACH PARTITION`: same fail-open window in the other entrypoint.
            with pytest.raises(Exception, match="Injected failure into MergeTreeData::createEmptyPart"):
                node1.query(f"ALTER TABLE {table} DETACH PARTITION 1")
            assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 4, (
                "DETACH PARTITION failed before its commit but the parts were still hidden"
            )
            assert detached_count() == 0, (
                "A DETACH PARTITION rejected after writing its clones left attachable copies behind"
            )
        finally:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")

        # The decisive behavior-level check: retire the partition for real, then ATTACH it.
        # Nothing may come back — with the fail-open window, the stale clones of the rejected
        # DETACHes above would resurrect the dropped rows here.
        node1.query(f"ALTER TABLE {table} DROP PARTITION 1")
        assert node1.query(f"SELECT x FROM {table} WHERE x > 0 ORDER BY x").strip() == "2\n4"
        node1.query(f"ALTER TABLE {table} ATTACH PARTITION 1")
        rows = node1.query(f"SELECT x FROM {table} WHERE x > 0 ORDER BY x").strip()
        assert rows == "2\n4", (
            f"ATTACH PARTITION resurrected data from a DETACH that never committed, got: {rows!r}"
        )
    finally:
        try:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        except Exception:
            pass
        try:
            node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
        except Exception:
            pass


SHARED_UUID_DEDUP_EPOCH_WRITER = "12345678-abcd-abcd-abcd-12345678ab37"


def test_stale_dedup_writer_discarded_on_leadership_loss(started_cluster):
    """
    Regression for the shared deduplication-log writer surviving a leadership epoch change. On
    the supported `S3`/`plain_rewritable` path every rotation leaves the log's `current_writer`
    pointing at the NEXT numbered `deduplication_log_N.txt` in `WriteMode::Rewrite` (append is
    unsupported). If the node then loses leadership while alive, that writer used to be carried
    along and finalized later — by `shutdown` (e.g. `DETACH TABLE`), the destructor, or the log
    reload on a future reacquisition — overwriting file `N` with its (empty or stale) buffer,
    even though the intervening leader had already written its own records into file `N`. That
    silently erased dedup history, so a retried `INSERT` could be applied twice.

    Now the leadership-loss callback discards (cancels) the writer, and `shutdown` fails closed
    by cancelling instead of finalizing when the lease is no longer fresh.

    The live demotion is real, not simulated: node1 is suspended (`SIGSTOP`/freezer) past
    `leader_election_session_timeout`, node2 claims the lease and writes a dedup record — into
    exactly the file number node1's stale writer still points at — and after resuming, node1
    observes the lost lease and transitions to follower.
    """
    ensure_node_up(node1)
    ensure_node_up(node2)
    table = "test_dedup_writer_epoch"

    def lost_leadership_count(node):
        return int(
            node.exec_in_container(
                [
                    "bash",
                    "-c",
                    "grep -c 'Lost leadership, stopping background write operations'"
                    " /var/log/clickhouse-server/clickhouse-server.log || true",
                ]
            ).strip()
        )

    try:
        node1.query(
            f"""
            CREATE TABLE {table} UUID '{SHARED_UUID_DEDUP_EPOCH_WRITER}' (x UInt64)
            ENGINE = MergeTree ORDER BY x
            SETTINGS {TABLE_SETTINGS}, non_replicated_deduplication_window = 100
            """
        )
        wait_for_leader([node1], table_name=table)

        # Leaves node1's dedup-log writer open on the next numbered log file (every record
        # rotates on object storage without append support).
        node1.query(f"INSERT INTO {table} VALUES (1)")
        assert int(node1.query(f"SELECT count() FROM {table} WHERE x > 0").strip()) == 1

        # The attach must carry the deduplication window too: the helper's definition would
        # leave node2 with `non_replicated_deduplication_window = 0`, i.e. no deduplication at
        # all after its takeover, and the final assertion would fail for an unrelated reason.
        node2.query(
            f"""
            ATTACH TABLE {table} UUID '{SHARED_UUID_DEDUP_EPOCH_WRITER}' (x UInt64)
            ENGINE = MergeTree ORDER BY x
            SETTINGS {TABLE_SETTINGS}, non_replicated_deduplication_window = 100
            """
        )

        baseline_losses = lost_leadership_count(node1)

        # Freeze node1 past the session timeout: the lease expires while the process — and its
        # open dedup-log writer — stays alive. node2 claims leadership and writes a dedup
        # record into the file number node1's stale writer still points at.
        with cluster.pause_container("node1"):
            wait_for_leader([node2], table_name=table)
            node2.query(f"INSERT INTO {table} VALUES (2)")

        # node1 resumes, observes node2's lease and demotes itself; wait until the
        # leadership-loss callback (which discards the stale writer) has actually run.
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if lost_leadership_count(node1) > baseline_losses:
                break
            time.sleep(1)
        assert lost_leadership_count(node1) > baseline_losses, (
            "node1 did not demote itself after its lease expired"
        )

        # Sanity: node2's deduplication is active — the retry is suppressed by its in-memory
        # state even before any reload.
        node2.query(f"INSERT INTO {table} VALUES (2)")
        assert int(node2.query(f"SELECT count() FROM {table} WHERE x = 2").strip()) == 1, (
            "Deduplication is not active on node2 after its takeover"
        )

        # The trigger of the old clobber: shutting the table down on the demoted node used to
        # finalize the stale writer, overwriting the log file that now holds node2's record.
        node1.query(f"DETACH TABLE {table}")

        # The decisive check: node2 reloads the dedup log from shared storage (as any leader
        # does on takeover) and the retried INSERT must still be deduplicated. With the stale
        # writer finalized, the record of `(2)` was erased and the retry inserted a duplicate.
        node2.query(f"DETACH TABLE {table}")
        node2.query(f"ATTACH TABLE {table}")
        wait_for_leader([node2], table_name=table)

        node2.query(f"INSERT INTO {table} VALUES (2)")
        assert int(node2.query(f"SELECT count() FROM {table} WHERE x = 2").strip()) == 1, (
            "The stale leader's dedup-log writer clobbered the new leader's log file: "
            "the retried INSERT was applied twice after the reload"
        )
        # And the table still has exactly the two real rows.
        assert node2.query(f"SELECT x FROM {table} WHERE x > 0 ORDER BY x").strip() == "1\n2"
    finally:
        try:
            node1.query(f"ATTACH TABLE {table}")
        except Exception:
            pass
        for node in (node1, node2):
            try:
                node.query(f"DROP TABLE IF EXISTS {table} SYNC")
            except Exception:
                pass


SHARED_UUID_MOVE_COMMIT_SRC = "12345678-abcd-abcd-abcd-12345678ab38"
SHARED_UUID_MOVE_COMMIT_DEST = "12345678-abcd-abcd-abcd-12345678ab39"


def test_move_partition_not_split_when_source_commit_fails(started_cluster):
    """
    Regression for the two-commit window of `MOVE PARTITION TO TABLE`: the command publishes and
    commits through TWO transactions (destination first, then source). `Transaction::commit`
    enforces the leadership fence itself and undoes only its OWN published renames, so a source
    side rejected AFTER the destination had already committed used to leave the moved partition
    visible in the destination while it was still present in the source — data duplicated by a
    command that returned an exception.

    Both commit-time checks now run before either transaction commits
    (`validateCommitPreconditions`), and the
    `merge_tree_leader_election_stale_lease_between_move_commits` failpoint fires exactly in
    between them: the destination side has passed its check (and, with the old code, would have
    been committed already) and the source side is then rejected. The whole command must be
    undone — neither table may show the partition twice, including after both part sets are
    reloaded from shared storage.
    """
    ensure_node_up(node1)
    failpoint = "merge_tree_leader_election_stale_lease_between_move_commits"
    src = "test_move_commit_src"
    dest = "test_move_commit_dest"
    try:
        for table, uuid in (
            (src, SHARED_UUID_MOVE_COMMIT_SRC),
            (dest, SHARED_UUID_MOVE_COMMIT_DEST),
        ):
            node1.query(
                f"""
                CREATE TABLE {table} UUID '{uuid}' (x UInt64)
                ENGINE = MergeTree PARTITION BY x % 2 ORDER BY x
                SETTINGS {TABLE_SETTINGS}
                """
            )
            wait_for_leader([node1], table_name=table)

        node1.query(f"INSERT INTO {src} VALUES (11)")
        node1.query(f"INSERT INTO {src} VALUES (13)")

        node1.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
        try:
            with pytest.raises(Exception, match="between the two commits"):
                node1.query(f"ALTER TABLE {src} MOVE PARTITION 1 TO TABLE {dest}")

            # `WHERE x > 0` excludes the `x = 0` probe rows inserted by `wait_for_leader`.
            assert int(node1.query(f"SELECT count() FROM {src} WHERE x > 0").strip()) == 2, (
                "MOVE PARTITION was rejected but the source lost rows"
            )
            assert int(node1.query(f"SELECT count() FROM {dest} WHERE x > 0").strip()) == 0, (
                "MOVE PARTITION was rejected after the destination commit: the partition is "
                "visible in both tables"
            )
        finally:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")

        # The decisive check: reload both part sets from shared storage, as the next leader
        # would after a failover. A destination part committed by the aborted command would be
        # picked up here even if the in-memory view looked clean.
        for table in (src, dest):
            node1.query(f"DETACH TABLE {table}")
            node1.query(f"ATTACH TABLE {table}")
            wait_for_leader([node1], table_name=table)
        assert int(node1.query(f"SELECT count() FROM {src} WHERE x > 0").strip()) == 2, (
            "The aborted MOVE PARTITION corrupted the source on shared storage"
        )
        assert int(node1.query(f"SELECT count() FROM {dest} WHERE x > 0").strip()) == 0, (
            "A committed destination part of the aborted MOVE PARTITION was left on shared storage"
        )

        # With the failpoint cleared the same command succeeds and moves the rows exactly once.
        node1.query(f"ALTER TABLE {src} MOVE PARTITION 1 TO TABLE {dest}")
        assert int(node1.query(f"SELECT count() FROM {src} WHERE x > 0").strip()) == 0
        assert int(node1.query(f"SELECT count() FROM {dest} WHERE x > 0").strip()) == 2
    finally:
        try:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        except Exception:
            pass
        for table in (src, dest):
            try:
                node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
            except Exception:
                pass


SHARED_UUID_RESTORE_SRC = "12345678-abcd-abcd-abcd-12345678ab40"
SHARED_UUID_RESTORE_DST = "12345678-abcd-abcd-abcd-12345678ab41"


def test_restore_fenced_to_admission_epoch(started_cluster):
    """
    Regression for the `RESTORE` admission-epoch fence: the restore tasks copy every file of
    every part into `tmp_restore_*` directories under the table's shared prefix long after the
    command was admitted, and the final `attachRestoredParts` publishes them. All of those
    stages are fenced to the leadership epoch sampled at admission (carried through
    `RestoredPartsHolder`), so a lease lost during the restore — even if reacquired, i.e. under
    a NEW epoch — must abort the copy loop and must not publish anything.

    The `merge_tree_leader_election_stale_epoch_before_commit` failpoint fires inside
    `assertWritableLeaderAtEpoch`, deterministically simulating exactly that lose-and-reacquire
    epoch change between the restore's admission and its per-part fence.
    """
    ensure_node_up(node1)
    failpoint = "merge_tree_leader_election_stale_epoch_before_commit"
    src = "test_restore_src"
    dst = "test_restore_dst"
    backup_destination = (
        "S3('http://minio1:9001/root/backups/test_restore_fenced', "
        "'minio', 'ClickHouse_Minio_P@ssw0rd')"
    )
    # The destination is pre-created (and its leadership awaited) so that the restore's
    # admission check passes deterministically and the failpoint provably rejects at the
    # epoch fence, not at admission. The probe rows of `wait_for_leader` make the table
    # non-empty, hence `allow_non_empty_tables`; the pre-created definition has a different
    # UUID than the backed-up one, hence `allow_different_table_def`.
    restore_settings = "SETTINGS allow_non_empty_tables = true, allow_different_table_def = true"
    try:
        create_table_on_first_node(node1, src, SHARED_UUID_RESTORE_SRC)
        create_table_on_first_node(node1, dst, SHARED_UUID_RESTORE_DST)
        wait_for_leader([node1], table_name=src)
        wait_for_leader([node1], table_name=dst)

        node1.query(f"INSERT INTO {src} VALUES (1), (2), (3)")
        node1.query(f"BACKUP TABLE {src} TO {backup_destination}")

        node1.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
        try:
            rejected = False
            try:
                node1.query(
                    f"RESTORE TABLE {src} AS {dst} FROM {backup_destination} {restore_settings}"
                )
            except Exception as e:
                msg = str(e)
                assert "Leadership epoch" in msg or "stale lease" in msg, (
                    f"RESTORE was rejected, but not by the leadership-epoch fence: {msg}"
                )
                rejected = True
            assert rejected, "RESTORE under a stale leadership epoch should have been rejected"

            # The rejection must leave nothing published in the destination.
            assert int(node1.query(f"SELECT count() FROM {dst} WHERE x > 0").strip()) == 0, (
                "The rejected RESTORE published parts into the destination"
            )
        finally:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")

        # The decisive check: reload the destination's part set from shared storage, as the
        # next leader would after a failover. A part streamed or published by the aborted
        # restore would be picked up here even if the in-memory view looked clean.
        node1.query(f"DETACH TABLE {dst}")
        node1.query(f"ATTACH TABLE {dst}")
        wait_for_leader([node1], table_name=dst)
        assert int(node1.query(f"SELECT count() FROM {dst} WHERE x > 0").strip()) == 0, (
            "The aborted RESTORE left parts on the destination's shared storage"
        )

        # With the failpoint cleared the same RESTORE succeeds and the rows become visible.
        node1.query(f"RESTORE TABLE {src} AS {dst} FROM {backup_destination} {restore_settings}")
        assert int(node1.query(f"SELECT count() FROM {dst} WHERE x > 0").strip()) == 3
    finally:
        try:
            node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
        except Exception:
            pass
        for table in (src, dst):
            try:
                node1.query(f"DROP TABLE IF EXISTS {table} SYNC")
            except Exception:
                pass


SHARED_UUID_DETACH_STAGED = "12345678-abcd-abcd-abcd-12345678ab42"


def test_uncommitted_detach_clone_not_attachable_after_failover(started_cluster):
    """
    Regression for the visibility of the copy a `DETACH PART` / `DETACH PARTITION` writes before
    it commits. The copy used to be written straight to `detached/<part>`, which is SHARED with
    the other servers, while the only guard against attaching it was the writing node's own
    process-local `temporary_parts` set. A failover in the window between the copy and the commit
    of the covering empty part therefore left the new leader with an attachable copy of a `DETACH`
    that never happened: its `ATTACH PARTITION` consults its own `temporary_parts` set, sees
    nothing, and `fillNewPartNameAndResetLevel` republishes the copy under a fresh block number —
    duplicating rows that are still live, because the rejected `DETACH` rolled back.

    The copy is now staged in a process-scoped `tmp_detach_*` directory outside `detached/` and
    moved there only after the commit. The `merge_tree_leader_election_pause_after_detach_clone`
    failpoint holds the `DETACH` exactly in that window, and the leader is killed inside it, so
    the copy is durable on the shared storage while the `DETACH` is provably not committed.
    """
    ensure_node_up(node1)
    ensure_node_up(node2)
    failpoint = "merge_tree_leader_election_pause_after_detach_clone"
    table = "test_detach_clone_staged"

    def active_parts(node):
        # Only the detached partition is counted: it holds a single part, so a background merge
        # cannot change this number behind the assertions below.
        return int(
            node.query(
                f"SELECT count() FROM system.parts WHERE database = currentDatabase()"
                f" AND table = '{table}' AND active AND partition = '1'"
            ).strip()
        )

    try:
        node1.query(
            f"""
            CREATE TABLE {table} UUID '{SHARED_UUID_DETACH_STAGED}' (x UInt64)
            ENGINE = MergeTree PARTITION BY x % 2 ORDER BY x
            SETTINGS {TABLE_SETTINGS}
            """
        )
        node2.query(
            f"""
            ATTACH TABLE {table} UUID '{SHARED_UUID_DETACH_STAGED}' (x UInt64)
            ENGINE = MergeTree PARTITION BY x % 2 ORDER BY x
            SETTINGS {TABLE_SETTINGS}
            """
        )
        leader, followers = wait_for_leader([node1, node2], table_name=table)
        follower = followers[0]

        leader.query(f"INSERT INTO {table} VALUES (1), (2), (3), (4)")
        assert leader.query(f"SELECT x FROM {table} WHERE x > 0 ORDER BY x").split() == [
            "1", "2", "3", "4",
        ]

        leader.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
        detach_error = []

        def run_detach():
            try:
                leader.query(f"ALTER TABLE {table} DETACH PARTITION 1")
            except Exception as e:
                detach_error.append(e)

        detach_thread = threading.Thread(target=run_detach)
        detach_thread.start()
        try:
            leader.query(f"SYSTEM WAIT FAILPOINT {failpoint} PAUSE", timeout=60)
        finally:
            # Kill the leader inside the window: the copy of the part is durable on the shared
            # storage, the covering empty part was never committed, and no rollback can run.
            leader.stop_clickhouse(kill=True)
            detach_thread.join()

        # The follower takes over (session timeout 5 s) and reads the shared storage anew.
        wait_for_leader([follower], table_name=table)
        follower.query(f"DETACH TABLE {table}")
        follower.query(f"ATTACH TABLE {table}")
        wait_for_leader([follower], table_name=table)

        assert follower.query(f"SELECT x FROM {table} WHERE x > 0 ORDER BY x").split() == [
            "1", "2", "3", "4",
        ], "The killed leader's uncommitted DETACH changed the data of the new leader"
        assert int(
            follower.query(
                f"SELECT count() FROM system.detached_parts WHERE database = currentDatabase()"
                f" AND table = '{table}'"
            ).strip()
        ) == 0, "The copy of an uncommitted DETACH is visible in the shared detached/ namespace"

        # The decisive check: the new leader must not be able to attach the copy.
        parts_before = active_parts(follower)
        follower.query(f"ALTER TABLE {table} ATTACH PARTITION 1")
        rows = follower.query(f"SELECT x FROM {table} WHERE x > 0 ORDER BY x").split()
        assert rows == ["1", "2", "3", "4"], (
            f"ATTACH PARTITION on the new leader duplicated the rows of a DETACH that never "
            f"committed, got: {rows}"
        )
        assert active_parts(follower) == parts_before, (
            "ATTACH PARTITION on the new leader attached a part of a DETACH that never committed"
        )

        # And the same `DETACH PARTITION`, run to completion by the new leader, does publish its
        # copy: staging must not lose the detached data of a committed DETACH.
        follower.query(f"ALTER TABLE {table} DETACH PARTITION 1")
        assert follower.query(f"SELECT x FROM {table} WHERE x > 0 ORDER BY x").split() == ["2", "4"]
        follower.query(f"ALTER TABLE {table} ATTACH PARTITION 1")
        assert follower.query(f"SELECT x FROM {table} WHERE x > 0 ORDER BY x").split() == [
            "1", "2", "3", "4",
        ], "The detached copy of a committed DETACH could not be attached back"
    finally:
        for node in (node1, node2):
            try:
                ensure_node_up(node)
            except Exception:
                pass
        for node in (node1, node2):
            try:
                node.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
            except Exception:
                pass
            try:
                node.query(f"DROP TABLE IF EXISTS {table} SYNC")
            except Exception:
                pass


SHARED_UUID_VANISHED_PARTS = "12345678-abcd-abcd-abcd-12345678ab43"


def test_vanished_parts_retired_on_takeover(started_cluster):
    """
    Regression for the additive-only refresh path (`loadNewlyAppearedParts`): a replica used to
    learn about a retirement only through the covering empty part ("tombstone") the leader
    publishes for `TRUNCATE` / `DROP PARTITION` / `REPLACE PARTITION` / `MOVE PARTITION`. That
    tombstone is itself removed by `clearEmptyParts` once the parts it covers are gone from the
    shared storage, so a replica that did not refresh in that window missed the retirement
    entirely — it kept serving the dropped partition, and kept those parts active when it later
    took over as the leader.

    The refresh now also diffs the active set against the storage listing and retires the parts
    that vanished (`retirePartsVanishedFromStorage`), which is what makes the retirement converge
    without the tombstone. The same function serves the periodic follower refresh and the
    takeover scan; the takeover is exercised here because it is both the dangerous case (the
    stale replica becomes the writer) and the deterministic one (the scan is forced by the
    leadership change).

    The follower's periodic refresh is frozen with the `merge_tree_refresh_parts_skip` failpoint
    once it has loaded the parts, so that it provably keeps the pre-`DROP PARTITION` view while
    the leader publishes and then cleans up the tombstone. The takeover scan calls
    `loadNewlyAppearedParts` directly and is not affected by that failpoint.
    """
    ensure_node_up(node1)
    ensure_node_up(node2)
    failpoint = "merge_tree_refresh_parts_skip"
    table = "test_vanished_parts"
    columns = "(x UInt64) ENGINE = MergeTree PARTITION BY x % 2 ORDER BY x"

    def parts_of_dropped_partition(node):
        return int(
            node.query(
                f"SELECT count() FROM system.parts WHERE database = currentDatabase()"
                f" AND table = '{table}' AND active AND partition = '1'"
            ).strip()
        )

    try:
        # Aggressive cleanup on the leader: the tombstone must actually be removed from the
        # shared storage during the test, which is the situation the fix is about.
        node1.query(
            f"""
            CREATE TABLE {table} UUID '{SHARED_UUID_VANISHED_PARTS}' {columns}
            SETTINGS {TABLE_SETTINGS},
                     old_parts_lifetime = 0,
                     merge_tree_clear_old_parts_interval_seconds = 1,
                     cleanup_delay_period = 1, cleanup_delay_period_random_add = 0
            """
        )
        wait_for_leader([node1], table_name=table)
        node1.query(f"INSERT INTO {table} VALUES (1), (3)")
        node1.query(f"INSERT INTO {table} VALUES (2), (4)")

        node2.query(
            f"""
            ATTACH TABLE {table} UUID '{SHARED_UUID_VANISHED_PARTS}' {columns}
            SETTINGS {TABLE_SETTINGS}
            """
        )
        leader, followers = wait_for_leader([node1, node2], table_name=table)
        assert leader == node1, "node1 must stay the leader for this scenario"
        follower = followers[0]

        # The follower needs one periodic refresh to pick up the parts (its snapshot of the
        # `plain_rewritable` path map predates the table), and is frozen right afterwards.
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if follower.query(
                f"SELECT x FROM {table} WHERE x > 0 ORDER BY x"
            ).split() == ["1", "2", "3", "4"]:
                break
            time.sleep(1)
        assert follower.query(f"SELECT x FROM {table} WHERE x > 0 ORDER BY x").split() == [
            "1", "2", "3", "4",
        ], "The follower did not load the parts written by the leader"

        follower.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")

        node1.query(f"ALTER TABLE {table} DROP PARTITION 1")
        assert node1.query(f"SELECT x FROM {table} WHERE x > 0 ORDER BY x").split() == ["2", "4"]

        # Wait until nothing of the dropped partition is left on the leader: neither the dropped
        # parts nor the covering empty part that retired them. From this moment on, the shared
        # storage holds no record of the retirement at all.
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if parts_of_dropped_partition(node1) == 0:
                break
            time.sleep(1)
        assert parts_of_dropped_partition(node1) == 0, (
            "The covering empty part of DROP PARTITION was not cleaned up on the leader, "
            "so the scenario under test was not reached"
        )

        # Precondition: the dropped partition is still active in the follower's frozen view.
        # (Its data is gone from the shared storage, so the rows themselves are not readable
        # there any more — the point is that the follower still considers the part part of the
        # table, and would keep it after becoming the leader.)
        assert parts_of_dropped_partition(follower) == 1, (
            "The follower refreshed although its periodic refresh is disabled"
        )

        # The follower takes over with that stale view. The takeover scan must retire the parts
        # that are no longer on the shared storage; before the fix the new leader resurrected the
        # dropped partition and served it as the authoritative data.
        node1.stop_clickhouse(kill=True)
        wait_for_leader([follower], table_name=table)
        follower.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")

        rows = follower.query(f"SELECT x FROM {table} WHERE x > 0 ORDER BY x").split()
        assert rows == ["2", "4"], (
            f"The new leader resurrected the partition dropped by the previous leader, got: {rows}"
        )
        assert follower.contains_in_log("no longer exists on the shared storage"), (
            "The vanished-parts retirement never ran during the takeover scan"
        )

        # And the resurrection must not come back through a reload of the shared storage either.
        follower.query(f"DETACH TABLE {table}")
        follower.query(f"ATTACH TABLE {table}")
        wait_for_leader([follower], table_name=table)
        assert follower.query(f"SELECT x FROM {table} WHERE x > 0 ORDER BY x").split() == [
            "2", "4",
        ], "The dropped partition came back after reloading from the shared storage"
    finally:
        for node in (node1, node2):
            try:
                ensure_node_up(node)
            except Exception:
                pass
            try:
                node.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
            except Exception:
                pass
            try:
                node.query(f"DROP TABLE IF EXISTS {table} SYNC")
            except Exception:
                pass
