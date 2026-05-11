import threading
import time

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster, ZOOKEEPER_CONTAINERS
from helpers.network import PartitionManager
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/config.xml"],
    user_configs=["configs/users.xml"],
    with_zookeeper=True,
    stay_alive=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config.xml"],
    user_configs=["configs/users.xml"],
    with_zookeeper=True,
    stay_alive=True,
)
# Third node without the shared-named_scalars ZK path — used to assert that
# cluster DDL errors cleanly when the config is missing.
node_no_zk = cluster.add_instance(
    "node_no_zk",
    main_configs=["configs/config_no_zk_path.xml"],
    user_configs=["configs/users.xml"],
    with_zookeeper=True,
)

nodes = [node1, node2]


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _drop_all_shared_named_scalars(node):
    rows = node.query(
        "SELECT name FROM system.named_scalars WHERE kind = 'shared'"
    ).strip()
    for name in [r for r in rows.splitlines() if r]:
        node.query(f"DROP NAMED SCALAR IF EXISTS {name}")


@pytest.fixture
def cleanup(started_cluster):
    yield
    # Best-effort cleanup from every node that might see the entry;
    # propagation between tests should not leak state.
    for node in nodes:
        try:
            _drop_all_shared_named_scalars(node)
        except Exception:
            pass


# -------- Regression locks: already green after step 2 --------

def test_zk_config_required(started_cluster, cleanup):
    with pytest.raises(QueryRuntimeException) as exc:
        node_no_zk.query("CREATE SHARED NAMED SCALAR nope AS SELECT 1")
    err = str(exc.value)
    assert "named_scalar_definitions_zookeeper_path" in err
    # Round-15 split the error-code surface: the dedicated
    # SHARED_NAMED_SCALARS_NOT_CONFIGURED (code 768) replaces the generic
    # BAD_ARGUMENTS (36) for this path. Accept either to keep the test
    # backward-compatible with builds that still throw the old code.
    assert (
        "SHARED_NAMED_SCALARS_NOT_CONFIGURED" in err
        or "768" in err
        or "BAD_ARGUMENTS" in err
        or "36" in err
    )


def test_single_node_create_read_drop(started_cluster, cleanup):
    node1.query("CREATE SHARED NAMED SCALAR sn_foo AS SELECT toUInt64(7)")
    assert node1.query("SELECT getNamedScalar('sn_foo')").strip() == "7"
    node1.query("DROP NAMED SCALAR sn_foo")
    err = node1.query_and_get_error("SELECT getNamedScalar('sn_foo')")
    assert "NAMED_SCALAR_NOT_FOUND" in err


# -------- Step 3: coordinator + watches --------

def test_cross_node_discovery(started_cluster, cleanup):
    node1.query("CREATE SHARED NAMED SCALAR xn_foo AS SELECT toUInt64(42)")
    assert_eq_with_retry(
        node2,
        "SELECT getNamedScalar('xn_foo')",
        "42\n",
        retry_count=40,
        sleep_time=0.25,
    )


def test_cross_node_drop(started_cluster, cleanup):
    node1.query("CREATE SHARED NAMED SCALAR xd_foo AS SELECT toUInt64(1)")
    assert_eq_with_retry(node2, "SELECT getNamedScalar('xd_foo')", "1\n")

    node1.query("DROP NAMED SCALAR xd_foo")

    def no_longer_exists():
        err = node2.query_and_get_error("SELECT getNamedScalar('xd_foo')")
        return "NAMED_SCALAR_NOT_FOUND" in err

    deadline = time.time() + 10
    while time.time() < deadline:
        if no_longer_exists():
            return
        time.sleep(0.25)
    pytest.fail("node2 still sees replicated xd_foo after DROP on node1")


def test_restart_picks_up_existing(started_cluster, cleanup):
    node1.query("CREATE SHARED NAMED SCALAR rs_foo AS SELECT toUInt64(100)")
    assert_eq_with_retry(node2, "SELECT getNamedScalar('rs_foo')", "100\n")

    node2.stop_clickhouse()
    node2.start_clickhouse()

    # On boot, node2 must load from ZK — no watch event to rely on.
    assert_eq_with_retry(
        node2,
        "SELECT getNamedScalar('rs_foo')",
        "100\n",
        retry_count=40,
        sleep_time=0.25,
    )


def test_create_or_replace_propagates(started_cluster, cleanup):
    node1.query("CREATE SHARED NAMED SCALAR repl_v AS SELECT toUInt64(1)")
    assert_eq_with_retry(node2, "SELECT getNamedScalar('repl_v')", "1\n")

    node1.query("CREATE OR REPLACE SHARED NAMED SCALAR repl_v AS SELECT toUInt64(2)")
    assert_eq_with_retry(node2, "SELECT getNamedScalar('repl_v')", "2\n")


def test_system_table_sees_cluster_rows(started_cluster, cleanup):
    node1.query("CREATE SHARED NAMED SCALAR st_foo AS SELECT toUInt64(5)")
    assert_eq_with_retry(node2, "SELECT getNamedScalar('st_foo')", "5\n")

    for node in nodes:
        row = node.query(
            "SELECT name, kind, value, type FROM system.named_scalars "
            "WHERE name = 'st_foo' AND kind = 'shared'"
        ).strip()
        assert row == "st_foo\tshared\t5\tUInt64", f"{node.name}: {row!r}"


# -------- Step 4: leader election + REFRESH --------

def test_refresh_clause_accepted(started_cluster, cleanup):
    # After step 4 this must stop returning NOT_IMPLEMENTED.
    node1.query(
        "CREATE SHARED NAMED SCALAR rc_wm REFRESH EVERY 1 SECOND AS SELECT toUInt64(now())"
    )


def test_refresh_updates_value(started_cluster, cleanup):
    node1.query(
        "CREATE SHARED NAMED SCALAR rv_wm REFRESH EVERY 1 SECOND AS SELECT toUInt64(now())"
    )
    assert_eq_with_retry(
        node2,
        "SELECT getNamedScalar('rv_wm') > 0",
        "1\n",
        retry_count=40,
        sleep_time=0.25,
    )

    first = int(node1.query("SELECT getNamedScalar('rv_wm')").strip())
    time.sleep(3)
    later_n1 = int(node1.query("SELECT getNamedScalar('rv_wm')").strip())
    later_n2 = int(node2.query("SELECT getNamedScalar('rv_wm')").strip())
    assert later_n1 > first
    assert later_n2 > first


def test_refresh_single_leader_per_tick(started_cluster, cleanup):
    """Every tick exactly one node writes to ZK (ephemeral lock serialises writers).
    The winner may alternate between ticks — we just assert every observed
    hostname is one of the real cluster members."""
    node1.query(
        "CREATE SHARED NAMED SCALAR sl_wm REFRESH EVERY 1 SECOND AS SELECT toUInt64(now())"
    )
    assert_eq_with_retry(node2, "SELECT getNamedScalar('sl_wm') > 0", "1\n")

    valid_hosts = {node1.hostname, node2.hostname}
    samples = []
    for _ in range(8):
        host = node1.query(
            "SELECT last_refresh_hostname FROM system.named_scalars "
            "WHERE name = 'sl_wm' AND kind = 'shared'"
        ).strip()
        samples.append(host)
        time.sleep(1)

    assert all(h in valid_hosts for h in samples), (samples, valid_hosts)


def test_system_refresh_variable(started_cluster, cleanup):
    node1.query(
        "CREATE SHARED NAMED SCALAR sr_wm REFRESH EVERY 36500 DAYS AS SELECT toUInt64(now())"
    )
    assert_eq_with_retry(node2, "SELECT getNamedScalar('sr_wm') > 0", "1\n")
    first = int(node2.query("SELECT getNamedScalar('sr_wm')").strip())
    time.sleep(1)
    node1.query("SYSTEM REFRESH NAMED SCALAR sr_wm")
    assert_eq_with_retry(
        node2,
        f"SELECT getNamedScalar('sr_wm') > {first}",
        "1\n",
        retry_count=40,
        sleep_time=0.25,
    )


def test_refreshable_create_publishes_initialized_entry(started_cluster, cleanup):
    """CREATE pauses after in-memory publish. SYSTEM REFRESH during that pause
    must not report "not refreshable"."""

    errors = []
    saw_refresh_success = False
    last_refresh_error = ""

    def create_variable():
        try:
            node1.query(
                "CREATE SHARED NAMED SCALAR init_pub REFRESH EVERY 36500 DAYS AS SELECT toUInt64(now())"
            )
        except Exception as exc:
            errors.append(exc)

    node1.query("SYSTEM ENABLE FAILPOINT named_scalar_create_after_publish_pause")
    creator = threading.Thread(target=create_variable)
    creator.start()
    try:
        deadline = time.time() + 30
        while time.time() < deadline:
            if errors:
                break

            try:
                node1.query("SYSTEM REFRESH NAMED SCALAR init_pub")
                saw_refresh_success = True
                break
            except Exception as exc:
                message = str(exc)
                last_refresh_error = message
                if "not refreshable" in message:
                    raise
                if "NAMED_SCALAR_NOT_FOUND" not in message:
                    # Keep polling while CREATE is still wiring the entry.
                    pass

            time.sleep(0.1)
    finally:
        node1.query("SYSTEM DISABLE FAILPOINT named_scalar_create_after_publish_pause")
        creator.join(timeout=30)

    assert not creator.is_alive()
    if errors:
        raise errors[0]
    assert saw_refresh_success, last_refresh_error or "SYSTEM REFRESH NAMED SCALAR never succeeded during paused CREATE"

    assert_eq_with_retry(
        node2,
        "SELECT getNamedScalar('init_pub') > 0",
        "1\n",
        retry_count=40,
        sleep_time=0.25,
    )


def test_refresh_failover(started_cluster, cleanup):
    node1.query(
        "CREATE SHARED NAMED SCALAR fo_wm REFRESH EVERY 2 SECOND AS SELECT toUInt64(now())"
    )
    assert_eq_with_retry(node2, "SELECT getNamedScalar('fo_wm') > 0", "1\n")

    node1.stop_clickhouse(kill=True)
    try:
        # Wait out the ephemeral-lock holder's ZK session. Default session timeout
        # for Kazoo-style helpers in this repo is a few seconds; budget generously.
        deadline = time.time() + 60
        seen = None
        while time.time() < deadline:
            v = node2.query("SELECT getNamedScalar('fo_wm')").strip()
            if seen is None:
                seen = v
            elif v != seen:
                return  # value moved; some replica is refreshing now
            time.sleep(1)
        pytest.fail("no refresh observed on node2 after node1 was killed")
    finally:
        node1.start_clickhouse()


# -------- Pass 2: races, ZK disruption, discovery churn --------


def test_duplicate_create_cluster_race(started_cluster, cleanup):
    """Two nodes CREATE the same shared scalar concurrently.
    Exactly one succeeds; the other sees NAMED_SCALAR_ALREADY_EXISTS.
    Both end up seeing the winner's value."""

    results = {}

    def create(node, tag):
        try:
            node.query(
                f"CREATE SHARED NAMED SCALAR race_cv AS SELECT toUInt64({tag})"
            )
            results[tag] = "ok"
        except Exception as exc:
            results[tag] = str(exc)

    t1 = threading.Thread(target=create, args=(node1, 1))
    t2 = threading.Thread(target=create, args=(node2, 2))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    outcomes = list(results.values())
    ok_count = sum(1 for o in outcomes if o == "ok")
    # The loser either hits NAMED_SCALAR_ALREADY_EXISTS (if the winner's ZK create
    # landed first) or the per-variable publish lock (if the winner is still
    # writing definition + value atomically). Either outcome proves the two
    # CREATEs were serialised.
    err_count = sum(
        1 for o in outcomes
        if "NAMED_SCALAR_ALREADY_EXISTS" in o
        or "currently creating or refreshing" in o
    )
    assert ok_count == 1 and err_count == 1, results

    winner_tag = next(t for t, o in results.items() if o == "ok")
    expected = str(winner_tag)
    for node in nodes:
        assert_eq_with_retry(
            node,
            "SELECT getNamedScalar('race_cv')",
            expected + "\n",
            retry_count=40,
            sleep_time=0.25,
        )


def test_create_or_replace_while_refreshing(started_cluster, cleanup):
    """OR REPLACE in the middle of a refresh cycle must converge both nodes
    on the new value and drop the REFRESH schedule."""

    node1.query(
        "CREATE SHARED NAMED SCALAR repl_race REFRESH EVERY 1 SECOND AS SELECT toUInt64(now())"
    )
    assert_eq_with_retry(node2, "SELECT getNamedScalar('repl_race') > 0", "1\n")

    # Let several ticks happen so the scheduler is genuinely active.
    time.sleep(2)

    node2.query(
        "CREATE OR REPLACE SHARED NAMED SCALAR repl_race AS SELECT toUInt64(42)"
    )

    for node in nodes:
        assert_eq_with_retry(
            node,
            "SELECT getNamedScalar('repl_race')",
            "42\n",
            retry_count=40,
            sleep_time=0.25,
        )

    # Stable at 42 across a window that would have covered several refresh ticks
    # if the schedule had survived.
    time.sleep(3)
    for node in nodes:
        assert node.query(
            "SELECT getNamedScalar('repl_race')"
        ).strip() == "42"


def test_create_cluster_rolls_back_on_value_store_failure(started_cluster, cleanup):
    """If initial value publication fails, CREATE must fail and leave no
    definition-only ghost variable in Keeper."""

    node1.query("SYSTEM ENABLE FAILPOINT shared_named_scalars_store_value_fail_once")
    try:
        err = node1.query_and_get_error(
            "CREATE SHARED NAMED SCALAR publish_fail_cv AS SELECT toUInt64(11)"
        )
    finally:
        # ONCE failpoints auto-disable after trigger, but disable explicitly in
        # case CREATE failed before reaching the injection point.
        node1.query("SYSTEM DISABLE FAILPOINT shared_named_scalars_store_value_fail_once")

    assert (
        "Injected failure while storing shared scalar" in err
        or "KEEPER_EXCEPTION" in err
    ), err

    for node in nodes:

        def missing(n=node):
            error = n.query_and_get_error(
                "SELECT getNamedScalar('publish_fail_cv')"
            )
            return "NAMED_SCALAR_NOT_FOUND" in error

        deadline = time.time() + 10
        while time.time() < deadline and not missing():
            time.sleep(0.25)
        assert missing(), f"{node.name} still sees replicated publish_fail_cv"

    node1.query("CREATE SHARED NAMED SCALAR publish_fail_cv AS SELECT toUInt64(11)")
    for node in nodes:
        assert_eq_with_retry(
            node,
            "SELECT getNamedScalar('publish_fail_cv')",
            "11\n",
            retry_count=40,
            sleep_time=0.25,
        )


def test_cluster_refresh_failure_flap(started_cluster, cleanup):
    """Refresh fails on the leader after its dependency is dropped; the last-good
    value + last_error propagate via ZK to the peer; recovery clears the error."""

    for node in nodes:
        node.query("DROP TABLE IF EXISTS default.flap_src SYNC")
        node.query("CREATE TABLE default.flap_src (x UInt8) ENGINE = Memory")
        node.query("INSERT INTO default.flap_src VALUES (42)")

    node1.query(
        "CREATE SHARED NAMED SCALAR flap_cv REFRESH EVERY 1 SECOND "
        "AS (SELECT max(x) FROM default.flap_src)"
    )
    assert_eq_with_retry(node1, "SELECT getNamedScalar('flap_cv')", "42\n")
    assert_eq_with_retry(node2, "SELECT getNamedScalar('flap_cv')", "42\n")

    for node in nodes:
        node.query("DROP TABLE default.flap_src SYNC")

    def is_valid_flipped(node):
        row = node.query(
            "SELECT has_value, current_value_is_valid, coalesce(exception,'') != '' "
            "FROM system.named_scalars "
            "WHERE name = 'flap_cv' AND kind = 'shared'"
        ).strip()
        return row == "1\t0\t1"

    deadline = time.time() + 30
    while time.time() < deadline:
        if all(is_valid_flipped(n) for n in nodes):
            break
        time.sleep(0.5)
    else:
        pytest.fail(
            "flap did not propagate: "
            + "; ".join(
                f"{n.name}: "
                + n.query(
                    "SELECT has_value, current_value_is_valid, exception "
                    "FROM system.named_scalars WHERE name='flap_cv'"
                ).strip()
                for n in nodes
            )
        )

    # Last-good value is still visible on both nodes.
    for node in nodes:
        assert node.query(
            "SELECT getNamedScalar('flap_cv')"
        ).strip() == "42"

    # Recovery: recreate table + data, expect current_value_is_valid back to 1 within a few ticks.
    for node in nodes:
        node.query("CREATE TABLE default.flap_src (x UInt8) ENGINE = Memory")
        node.query("INSERT INTO default.flap_src VALUES (7)")

    def recovered(node):
        row = node.query(
            "SELECT current_value_is_valid, coalesce(exception,'') = '' "
            "FROM system.named_scalars "
            "WHERE name = 'flap_cv' AND kind = 'shared'"
        ).strip()
        return row == "1\t1"

    deadline = time.time() + 30
    while time.time() < deadline:
        if all(recovered(n) for n in nodes):
            for node in nodes:
                node.query("DROP TABLE IF EXISTS default.flap_src SYNC")
            return
        time.sleep(0.5)
    pytest.fail("recovery did not happen in time")

def test_zk_disconnect_reads_continue(started_cluster, cleanup):
    """Partition node2 from ZK. getNamedScalar on node2 must keep serving the
    cached value. After the partition heals, a new write from node1 reaches
    node2 again."""

    node1.query(
        "CREATE SHARED NAMED SCALAR zkd_cv AS SELECT toUInt64(100)"
    )
    assert_eq_with_retry(node2, "SELECT getNamedScalar('zkd_cv')", "100\n")

    with PartitionManager() as pm:
        pm.drop_instance_zk_connections(node2)
        # Cached reads keep working even while ZK is unreachable.
        for _ in range(5):
            assert node2.query(
                "SELECT getNamedScalar('zkd_cv')"
            ).strip() == "100"
            time.sleep(0.5)

    # After partition heals, a new write on node1 should propagate.
    node1.query(
        "CREATE OR REPLACE SHARED NAMED SCALAR zkd_cv AS SELECT toUInt64(101)"
    )
    assert_eq_with_retry(
        node2,
        "SELECT getNamedScalar('zkd_cv')",
        "101\n",
        retry_count=80,
        sleep_time=0.25,
    )


def test_zk_session_loss_re_enumerate(started_cluster, cleanup):
    """Stop Keeper long enough for the ZK session to expire, restart it, and
    assert the coordinator on both nodes re-enumerates — a fresh CREATE on
    node1 must reach node2 without a node restart."""

    node1.query(
        "CREATE SHARED NAMED SCALAR sess_cv AS SELECT toUInt64(1)"
    )
    assert_eq_with_retry(node2, "SELECT getNamedScalar('sess_cv')", "1\n")

    cluster.stop_zookeeper_nodes(ZOOKEEPER_CONTAINERS)
    # Session timeout for the integration fixture is ~30 s; wait past it.
    time.sleep(35)
    cluster.start_zookeeper_nodes(ZOOKEEPER_CONTAINERS)

    # After the session returns, the coordinator should reconnect and a new
    # CREATE on node1 must reach node2.
    node1.query_with_retry(
        "CREATE OR REPLACE SHARED NAMED SCALAR sess_cv AS SELECT toUInt64(2)",
        retry_count=40,
        sleep_time=1.0,
    )
    assert_eq_with_retry(
        node2,
        "SELECT getNamedScalar('sess_cv')",
        "2\n",
        retry_count=60,
        sleep_time=1.0,
    )


def test_restart_during_refresh_no_leak(started_cluster, cleanup):
    """Kill the node most likely to hold the ephemeral refresh lock; after
    restart it should rejoin cleanly and the variable's value should keep
    advancing without any node getting stuck on a stale hostname."""

    node1.query(
        "CREATE SHARED NAMED SCALAR rlk_cv REFRESH EVERY 1 SECOND AS SELECT toUInt64(now())"
    )
    assert_eq_with_retry(node2, "SELECT getNamedScalar('rlk_cv') > 0", "1\n")

    node1.stop_clickhouse(kill=True)
    try:
        time.sleep(3)
        # While node1 is down, node2 must keep refreshing.
        before = int(node2.query("SELECT getNamedScalar('rlk_cv')").strip())
        time.sleep(3)
        after = int(node2.query("SELECT getNamedScalar('rlk_cv')").strip())
        assert after > before, (before, after)
    finally:
        node1.start_clickhouse()

    # After restart node1 sees the variable and the value continues to advance.
    assert_eq_with_retry(node1, "SELECT getNamedScalar('rlk_cv') > 0", "1\n")
    v1 = int(node1.query("SELECT getNamedScalar('rlk_cv')").strip())
    time.sleep(3)
    v2 = int(node1.query("SELECT getNamedScalar('rlk_cv')").strip())
    assert v2 > v1, (v1, v2)


def test_or_replace_drops_refresh_schedule_on_peer(started_cluster, cleanup):
    """Reported by codex review. The peer's coordinator fast-path skipped rebuilds
    when the AST hash of the expression matched. If OR REPLACE only removed the
    REFRESH clause (keeping the same expression), the peer kept its old scheduler
    alive and continued publishing refreshed values via ZK, violating the new
    definition that says 'no refresh'."""

    node1.query(
        "CREATE SHARED NAMED SCALAR rs_strip REFRESH EVERY 1 SECOND AS SELECT toUInt64(now())"
    )
    assert_eq_with_retry(node2, "SELECT getNamedScalar('rs_strip') > 0", "1\n")

    # Let ticks run on both sides so both nodes have a live refresh scheduler.
    time.sleep(2)

    # Same expression, REFRESH removed. Hash of expression is unchanged — the
    # fast-path used to short-circuit and leave the old scheduler alive.
    node1.query(
        "CREATE OR REPLACE SHARED NAMED SCALAR rs_strip AS SELECT toUInt64(now())"
    )

    # Give any lingering scheduler ticks a chance to fire.
    time.sleep(3)

    # After OR REPLACE the value is whatever CREATE evaluated once — not a
    # moving target. Sample on each node across another window; value must be
    # identical on each sample.
    for node in nodes:
        snapshots = []
        for _ in range(4):
            snapshots.append(node.query("SELECT getNamedScalar('rs_strip')").strip())
            time.sleep(1)
        assert len(set(snapshots)) == 1, (node.name, snapshots)

    # system.named_scalars.refresh_interval is NULL for a non-refreshable var.
    for node in nodes:
        row = node.query(
            "SELECT refresh_interval IS NULL FROM system.named_scalars "
            "WHERE name = 'rs_strip' AND kind = 'shared'"
        ).strip()
        assert row == "1", (node.name, row)


def test_drop_while_discovery_in_flight(started_cluster, cleanup):
    """On node1, create a batch of shared named_scalars; simultaneously drop one
    of them from node2. All other entries must eventually be visible on node2
    and the coordinator must keep processing subsequent DDL."""

    names = [f"batch_cv_{i:02d}" for i in range(20)]
    target = names[7]

    # Seed the target first so that DROP on node2 has something to race with.
    node1.query(f"CREATE SHARED NAMED SCALAR {target} AS SELECT toUInt64(999)")
    assert_eq_with_retry(
        node2, f"SELECT getNamedScalar('{target}')", "999\n"
    )

    def create_batch():
        for name in names:
            if name == target:
                continue
            node1.query(f"CREATE SHARED NAMED SCALAR {name} AS SELECT toUInt64(1)")

    def drop_target():
        time.sleep(0.05)  # small head start for create_batch
        node2.query(f"DROP NAMED SCALAR IF EXISTS {target}")

    t1 = threading.Thread(target=create_batch)
    t2 = threading.Thread(target=drop_target)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    # Non-target entries must end up visible on both nodes.
    for node in nodes:
        for name in names:
            if name == target:
                continue
            assert_eq_with_retry(
                node,
                f"SELECT getNamedScalar('{name}')",
                "1\n",
                retry_count=40,
                sleep_time=0.25,
            )

    # Target is gone on both nodes.
    for node in nodes:

        def target_gone(n=node):
            err = n.query_and_get_error(
                f"SELECT getNamedScalar('{target}')"
            )
            return "NAMED_SCALAR_NOT_FOUND" in err

        deadline = time.time() + 10
        while time.time() < deadline and not target_gone():
            time.sleep(0.25)
        assert target_gone(), (
            f"{node.name} still sees replicated {target} after DROP"
        )

    # Coordinator is still alive: further DDL goes through.
    node1.query("CREATE SHARED NAMED SCALAR batch_post AS SELECT toUInt64(1)")
    assert_eq_with_retry(
        node2, "SELECT getNamedScalar('batch_post')", "1\n"
    )


# -------- LOCAL persistence (replaces stateless 03802 / 03807 / 03810) --------
# These properties used to live in stateless tests driven by clickhouse-local,
# but timing-coupled assertions against a short-lived local instance are
# fragile under CI load. Real stop_clickhouse() / start_clickhouse() with
# assert_eq_with_retry is the proper harness.


def _drop_all_local_named_scalars(node):
    rows = node.query(
        "SELECT name FROM system.named_scalars WHERE kind = 'local'"
    ).strip()
    for name in [r for r in rows.splitlines() if r]:
        node.query(f"DROP NAMED SCALAR IF EXISTS {name}")


@pytest.fixture
def cleanup_local():
    yield
    try:
        _drop_all_local_named_scalars(node1)
    except Exception:
        pass


def test_local_restart_reloads_state(started_cluster, cleanup_local):
    """LOCAL scalar definitions and last-good values are loaded synchronously
    during server startup, and the refresh task resumes ticking afterwards."""

    node1.query("DROP NAMED SCALAR IF EXISTS lr_cv")
    node1.query("DROP TABLE IF EXISTS default.lr_src SYNC")
    node1.query("CREATE TABLE default.lr_src (x UInt64) ENGINE=MergeTree() ORDER BY x")
    node1.query("INSERT INTO default.lr_src VALUES (123)")
    node1.query(
        "CREATE NAMED SCALAR lr_cv REFRESH EVERY 1 SECOND "
        "AS (SELECT max(x) FROM default.lr_src)"
    )
    assert node1.query("SELECT getNamedScalar('lr_cv')").strip() == "123"

    node1.stop_clickhouse()
    node1.start_clickhouse()

    # Reload is sync: definition + last-good value visible immediately.
    assert node1.query("SELECT getNamedScalar('lr_cv')").strip() == "123"

    # Refresh task resumes; advance the value and observe the new one.
    node1.query("INSERT INTO default.lr_src VALUES (200)")
    assert_eq_with_retry(
        node1, "SELECT getNamedScalar('lr_cv')", "200\n",
        retry_count=60, sleep_time=0.25,
    )

    node1.query("DROP TABLE default.lr_src SYNC")


def test_persistent_cadence_resumes_schedule(started_cluster, cleanup_local):
    """Refresh schedule resumes from the persisted last_successful_update_time
    instead of the restart moment. Without the fix, EVERY 1 HOUR would defer
    the next tick by ~1 hour even if it was almost due."""

    node1.query("DROP NAMED SCALAR IF EXISTS pc_cv")
    node1.query(
        "CREATE NAMED SCALAR pc_cv REFRESH EVERY 1 HOUR AS SELECT toUInt64(1)"
    )

    node1.stop_clickhouse()

    # Rewrite the persisted timestamps to "3570 seconds ago" so the next tick
    # under EVERY 1 HOUR should land ~30 s after restart.
    rewrite_script = (
        "import os, re, time\n"
        "root = '/var/lib/clickhouse/named_scalars_cache'\n"
        "for f in os.listdir(root):\n"
        "    p = os.path.join(root, f)\n"
        "    t = open(p).read()\n"
        "    target = int(time.time()) - 3570\n"
        "    t = re.sub(r'^last_update_time: \\d+$', "
        "f'last_update_time: {target}', t, count=1, flags=re.MULTILINE)\n"
        "    t = re.sub(r'^last_successful_update_time: \\d+$', "
        "f'last_successful_update_time: {target}', t, count=1, flags=re.MULTILINE)\n"
        "    open(p, 'w').write(t)\n"
    )
    node1.exec_in_container(["python3", "-c", rewrite_script], user="root")

    node1.start_clickhouse()

    # Next refresh should land within ~5 minutes; anything close to 3600
    # would mean the schedule reset to "now".
    next_in = int(node1.query(
        "SELECT toInt64(next_refresh_time) - toInt64(now()) "
        "FROM system.named_scalars WHERE name = 'pc_cv' AND kind = 'local'"
    ).strip())
    assert next_in < 300, f"next_refresh_time is {next_in} s away (expected < 300)"


def test_creator_database_normalization(started_cluster, cleanup_local):
    """Unqualified table refs in the CREATE NAMED SCALAR body are rewritten
    with the creator's current database. After restart, SYSTEM REFRESH from a
    process whose default database is `default` still resolves correctly."""

    node1.query("DROP NAMED SCALAR IF EXISTS cd_cv")
    node1.query("DROP DATABASE IF EXISTS cd_db SYNC")
    node1.query("CREATE DATABASE cd_db")
    node1.query("CREATE TABLE cd_db.t (x UInt64) ENGINE=MergeTree() ORDER BY x")
    node1.query("INSERT INTO cd_db.t VALUES (10), (20), (30)")

    # CREATE in cd_db; refer to `t` unqualified.
    node1.query(
        "CREATE NAMED SCALAR cd_cv REFRESH EVERY 36500 DAYS AS (SELECT sum(x) FROM t)",
        database="cd_db",
    )
    assert node1.query("SELECT getNamedScalar('cd_cv')").strip() == "60"

    node1.stop_clickhouse()
    node1.start_clickhouse()

    # SYSTEM REFRESH from default database; without normalization this would
    # try to resolve `t` against `default` and fail with UNKNOWN_TABLE.
    node1.query("SYSTEM REFRESH NAMED SCALAR cd_cv")
    assert_eq_with_retry(
        node1,
        "SELECT current_value_is_valid, coalesce(exception, '') = '' "
        "FROM system.named_scalars WHERE name = 'cd_cv'",
        "1\t1\n",
        retry_count=60, sleep_time=0.25,
    )

    node1.query("DROP DATABASE cd_db SYNC")
