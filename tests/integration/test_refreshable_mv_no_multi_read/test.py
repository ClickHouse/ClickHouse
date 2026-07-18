#!/usr/bin/env python3

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/106737
#
# A coordinated refreshable materialized view (created in a Replicated database) used to
# crash the server when it was re-attached on a Keeper that does not support the MULTI_READ
# feature flag. The constructor's up-front MULTI_READ check was gated behind the fresh-CREATE
# path only, so on ATTACH/restore the view became coordinated=true and the scheduling thread
# later threw NOT_IMPLEMENTED in readZnodesIfNeeded, which the doScheduling catch-all re-raised
# as a LOGICAL_ERROR -> server abort (and crash-loop on restart). The view must instead stop
# gracefully and leave the server running.

import os
import time

import pytest

from helpers.cluster import ClickHouseCluster

CURRENT_TEST_DIR = os.path.dirname(os.path.abspath(__file__))

cluster = ClickHouseCluster(__file__)

# The keeper config is swapped at runtime (see use_keeper_config), so it is installed from the
# test body rather than as a main_config that the harness would also try to manage.
node = cluster.add_instance(
    "node",
    user_configs=["configs/settings.xml"],
    stay_alive=True,
    macros={"shard": 1, "replica": 1},
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def use_keeper_config(config_name):
    """Install one of the keeper configs as the single enable_keeper.xml on the node."""
    node.copy_file_to_container(
        os.path.join(CURRENT_TEST_DIR, "configs", config_name),
        "/etc/clickhouse-server/config.d/enable_keeper.xml",
    )


def test_refreshable_mv_attach_without_multi_read(started_cluster):
    use_keeper_config("enable_keeper_multi_read.xml")
    node.restart_clickhouse()

    node.query(
        "CREATE DATABASE rdb ENGINE = Replicated('/clickhouse/rdb', '{shard}', '{replica}')"
    )
    # Non-APPEND refreshable MV in a Replicated database is always coordinated.
    node.query(
        """
        CREATE MATERIALIZED VIEW rdb.mv
        REFRESH EVERY 1 SECOND
        ENGINE = ReplicatedMergeTree ORDER BY x
        EMPTY
        AS SELECT number AS x FROM numbers(3)
        """
    )

    # Refresh once while MULTI_READ is available, so coordination znodes exist and the view's
    # persisted state is the normal coordinated one.
    node.query("SYSTEM REFRESH VIEW rdb.mv")
    node.query("SYSTEM WAIT VIEW rdb.mv")
    assert node.query("SELECT count() FROM rdb.mv").strip() == "3"

    # Downgrade Keeper: disable MULTI_READ, then restart. On startup the Replicated database
    # re-attaches rdb.mv (attach=true), which is exactly the path that used to crash.
    use_keeper_config("enable_keeper_no_multi_read.xml")
    node.restart_clickhouse()

    # The server must be up and answering queries (no crash, no crash-loop).
    assert node.query("SELECT 1").strip() == "1"

    # Attach schedules the graceful-stop pass asynchronously, so the status read can otherwise
    # observe the transient Scheduling state before doScheduling reaches the Disabled state. Wait
    # for the scheduled pass to settle (WAIT VIEW returns immediately once the view is Disabled).
    node.query("SYSTEM WAIT VIEW rdb.mv")

    # The view must have stopped gracefully, reporting the reason rather than aborting.
    status = node.query(
        "SELECT status, exception FROM system.view_refreshes WHERE view = 'mv'"
    )
    assert "Disabled" in status, status
    assert "multi-read" in status.lower() or "multi_read" in status.lower(), status

    # The doScheduling catch-all LOGICAL_ERROR must not have fired.
    assert not node.contains_in_log("Unexpected exception in refresh scheduling")

    # The gracefully-stopped state must be non-resumable: a coordinated view must never be turned
    # into an uncoordinated local refresh (that would corrupt the replicated target table). So
    # SYSTEM START VIEW must NOT resume it while MULTI_READ is still missing - it stays Disabled,
    # runs no refresh, and the server stays up.
    node.query("SYSTEM START VIEW rdb.mv")
    node.query("SYSTEM REFRESH VIEW rdb.mv")
    assert node.query("SELECT 1").strip() == "1"
    # SYSTEM REFRESH VIEW is async: run() moves the task to Scheduling, and only the background
    # scheduler later hits the coordination.unavailable branch and switches it back to Disabled.
    # Wait for that scheduled pass before asserting, so the read never observes transient Scheduling.
    node.query("SYSTEM WAIT VIEW rdb.mv")
    status = node.query(
        "SELECT status FROM system.view_refreshes WHERE view = 'mv'"
    ).strip()
    assert status == "Disabled", status
    assert not node.contains_in_log("Unexpected exception in refresh scheduling")

    # Restoring MULTI_READ and restarting must keep the server healthy.
    use_keeper_config("enable_keeper_multi_read.xml")
    node.restart_clickhouse()
    assert node.query("SELECT 1").strip() == "1"

    node.query("DROP DATABASE rdb SYNC")


def test_refreshable_mv_attach_feature_flag_propagation_race(started_cluster):
    # The constructor reads the Keeper feature flags once, when its Keeper connection is
    # established. A view re-attached during a (re)start can read them before the connection
    # settles on the current Keeper, see MULTI_READ as still present, and stay coordinated even
    # though the Keeper actually lacks it. The scheduling thread would then throw NOT_IMPLEMENTED
    # in readZnodesIfNeeded and the doScheduling catch-all would abort the server. doScheduling
    # must re-check the flags before touching Keeper and stop the view gracefully.
    #
    # That race is timing-dependent and only shows up under slow configs, so it is forced
    # deterministically here: the no-multi-read Keeper config also activates the
    # refresh_mv_skip_attach_feature_flag_check failpoint, which makes the constructor's check
    # behave as if the flags were present. The view therefore attaches as coordinated on a Keeper
    # without MULTI_READ - exactly the post-race state - and only doScheduling can save the server.
    use_keeper_config("enable_keeper_multi_read.xml")
    node.restart_clickhouse()

    node.query(
        "CREATE DATABASE rdb2 ENGINE = Replicated('/clickhouse/rdb2', '{shard}', '{replica}')"
    )
    node.query(
        """
        CREATE MATERIALIZED VIEW rdb2.mv
        REFRESH EVERY 1 SECOND
        ENGINE = ReplicatedMergeTree ORDER BY x
        EMPTY
        AS SELECT number AS x FROM numbers(3)
        """
    )
    node.query("SYSTEM REFRESH VIEW rdb2.mv")
    node.query("SYSTEM WAIT VIEW rdb2.mv")
    assert node.query("SELECT count() FROM rdb2.mv").strip() == "3"

    # The prior test (same node) may already have the catch-all message in the log, so compare the
    # count across this restart rather than asserting absolute absence.
    aborts_before = int(node.count_in_log("Unexpected exception in refresh scheduling"))

    # Downgrade Keeper AND make the constructor miss the downgrade (simulating the race).
    use_keeper_config("enable_keeper_no_multi_read_simulate_attach_race.xml")
    node.restart_clickhouse()

    # The server must be up and answering queries (no crash, no crash-loop).
    assert node.query("SELECT 1").strip() == "1"

    # doScheduling re-detected the missing flags and stopped the view gracefully. WAIT VIEW returns
    # immediately once the view is Disabled.
    node.query("SYSTEM WAIT VIEW rdb2.mv")
    status = node.query(
        "SELECT status, exception FROM system.view_refreshes WHERE view = 'mv' AND database = 'rdb2'"
    )
    assert "Disabled" in status, status
    assert "multi-read" in status.lower() or "multi_read" in status.lower(), status

    # The catch-all LOGICAL_ERROR must not have fired during this restart.
    aborts_after = int(node.count_in_log("Unexpected exception in refresh scheduling"))
    assert aborts_after == aborts_before, (aborts_before, aborts_after)

    use_keeper_config("enable_keeper_multi_read.xml")
    node.restart_clickhouse()
    assert node.query("SELECT 1").strip() == "1"

    node.query("DROP DATABASE rdb2 SYNC")


def test_refreshable_mv_scheduling_detects_missing_flags_mid_refresh(started_cluster):
    # The doScheduling feature-flag re-check runs before the in-flight refresh reconciliation, so a
    # scheduling pass that discovers missing Keeper flags while this replica already has a refresh
    # Running (or Finished) must cancel/reconcile that local refresh before giving up coordination.
    # Otherwise a non-APPEND refresh would keep exchanging/dropping tables locally while Keeper still
    # believes this replica is running the refresh, blocking the coordinated view on other replicas
    # until session expiry. (The flags can stop being visible mid-execution, e.g. on a Keeper session
    # change.)
    #
    # The flags cannot actually flip on a live connection, so the mid-refresh case is forced with the
    # refresh_mv_force_scheduling_feature_flags_missing failpoint: a coordinated view runs a slow
    # refresh on a healthy (multi-read) Keeper, the failpoint makes the next scheduling pass see the
    # flags as missing, and that pass must stop the running refresh instead of letting it finish
    # outside coordination.
    use_keeper_config("enable_keeper_multi_read.xml")
    node.restart_clickhouse()

    node.query(
        "CREATE DATABASE rdb3 ENGINE = Replicated('/clickhouse/rdb3', '{shard}', '{replica}')"
    )
    # A slow, coordinated refresh: sleepEachRow with max_block_size=1 keeps it Running long enough to
    # interrupt. The target row count lets us tell a cancelled refresh apart from one that ran to
    # completion: if the refresh is NOT cancelled it exchanges tables and the target gets REFRESH_ROWS
    # rows; if it IS cancelled the target stays empty.
    REFRESH_ROWS = 20
    node.query(
        f"""
        CREATE MATERIALIZED VIEW rdb3.mv
        REFRESH EVERY 1 YEAR
        ENGINE = ReplicatedMergeTree ORDER BY x
        EMPTY
        AS SELECT number AS x FROM numbers({REFRESH_ROWS}) WHERE sleepEachRow(1) = 0 SETTINGS max_block_size = 1
        """
    )

    aborts_before = int(node.count_in_log("Unexpected exception in refresh scheduling"))

    # Start a refresh and wait until it is actually Running on this replica.
    node.query("SYSTEM REFRESH VIEW rdb3.mv")
    status = None
    for _ in range(60):
        status = node.query(
            "SELECT status FROM system.view_refreshes WHERE view = 'mv' AND database = 'rdb3'"
        ).strip()
        if status == "Running":
            break
        time.sleep(0.5)
    assert status == "Running", status
    # The target table is still empty: the slow refresh has not exchanged tables yet.
    assert node.query("SELECT count() FROM rdb3.mv").strip() == "0"

    # Make the next scheduling pass observe the flags as missing while the refresh is in flight, then
    # poke the scheduler so the pass runs now rather than waiting a year.
    node.query("SYSTEM ENABLE FAILPOINT refresh_mv_force_scheduling_feature_flags_missing")
    node.query("SYSTEM REFRESH VIEW rdb3.mv")

    # The scheduling pass must stop the view gracefully (Disabled), with the missing-flags reason and
    # no scheduling-thread abort. WAIT VIEW returns immediately once the view is Disabled.
    node.query("SYSTEM WAIT VIEW rdb3.mv")
    status = node.query(
        "SELECT status, exception FROM system.view_refreshes WHERE view = 'mv' AND database = 'rdb3'"
    )
    assert "Disabled" in status, status
    assert "multi-read" in status.lower() or "multi_read" in status.lower(), status

    aborts_after = int(node.count_in_log("Unexpected exception in refresh scheduling"))
    assert aborts_after == aborts_before, (aborts_before, aborts_after)

    # The key check: the in-flight refresh must have been cancelled, not left running. If it was NOT
    # cancelled it keeps going and, on this healthy Keeper, completes the table exchange, populating
    # the target with REFRESH_ROWS rows. We watch the target for longer than the whole refresh would
    # take (sleepEachRow(1) per row): with the fix the target stays empty; without it the rows appear.
    deadline = time.monotonic() + REFRESH_ROWS + 15
    while time.monotonic() < deadline:
        count = node.query("SELECT count() FROM rdb3.mv").strip()
        assert count == "0", f"the cancelled refresh leaked {count} rows into the target"
        time.sleep(1)

    node.query("SYSTEM DISABLE FAILPOINT refresh_mv_force_scheduling_feature_flags_missing")
    node.query("DROP DATABASE rdb3 SYNC")


def test_refreshable_mv_scheduling_detects_missing_flags_after_insert(started_cluster):
    # Same coordination leak as the previous test, but in the narrower post-insert window: the
    # scheduling pass discovers the missing flags AFTER the insert pipeline has already finished but
    # BEFORE the target-table exchange. Once the pipeline finished, the executor is gone, so
    # interruptExecution() (which only cancels the executor) is a no-op; the refresh thread would
    # otherwise proceed to exchange the target table outside coordination while Keeper still thinks
    # this replica is running the refresh. The cancellation must be observable at the exchange
    # boundary itself, not just via the executor.
    #
    # The window is forced with two failpoints: refresh_mv_pause_before_exchange pauses the refresh
    # thread right after the insert finished and before the exchange, and
    # refresh_mv_force_scheduling_feature_flags_missing makes the scheduling pass that runs while it
    # is paused decide to give up coordination (setting the interrupt flag). Because a refresh is
    # still in flight, that pass must only interrupt it, NOT disable the view yet; it must let the
    # attempt reconcile first. When the refresh thread resumes it sees the flag and skips the
    # exchange, and only the following scheduling pass disables the view.
    use_keeper_config("enable_keeper_multi_read.xml")
    node.restart_clickhouse()

    node.query(
        "CREATE DATABASE rdb4 ENGINE = Replicated('/clickhouse/rdb4', '{shard}', '{replica}')"
    )
    # A fast insert: the pause failpoint (not slow rows) controls timing, so the pipeline completes
    # quickly and the thread blocks at the pre-exchange point. The target row count tells a skipped
    # exchange apart from one that happened: if the exchange is skipped the target stays empty; if it
    # happens the target gets REFRESH_ROWS rows.
    REFRESH_ROWS = 5
    node.query(
        f"""
        CREATE MATERIALIZED VIEW rdb4.mv
        REFRESH EVERY 1 YEAR
        ENGINE = ReplicatedMergeTree ORDER BY x
        EMPTY
        AS SELECT number AS x FROM numbers({REFRESH_ROWS})
        """
    )

    aborts_before = int(node.count_in_log("Unexpected exception in refresh scheduling"))

    # Pause the next refresh right after its insert pipeline finishes, before the exchange.
    node.query("SYSTEM ENABLE FAILPOINT refresh_mv_pause_before_exchange")
    node.query("SYSTEM REFRESH VIEW rdb4.mv")
    # Block until the refresh thread is paused at the pre-exchange point. Reaching it proves the
    # insert pipeline already completed (the data is in the temp table, the executor is gone).
    node.query("SYSTEM WAIT FAILPOINT refresh_mv_pause_before_exchange PAUSE", timeout=60)

    # The real target table is still empty: the exchange has not happened yet.
    assert node.query("SELECT count() FROM rdb4.mv").strip() == "0"

    # Run a scheduling pass that discovers the missing flags while the refresh sits in the post-insert
    # window. It finds execution.state == Running, so it only interrupts the in-flight refresh and
    # must NOT declare coordination unavailable yet: disabling the view now would make every later
    # scheduling pass bail before the attempt is reconciled. The view therefore stays Running (not
    # Disabled) while the refresh is still paused.
    node.query("SYSTEM ENABLE FAILPOINT refresh_mv_force_scheduling_feature_flags_missing")
    node.query("SYSTEM REFRESH VIEW rdb4.mv")
    # Give the scheduling pass time to run, then confirm the view was NOT prematurely disabled.
    time.sleep(3)
    status = node.query(
        "SELECT status FROM system.view_refreshes WHERE view = 'mv' AND database = 'rdb4'"
    ).strip()
    assert status == "Running", status
    # The exchange still has not happened.
    assert node.query("SELECT count() FROM rdb4.mv").strip() == "0"

    # Resume the paused refresh thread. Its pre-exchange check must now observe the interrupt flag and
    # skip the exchange instead of swapping the target table outside coordination. The refresh reaches
    # Finished, and only the following scheduling pass gives up coordination and disables the view.
    node.query("SYSTEM DISABLE FAILPOINT refresh_mv_pause_before_exchange")

    status = None
    for _ in range(60):
        status = node.query(
            "SELECT status, exception FROM system.view_refreshes WHERE view = 'mv' AND database = 'rdb4'"
        )
        if "Disabled" in status:
            break
        time.sleep(0.5)
    assert "Disabled" in status, status
    assert "multi-read" in status.lower() or "multi_read" in status.lower(), status

    aborts_after = int(node.count_in_log("Unexpected exception in refresh scheduling"))
    assert aborts_after == aborts_before, (aborts_before, aborts_after)

    # The key check: the exchange must have been skipped, so the target stays empty. Without the
    # pre-exchange cancellation check the resumed thread would complete the exchange and the target
    # would get REFRESH_ROWS rows.
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        count = node.query("SELECT count() FROM rdb4.mv").strip()
        assert count == "0", f"the cancelled refresh leaked {count} rows into the target"
        time.sleep(1)

    node.query("SYSTEM DISABLE FAILPOINT refresh_mv_force_scheduling_feature_flags_missing")
    node.query("DROP DATABASE rdb4 SYNC")


def test_refreshable_mv_scheduling_missing_flags_when_exchange_wins_race(started_cluster):
    # The exchange-wins variant of the post-insert race. The pre-exchange interrupt re-check reads the
    # interrupt flag under executor_mutex, releases the mutex, and only THEN exchanges tables. A
    # scheduling pass that discovers the missing flags in that tiny gap (after the check passed, before
    # the exchange) sets the interrupt flag too late: the exchange happens anyway. This is safe ONLY
    # because doScheduling does not disable the view while a refresh is in flight; it just interrupts
    # and lets the attempt reach Finished, so the next scheduling pass reconciles the coordination
    # state (records the znode, clears '/running') before giving up. If doScheduling disabled the view
    # in that gap instead, coordination.unavailable would short-circuit every later pass and the
    # Finished result would never be reconciled, leaving Keeper believing this replica is still running.
    #
    # The gap is forced deterministically with refresh_mv_pause_after_interrupt_check, which pauses the
    # refresh thread after the interrupt check passed but before the exchange.
    use_keeper_config("enable_keeper_multi_read.xml")
    node.restart_clickhouse()

    node.query(
        "CREATE DATABASE rdb5 ENGINE = Replicated('/clickhouse/rdb5', '{shard}', '{replica}')"
    )
    REFRESH_ROWS = 5
    node.query(
        f"""
        CREATE MATERIALIZED VIEW rdb5.mv
        REFRESH EVERY 1 YEAR
        ENGINE = ReplicatedMergeTree ORDER BY x
        EMPTY
        AS SELECT number AS x FROM numbers({REFRESH_ROWS})
        """
    )

    aborts_before = int(node.count_in_log("Unexpected exception in refresh scheduling"))

    # Pause the refresh after the pre-exchange interrupt check has already passed, before the exchange.
    node.query("SYSTEM ENABLE FAILPOINT refresh_mv_pause_after_interrupt_check")
    node.query("SYSTEM REFRESH VIEW rdb5.mv")
    node.query("SYSTEM WAIT FAILPOINT refresh_mv_pause_after_interrupt_check PAUSE", timeout=60)

    # The exchange has not happened yet, but the interrupt check for this attempt is already behind us.
    assert node.query("SELECT count() FROM rdb5.mv").strip() == "0"

    # A scheduling pass now finds the flags missing while the refresh is in flight. It must only
    # interrupt (a no-op here: the check already passed and the executor is gone) and leave the view
    # Running, not Disabled.
    node.query("SYSTEM ENABLE FAILPOINT refresh_mv_force_scheduling_feature_flags_missing")
    node.query("SYSTEM REFRESH VIEW rdb5.mv")
    time.sleep(3)
    status = node.query(
        "SELECT status FROM system.view_refreshes WHERE view = 'mv' AND database = 'rdb5'"
    ).strip()
    assert status == "Running", status

    # Resume. The exchange wins the race and populates the target (the interrupt landed too late). That
    # is fine: the attempt reaches Finished and the next scheduling pass reconciles it cleanly and only
    # then disables the view, with the missing-flags reason and no scheduling-thread abort.
    node.query("SYSTEM DISABLE FAILPOINT refresh_mv_pause_after_interrupt_check")

    status = None
    for _ in range(60):
        status = node.query(
            "SELECT status, exception FROM system.view_refreshes WHERE view = 'mv' AND database = 'rdb5'"
        )
        if "Disabled" in status:
            break
        time.sleep(0.5)
    assert "Disabled" in status, status
    assert "multi-read" in status.lower() or "multi_read" in status.lower(), status

    aborts_after = int(node.count_in_log("Unexpected exception in refresh scheduling"))
    assert aborts_after == aborts_before, (aborts_before, aborts_after)

    # The exchange did happen (it won the race), so the target holds the refreshed rows. The point of
    # this test is that the state machine stayed consistent (the view reconciled and then disabled),
    # not that the exchange was skipped.
    assert node.query("SELECT count() FROM rdb5.mv").strip() == str(REFRESH_ROWS)

    # The give-up path must reconcile the finished attempt in Keeper before entering the permanent
    # coordination-unavailable state. The reconciliation write (set + remove, no MULTI_READ /
    # CREATE_IF_NOT_EXISTS needed) records the successful attempt and clears the '/running' lock. If
    # it were skipped, last_success_time would stay NULL and '/running' would dangle until another
    # replica reclaimed it as a crash, leaving Keeper and system.view_refreshes on the pre-refresh
    # state even though the target table was already swapped.
    last_success = node.query(
        "SELECT last_success_time IS NOT NULL FROM system.view_refreshes WHERE view = 'mv' AND database = 'rdb5'"
    ).strip()
    assert last_success == "1", last_success

    uuid = node.query(
        "SELECT uuid FROM system.tables WHERE database = 'rdb5' AND name = 'mv'"
    ).strip()
    running_exists = node.query(
        f"SELECT count() FROM system.zookeeper WHERE path = '/clickhouse/tables/{uuid}/1' AND name = 'running'"
    ).strip()
    assert running_exists == "0", running_exists

    node.query("SYSTEM DISABLE FAILPOINT refresh_mv_force_scheduling_feature_flags_missing")
    node.query("DROP DATABASE rdb5 SYNC")


def test_refreshable_mv_scheduling_missing_flags_lost_ownership_race(started_cluster):
    # A lost-Keeper-session variant of the exchange-wins race. The feature-flags-missing give-up path
    # reconciles a Finished in-flight refresh in Keeper with a version-checked set(). If this replica
    # lost its Keeper session while the refresh was running, another replica can advance the
    # coordination znode first (reclaim the stale '/running' lock via the crashed-replica grace
    # period, or start the next refresh), so the reconciling set() is rejected with ZBADVERSION.
    #
    # This give-up path returns before readZnodesIfNeeded (the only place a fresh znode version is
    # re-read), and updateCoordinationState only converts version mismatches into a re-read for the
    # running=true acquire path. So a naive retry would keep reusing the same stale version and loop
    # in Scheduling forever instead of converging to Disabled. A lost-ownership conflict is terminal:
    # another replica now owns the coordination state and this replica is giving up coordination
    # permanently anyway, so the view must become Disabled without spinning.
    #
    # The Finished-at-give-up window is forced with refresh_mv_pause_after_interrupt_check exactly as
    # in test_..._when_exchange_wins_race; refresh_mv_force_scheduling_feature_flags_missing drives
    # the give-up path on a healthy Keeper; and refresh_mv_force_coordination_version_conflict makes
    # the reconciling set() carry a stale version so Keeper rejects it with ZBADVERSION.
    use_keeper_config("enable_keeper_multi_read.xml")
    node.restart_clickhouse()

    node.query(
        "CREATE DATABASE rdb6 ENGINE = Replicated('/clickhouse/rdb6', '{shard}', '{replica}')"
    )
    REFRESH_ROWS = 5
    node.query(
        f"""
        CREATE MATERIALIZED VIEW rdb6.mv
        REFRESH EVERY 1 YEAR
        ENGINE = ReplicatedMergeTree ORDER BY x
        EMPTY
        AS SELECT number AS x FROM numbers({REFRESH_ROWS})
        """
    )

    aborts_before = int(node.count_in_log("Unexpected exception in refresh scheduling"))

    # Pause the refresh after the pre-exchange interrupt check has passed, before the exchange.
    node.query("SYSTEM ENABLE FAILPOINT refresh_mv_pause_after_interrupt_check")
    node.query("SYSTEM REFRESH VIEW rdb6.mv")
    node.query("SYSTEM WAIT FAILPOINT refresh_mv_pause_after_interrupt_check PAUSE", timeout=60)
    assert node.query("SELECT count() FROM rdb6.mv").strip() == "0"

    # A scheduling pass now finds the flags missing while the refresh is in flight. It only interrupts
    # (a no-op: the interrupt check already passed) and leaves the view Running. Arm the version
    # conflict too, so that when the deferred attempt reaches Finished the reconciling set() is
    # rejected with ZBADVERSION (simulating another replica having taken over after a lost session).
    node.query("SYSTEM ENABLE FAILPOINT refresh_mv_force_scheduling_feature_flags_missing")
    node.query("SYSTEM ENABLE FAILPOINT refresh_mv_force_coordination_version_conflict")
    node.query("SYSTEM REFRESH VIEW rdb6.mv")
    time.sleep(3)
    status = node.query(
        "SELECT status FROM system.view_refreshes WHERE view = 'mv' AND database = 'rdb6'"
    ).strip()
    assert status == "Running", status

    # Resume. The exchange wins the race, the attempt reaches Finished, and the next scheduling pass
    # runs the Finished reconciliation on the give-up path, where the set() hits ZBADVERSION. With the
    # fix that conflict is terminal and the view converges to Disabled; without it the pass throws,
    # reschedules, and re-enters the same stale-version give-up forever (never Disabled). A bounded
    # poll is used instead of an unbounded WAIT VIEW so the missing-fix case fails instead of hanging.
    node.query("SYSTEM DISABLE FAILPOINT refresh_mv_pause_after_interrupt_check")

    status = None
    for _ in range(120):
        status = node.query(
            "SELECT status, exception FROM system.view_refreshes WHERE view = 'mv' AND database = 'rdb6'"
        )
        if "Disabled" in status:
            break
        time.sleep(0.5)
    assert "Disabled" in status, status
    assert "multi-read" in status.lower() or "multi_read" in status.lower(), status

    # A lost-ownership ZBADVERSION is a normal Keeper error caught by the give-up path, not a
    # scheduling-thread abort.
    aborts_after = int(node.count_in_log("Unexpected exception in refresh scheduling"))
    assert aborts_after == aborts_before, (aborts_before, aborts_after)

    # The view stays Disabled (no oscillation back into Scheduling on the next poke).
    node.query("SYSTEM REFRESH VIEW rdb6.mv")
    time.sleep(3)
    status = node.query(
        "SELECT status FROM system.view_refreshes WHERE view = 'mv' AND database = 'rdb6'"
    ).strip()
    assert status == "Disabled", status

    node.query("SYSTEM DISABLE FAILPOINT refresh_mv_force_coordination_version_conflict")
    node.query("SYSTEM DISABLE FAILPOINT refresh_mv_force_scheduling_feature_flags_missing")
    node.query("DROP DATABASE rdb6 SYNC")
