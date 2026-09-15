import concurrent.futures
import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# The default `background_schedule_pool_size` is required: with a single pool thread the second
# executor of `ReplicatedMergeTreeRestartingThread::run` this test needs cannot exist at all.
node = cluster.add_instance("node", with_zookeeper=True, stay_alive=True)

FAILPOINT = "rmt_restarting_thread_pause_after_activation"
SESSION_QUERY = "SELECT client_id FROM system.zookeeper_connection WHERE name = 'default'"
RESTART_TASK_QUERY = (
    "SELECT count() FROM system.background_schedule_pool "
    "WHERE table = 't' AND log_name LIKE '%ReplicatedMergeTreeRestartingThread%'"
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def wait_failpoint_paused(instance, failpoint, timeout=60):
    """Block until a thread parks at `failpoint`.

    `SYSTEM WAIT FAILPOINT ... PAUSE` blocks, so it runs on a worker thread: a failpoint that is
    never reached must fail the test rather than hang it. The executor is not joined on the failure
    path, because its worker is still stuck inside the blocking query."""
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    future = pool.submit(instance.query, f"SYSTEM WAIT FAILPOINT {failpoint} PAUSE")
    done, _ = concurrent.futures.wait([future], timeout=timeout)
    if not done:
        pool.shutdown(wait=False, cancel_futures=True)
        raise AssertionError(f"failpoint {failpoint} was not reached within {timeout}s")
    pool.shutdown(wait=False)
    future.result()


def test_session_expires_during_attach_path_activation(started_cluster):
    node.query("DROP TABLE IF EXISTS t SYNC")
    node.query(
        "CREATE TABLE t (k UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/t', 'r1') ORDER BY k"
    )
    node.query("INSERT INTO t VALUES (1)")

    # `DETACH` is the lever back onto the attach path: `ATTACH` creates an `attach_thread`, whose
    # `startupImpl(from_attach_thread=true)` calls `restarting_thread.run` inline instead of
    # letting the pool run it. The failpoint is enabled only now, because `CREATE TABLE` waits on
    # `startup_event` and would block forever on a pause inside its own first activation.
    node.query("DETACH TABLE t")
    node.query(f"SYSTEM ENABLE FAILPOINT {FAILPOINT}")

    attach_pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    attach_future = attach_pool.submit(node.query, "ATTACH TABLE t")
    try:
        # Reaching the pause proves `setNotReadonly` has run while `first_time` is still set.
        wait_failpoint_paused(node, FAILPOINT)

        old_session = node.query(SESSION_QUERY)
        # Finalizing the shared session leaves the handle the storage latched in `setZooKeeper`
        # expired, which is what makes the parked activation's state inconsistent. A lost
        # connection here is already a symptom of the abort this test is about, so the verdict is
        # left to the log checks below, which do not need a live server.
        try:
            node.query("SYSTEM RECONNECT ZOOKEEPER")
        except Exception:
            pass

        # Finalization fires the watch `queue_updating_task` installed, so it re-runs, throws
        # `ZSESSIONEXPIRED` and calls `restarting_thread.wakeup`. Waiting for that line is what
        # makes the second executor's arming observed rather than assumed, and it is what stops
        # this test from passing vacuously if the session was never actually replaced.
        node.wait_for_log_line("queueUpdatingTask.*Session expired", timeout=60)

        # The refused schedule is the whole mechanism, so observe it rather than infer it from the
        # absence of an abort: a deactivated task is in none of the pool's collections, so it does
        # not appear in `system.background_schedule_pool`, while one the `wakeup` managed to schedule
        # does. Polling also hands the pool worker its window, since the line waited for above is
        # logged before `wakeup` is even called.
        deadline = time.monotonic() + 30
        samples = 0
        while time.monotonic() < deadline:
            try:
                queued = node.query(RESTART_TASK_QUERY).strip()
            except Exception:
                # A dead server here is the abort this test is about, and the log check below is the
                # verdict for it. Anything else is transient, so it costs one sample rather than the
                # rest of the window; a window that observed nothing is reported after that verdict.
                time.sleep(0.5)
                continue
            samples += 1
            assert queued == "0", (
                "the restarting thread's task was scheduled while the inline activation was still "
                f"parked (system.background_schedule_pool rows: {queued})"
            )
            time.sleep(0.5)
    finally:
        # Releases the parked thread and disarms the failpoint. The server is already dead when
        # the assertion below is the one that fires, so this must not mask it.
        try:
            node.query(f"SYSTEM DISABLE FAILPOINT {FAILPOINT}")
        except Exception:
            pass
        attach_pool.shutdown(wait=False, cancel_futures=True)

    assert not node.contains_in_log("Logical error: 'storage.is_readonly'"), (
        "the restarting thread aborted on chassert(storage.is_readonly): a pool execution of "
        "run observed first_time while the replica was already not readonly"
    )

    assert samples >= 1, (
        "the parked observation window produced no sample in 30s, so it cannot tell a refused schedule "
        "from a query that never ran"
    )

    attach_future.result(timeout=60)
    assert node.query(SESSION_QUERY) != old_session, (
        f"the Keeper session was not replaced (still {old_session!r})"
    )

    # The refused `wakeup` must be coalesced, not lost: the table has to come back on its own.
    node.query_with_retry(
        "SELECT is_readonly FROM system.replicas WHERE table = 't'",
        check_callback=lambda res: res.strip() == "0",
        retry_count=120,
        sleep_time=0.5,
    )
    node.query("INSERT INTO t VALUES (2)")
    assert node.query("SELECT count() FROM t") == "2\n"

    # Positive control for the poll above: the same query must be able to return a row, so that a
    # zero while parked means "refused" and not "matches nothing". A recovered task is delay-scheduled
    # with `zookeeper_session_expiration_check_period`, so it stays in the pool's collections.
    # `query_with_retry` retries a transient sample but returns its last result once the retries run
    # out, so the value it settles on is what decides the control.
    queued_after_recovery = node.query_with_retry(
        RESTART_TASK_QUERY,
        check_callback=lambda res: res.strip() == "1",
        retry_count=60,
        sleep_time=0.5,
    )
    assert queued_after_recovery.strip() == "1", (
        "the restarting thread's task never came back to system.background_schedule_pool, so the poll "
        f"above cannot tell a refused schedule from a query that matches nothing (rows: {queued_after_recovery!r})"
    )
