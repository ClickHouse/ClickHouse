import concurrent.futures
import os
import time

import pytest

from helpers.client import QueryTimeoutExceedException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True, with_zookeeper=True)

# Three ways a retryable error can interrupt background loading of outdated parts:
#   - thrown by the worker while loading the part (loadDataPartWithRetries)
#   - thrown by the runner while scheduling the worker (ThreadPool::scheduleOrThrow)
#   - thrown after the part was loaded, while doing its post-load cleanup (preparePartForRemoval, ...)
# All must reschedule the task and keep the failed part in the queue, not drop it.
WORKER_FAILPOINT = "mergetree_load_outdated_parts_inject_retryable_exception"
SCHEDULE_FAILPOINT = "mergetree_load_outdated_parts_inject_schedule_failure"
POST_LOAD_FAILPOINT = "mergetree_load_outdated_parts_inject_post_load_retryable_exception"
# loadUnexpectedDataParts has the same contract: a retryable error thrown after the part is loaded
# but before its optional broken-on-start detach finishes must reschedule, not skip the part.
UNEXPECTED_POST_LOAD_FAILPOINT = (
    "mergetree_load_unexpected_parts_inject_post_load_retryable_exception"
)
# Fire a retryable error AFTER a cleanup step already moved the part's on-disk directory
# (renameToDetached for broken parts, remove() for duplicates). The original path is gone at that
# point, so requeueing+reloading would chase a moved/missing path; the loaders must fail fast instead.
OUTDATED_POST_CLEANUP_MOVE_FAILPOINT = (
    "mergetree_load_outdated_parts_inject_post_cleanup_move_retryable_exception"
)
UNEXPECTED_POST_CLEANUP_MOVE_FAILPOINT = (
    "mergetree_load_unexpected_parts_inject_post_cleanup_move_retryable_exception"
)
# Fire a retryable error in the PRE-move phase of a directory-moving cleanup step (before renameToDetached /
# remove() actually call moveDirectory - e.g. while the zero-copy can_remove check talks to Keeper). The
# original directory is still in place at that point, so this MUST be retried (requeue + reschedule), NOT
# failed fast. This is the regression for the over-eager fail-fast that set the moved flag before the move.
PART_CLEANUP_PRE_MOVE_FAILPOINT = (
    "mergetree_part_cleanup_inject_pre_move_retryable_exception"
)
# loadUnexpectedDataParts schedules one worker per unexpected part. A retryable error while scheduling a
# later worker (CANNOT_SCHEDULE_TASK) must reschedule the task (pure scheduling pressure) WITHOUT dropping
# an error already stored by an earlier worker. These two failpoints reproduce that race: the first makes
# an already-scheduled worker fail non-retryably (a real inconsistent-part error), the second makes a later
# schedule fail retryably (it only fires once a worker has been scheduled, so the worker error is stored first).
UNEXPECTED_SCHEDULE_FAILPOINT = "mergetree_load_unexpected_parts_inject_schedule_failure"
UNEXPECTED_WORKER_NONRETRYABLE_FAILPOINT = (
    "mergetree_load_unexpected_parts_inject_worker_nonretryable_exception"
)
# Makes only the FIRST scheduled unexpected-part worker fail retryably. Paired with the non-retryable
# failpoint (which fires for every other worker), this reproduces a retryable worker error stored ahead of
# a later non-retryable one in scheduling order, so the drain must not surface the retryable error first.
UNEXPECTED_WORKER_RETRYABLE_FAILPOINT = (
    "mergetree_load_unexpected_parts_inject_worker_retryable_exception"
)
# Pauseable failpoints used to choreograph the cancellation-vs-requeue lock-ordering test below. The first
# parks a worker in its retryable catch just before it takes outdated_data_parts_mutex; the second parks the
# dispatch loop (after a worker was scheduled) just before it checks the cancel flag under that same mutex.
# Together they let the test hold a retrying worker in flight while the loader enters the cancellation path.
WORKER_RETRYABLE_CATCH_PAUSE = (
    "mergetree_load_outdated_parts_pause_in_worker_retryable_catch"
)
BEFORE_CANCEL_CHECK_PAUSE = "mergetree_load_outdated_parts_pause_before_cancel_check"

# Every retryable interrupt in loadOutdatedDataParts logs this single line, regardless of which of the
# three failpoints fired. loadUnexpectedDataParts logs its own line.
OUTDATED_INTERRUPT_LOG = "Loading of outdated parts was interrupted by a retryable error"
UNEXPECTED_INTERRUPT_LOG = "Loading of unexpected parts was interrupted by a retryable error"
# Both loaders log this just before std::terminate when they decide to fail fast.
TERMINATE_LOG = "Will terminate to avoid undefined behaviour due to inconsistent set of parts"


def wait_for_process_stop(node, timeout=120):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if node.get_process_pid("clickhouse") is None:
            return
        time.sleep(0.5)
    raise AssertionError(f"clickhouse process did not stop within {timeout}s")


def wait_for_log_count_above(node, substring, baseline, timeout=30):
    # The cases run on one shared server log, so a presence check (contains_in_log) for a later case can be
    # satisfied by the identical line an earlier case already wrote. Count instead, against a baseline taken
    # right before the failpoint was enabled, so each case proves its OWN retry path fired.
    deadline = time.time() + timeout
    count = baseline
    while time.time() < deadline:
        count = int(node.count_in_log(substring))
        if count > baseline:
            return count
        time.sleep(0.5)
    raise AssertionError(
        f"'{substring}' count did not increase above {baseline} within {timeout}s (last seen {count})"
    )


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        # The post-cleanup-move cases intentionally std::terminate the server (fail-fast on an
        # unsafe-to-retry error), leaving a <Fatal> line in the log, so do not treat it as a crash.
        cluster.shutdown(ignore_fatal=True)


@pytest.mark.parametrize(
    "failpoint", [WORKER_FAILPOINT, SCHEDULE_FAILPOINT, POST_LOAD_FAILPOINT]
)
def test_retryable_exception_while_loading_outdated_parts_does_not_terminate(failpoint):
    # A transient retryable error (MEMORY_LIMIT_EXCEEDED, CANNOT_SCHEDULE_TASK, ...) thrown while
    # loading outdated parts in the background must not take down the whole server: the catch(...) in
    # MergeTreeData::loadOutdatedDataParts used to call std::terminate() unconditionally. It must
    # also not drop the failed parts from the queue, otherwise they stay untracked and never removed.
    node.query("DROP TABLE IF EXISTS t_outdated SYNC")
    node.query(
        """
        CREATE TABLE t_outdated (a UInt64, b String) ENGINE = MergeTree ORDER BY a
        SETTINGS old_parts_lifetime = 600, merge_tree_clear_old_parts_interval_seconds = 600
        """
    )
    # Produce outdated parts: the source parts of a merge stay around (inactive) until
    # old_parts_lifetime expires.
    node.query("INSERT INTO t_outdated SELECT number, toString(number) FROM numbers(1000)")
    node.query("INSERT INTO t_outdated SELECT number, toString(number) FROM numbers(1000, 1000)")
    node.query("INSERT INTO t_outdated SELECT number, toString(number) FROM numbers(2000, 1000)")
    node.query("OPTIMIZE TABLE t_outdated FINAL")
    outdated_before = int(
        node.query("SELECT count() FROM system.parts WHERE table = 't_outdated' AND active = 0")
    )
    assert outdated_before > 0

    # All three parametrized cases share one module-scoped server, so the interrupt line from an earlier
    # case is already in the log. Snapshot the count before enabling THIS failpoint and require it to grow,
    # so the assertion proves this case's own retry path fired (and times out if the failpoint never does).
    interrupts_before = int(node.count_in_log(OUTDATED_INTERRUPT_LOG))

    # Inject a retryable failure into the outdated-parts loader, then force the loader to run.
    node.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
    node.query("DETACH TABLE t_outdated")
    node.query("ATTACH TABLE t_outdated")

    # The injected exception is caught and the loading task is rescheduled instead of terminating.
    # The loader runs on a background scheduling-pool thread, so poll the log instead of checking once.
    wait_for_log_count_above(node, OUTDATED_INTERRUPT_LOG, interrupts_before)

    # The server must still be alive and serving queries (active parts load synchronously at ATTACH).
    assert node.query("SELECT count() FROM t_outdated").strip() == "3000"

    # While the failpoint fires the parts stay unloaded, so they are not in system.parts yet.
    assert (
        int(node.query("SELECT count() FROM system.parts WHERE table = 't_outdated' AND active = 0"))
        == 0
    )

    # Clear the transient condition and wait for the SAME background task to finish (no second
    # DETACH/ATTACH). If the failed parts had been dropped from the queue, the loader would mark
    # loading finished without ever processing them, leaving those outdated parts untracked.
    node.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
    node.query("SYSTEM WAIT LOADING PARTS t_outdated", timeout=60)

    # All the outdated parts were retried and are tracked again (none were leaked by the
    # interrupted run). They are kept as Outdated until old_parts_lifetime expires.
    outdated_after = int(
        node.query("SELECT count() FROM system.parts WHERE table = 't_outdated' AND active = 0")
    )
    assert outdated_after == outdated_before
    assert node.query("SELECT count() FROM t_outdated").strip() == "3000"

    # The POST_LOAD failpoint fires after loadDataPartWithRetries already published the Outdated part
    # into data_parts_indexes. Requeueing it without rolling it back would make the retry reload the
    # same directory as a fresh part, hit the duplicate-part path ("Duplicate part ..."), and
    # res.part->remove() could delete the directory still referenced by the published part. The retry
    # must instead roll the published part back and reload it cleanly, so no part is ever detached.
    assert (
        int(node.query("SELECT count() FROM system.detached_parts WHERE table = 't_outdated'")) == 0
    )

    node.query("DROP TABLE t_outdated SYNC")


def test_retryable_exception_while_loading_unexpected_parts_does_not_terminate():
    # loadUnexpectedDataParts has the same retry contract as loadOutdatedDataParts. A retryable error
    # thrown after the part is loaded but before its broken-on-start detach finishes used to take down
    # the server (catch(...) -> std::terminate). It must reschedule and KEEP RETRYING the same part:
    # the done-marker is the `finished` flag, not `load_state.part` (which is set before the detach).
    # With the pre-fix `load_state.part` marker the retry would skip the part and mark loading finished
    # after a single failure, instead of retrying until the transient error clears.
    table = "t_unexpected"
    zk_path = "/clickhouse/tables/t_unexpected"
    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node.query(
        f"""
        CREATE TABLE {table} (key UInt64) ENGINE = ReplicatedMergeTree('{zk_path}', '1') ORDER BY key
        SETTINGS max_suspicious_broken_parts = 0, replicated_max_ratio_of_wrong_parts = 0
        """
    )
    node.query(f"INSERT INTO {table} SELECT number FROM numbers(1000)")
    node.query(f"INSERT INTO {table} SELECT number FROM numbers(1000, 1000)")
    # Merge the two inserted parts. The merged part is committed to ZooKeeper and stays active; the
    # source part all_0_0_0 becomes inactive but is still on disk.
    node.query(f"OPTIMIZE TABLE {table} FINAL")

    data_path = node.query(
        f"SELECT arrayElement(data_paths, 1) FROM system.tables WHERE name = '{table}'"
    ).strip()

    # Drop the source part from ZooKeeper (no longer expected) and corrupt it on disk. On the next
    # ATTACH it is found on disk, is not expected, and fails to load, so loadUnexpectedDataParts loads
    # it as a broken unexpected part.
    zk = cluster.get_kazoo_client("zoo1")
    zk.delete(os.path.join(zk_path, "replicas/1/parts", "all_0_0_0"))
    node.exec_in_container(
        ["mv", f"{data_path}/all_0_0_0/columns.txt", f"{data_path}/all_0_0_0/columns.txt.bak"]
    )

    # Snapshot the interrupt count before enabling the failpoint so the assertion below counts only the
    # retries triggered by THIS failpoint, not any line left by an earlier run of the module.
    interrupts_before = int(node.count_in_log(UNEXPECTED_INTERRUPT_LOG))

    node.query(f"SYSTEM ENABLE FAILPOINT {UNEXPECTED_POST_LOAD_FAILPOINT}")
    node.query(f"DETACH TABLE {table}")
    # The replicated attach thread waits for the unexpected parts to be loaded, so ATTACH blocks while
    # the failpoint keeps firing. Run it asynchronously and clear the failpoint once retries are seen.
    attach = node.get_query_request(f"ATTACH TABLE {table}")

    # While the failpoint keeps firing the loader must KEEP retrying the same part (it backs off and
    # reschedules every ~100ms). The done-marker is `finished`, so the part is re-attempted every pass.
    # With the pre-fix `load_state.part` marker the retry would skip the part and mark loading finished
    # after a single failure, so only one new interrupt line would appear. Require several NEW retries.
    retries = 0
    for _ in range(60):
        retries = int(node.count_in_log(UNEXPECTED_INTERRUPT_LOG)) - interrupts_before
        if retries >= 3:
            break
        time.sleep(0.5)
    assert retries >= 3
    # The server stayed up the whole time (it never std::terminate'd on the retryable error).
    assert node.query("SELECT 1").strip() == "1"

    # Clear the transient condition; the loader finishes the part and ATTACH returns. The replicated
    # attach thread blocks in waitForUnexpectedPartsToBeLoaded() until loading is finished, so a
    # returned ATTACH already means the previously-failing part was retried and loaded successfully.
    node.query(f"SYSTEM DISABLE FAILPOINT {UNEXPECTED_POST_LOAD_FAILPOINT}")
    attach.get_answer()
    # The data is intact and the broken part reaches its detached state during the replica sanity check.
    assert node.query(f"SELECT count() FROM {table}").strip() == "2000"
    for _ in range(60):
        detached = node.query(
            f"SELECT count() FROM system.detached_parts WHERE table = '{table}' AND name LIKE '%all_0_0_0%'"
        ).strip()
        if detached == "1":
            break
        time.sleep(0.5)
    assert detached == "1"

    node.query(f"DROP TABLE {table} SYNC")


def test_retryable_exception_after_outdated_cleanup_move_fails_fast():
    # Not every retryable error in the outdated-parts loader can be retried by requeueing the part.
    # The broken-part (renameToDetached) and duplicate (remove()) cleanup steps MOVE the part's
    # on-disk directory before returning, so once the move starts the original path is gone. Requeueing
    # the part would make the retry reload a moved/missing path: for duplicates a transient remove error
    # would turn into the broken-part path and a later terminate, for broken parts the retry would detach
    # the wrong path. The loader must instead fail fast (std::terminate) for these side-effecting failures,
    # exactly as it does for a genuinely inconsistent on-disk part. A restart then reconciles the
    # already-moved directory cleanly, so fail-fast does not cause a crash loop.
    #
    # This exercises the broken-part branch (renameToDetached); the duplicate branch (remove()) shares the
    # same `cleanup_moved_directory` guard and catch, so it fails fast through the identical code.
    node.query("DROP TABLE IF EXISTS t_outdated_move SYNC")
    node.query(
        """
        CREATE TABLE t_outdated_move (a UInt64, b String) ENGINE = MergeTree ORDER BY a
        SETTINGS old_parts_lifetime = 600, merge_tree_clear_old_parts_interval_seconds = 600
        """
    )
    node.query("INSERT INTO t_outdated_move SELECT number, toString(number) FROM numbers(1000)")
    node.query("INSERT INTO t_outdated_move SELECT number, toString(number) FROM numbers(1000, 1000)")
    node.query("INSERT INTO t_outdated_move SELECT number, toString(number) FROM numbers(2000, 1000)")
    # The merge source parts stay on disk as inactive (outdated) parts until old_parts_lifetime expires.
    node.query("OPTIMIZE TABLE t_outdated_move FINAL")

    data_path = node.query(
        "SELECT arrayElement(data_paths, 1) FROM system.tables WHERE name = 't_outdated_move'"
    ).strip()
    # Pick one inactive (outdated) part and corrupt it on disk so the outdated loader fails to load it
    # and marks it broken, which routes it through renameToDetached (a directory-moving cleanup step).
    outdated_part = node.query(
        "SELECT name FROM system.parts WHERE table = 't_outdated_move' AND active = 0 ORDER BY name LIMIT 1"
    ).strip()
    assert outdated_part
    node.exec_in_container(
        ["mv", f"{data_path}/{outdated_part}/columns.txt", f"{data_path}/{outdated_part}/columns.txt.bak"]
    )

    # Snapshot the reschedule-interrupt count before enabling the failpoint. With the fix the loader fails
    # fast for this side-effecting failure, so the count must NOT grow; the pre-fix requeue-on-any-retryable
    # behaviour would log this line (and chase the moved/missing directory) before any terminate.
    interrupts_before = int(node.count_in_log(OUTDATED_INTERRUPT_LOG))

    # Inject a retryable error right after the broken part's directory is moved to detached, then force
    # the background outdated loader to run. The loader must NOT requeue the part (its directory has
    # already moved); it must fail fast and terminate.
    node.query(f"SYSTEM ENABLE FAILPOINT {OUTDATED_POST_CLEANUP_MOVE_FAILPOINT}")
    node.query("DETACH TABLE t_outdated_move")
    node.query("ATTACH TABLE t_outdated_move")

    # The loader runs on a background thread; it logs the fatal line and calls std::terminate.
    node.wait_for_log_line(TERMINATE_LOG, timeout=90)
    wait_for_process_stop(node)
    # The reschedule-interrupt line must NOT appear for this case: a side-effecting cleanup failure is not
    # retried (that is exactly the bug - requeueing would chase the moved/missing directory). This is the
    # discriminator from the pre-fix behaviour, which would requeue and log the line before terminating.
    assert int(node.count_in_log(OUTDATED_INTERRUPT_LOG)) == interrupts_before

    # The directory was already moved to detached before the terminate, so a restart reconciles cleanly
    # (no crash loop) and the data is intact.
    node.start_clickhouse()
    assert node.query("SELECT count() FROM t_outdated_move").strip() == "3000"
    node.query("SYSTEM WAIT LOADING PARTS t_outdated_move", timeout=60)
    assert node.query("SELECT count() FROM t_outdated_move").strip() == "3000"

    node.query("DROP TABLE t_outdated_move SYNC")


def test_retryable_exception_after_unexpected_cleanup_move_fails_fast():
    # loadUnexpectedDataParts has the same side-effecting-cleanup hazard: a retryable error thrown after
    # renameToDetached already moved the broken part's directory cannot be retried by reloading the part
    # on its now-moved path, so the loader must fail fast (std::terminate) rather than reschedule. This is
    # distinct from the post-load (pre-detach) failpoint, which is safely retried (the dir is still there).
    table = "t_unexpected_move"
    zk_path = "/clickhouse/tables/t_unexpected_move"
    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node.query(
        f"""
        CREATE TABLE {table} (key UInt64) ENGINE = ReplicatedMergeTree('{zk_path}', '1') ORDER BY key
        SETTINGS max_suspicious_broken_parts = 0, replicated_max_ratio_of_wrong_parts = 0
        """
    )
    node.query(f"INSERT INTO {table} SELECT number FROM numbers(1000)")
    node.query(f"INSERT INTO {table} SELECT number FROM numbers(1000, 1000)")
    node.query(f"OPTIMIZE TABLE {table} FINAL")

    data_path = node.query(
        f"SELECT arrayElement(data_paths, 1) FROM system.tables WHERE name = '{table}'"
    ).strip()
    # Make all_0_0_0 an unexpected BROKEN part: drop it from ZooKeeper (no longer expected) and remove the
    # file the unexpected loader reads (count.txt) so loadUnexpectedDataPart fails non-retryably and marks
    # it broken, which routes it through renameToDetached (the directory-moving cleanup step we fail fast on).
    zk = cluster.get_kazoo_client("zoo1")
    zk.delete(os.path.join(zk_path, "replicas/1/parts", "all_0_0_0"))
    node.exec_in_container(
        ["mv", f"{data_path}/all_0_0_0/count.txt", f"{data_path}/all_0_0_0/count.txt.bak"]
    )

    # Snapshot the reschedule-interrupt count; with the fix it must NOT grow (fail fast, no requeue).
    interrupts_before = int(node.count_in_log(UNEXPECTED_INTERRUPT_LOG))

    # Inject a retryable error right after the broken unexpected part is detached (its directory moved),
    # then force loading. The loader must fail fast and terminate rather than reschedule the moved part.
    node.query(f"SYSTEM ENABLE FAILPOINT {UNEXPECTED_POST_CLEANUP_MOVE_FAILPOINT}")
    node.query(f"DETACH TABLE {table}")
    # The replicated attach thread blocks until unexpected parts finish loading, so run ATTACH async; the
    # server terminates underneath it.
    node.get_query_request(f"ATTACH TABLE {table}")

    node.wait_for_log_line(TERMINATE_LOG, timeout=90)
    wait_for_process_stop(node)
    # The reschedule-interrupt line must NOT appear: a post-move failure is not retried (discriminator
    # from the pre-fix requeue behaviour, which would log it and reload the moved part before terminating).
    assert int(node.count_in_log(UNEXPECTED_INTERRUPT_LOG)) == interrupts_before

    # Restart reconciles the already-detached directory cleanly and the data on the merged part is intact.
    node.start_clickhouse()
    assert node.query(f"SELECT count() FROM {table}").strip() == "2000"

    node.query(f"DROP TABLE {table} SYNC")


def test_retryable_exception_before_outdated_cleanup_move_reschedules():
    # The inverse of the post-move case above. The directory-moving cleanup steps (renameToDetached for broken
    # parts, remove() for duplicates) do retryable PRE-move work before they touch the directory - for example
    # remove()'s zero-copy can_remove check talks to Keeper, which can fail transiently. A retryable error
    # there leaves the original directory in place, so it MUST be retried (requeue + reschedule), not failed
    # fast. The fix only converts a retryable error to fail-fast once the move has actually started; this test
    # pins that boundary so a regression that fails fast on a transient pre-move error is caught.
    node.query("DROP TABLE IF EXISTS t_outdated_premove SYNC")
    node.query(
        """
        CREATE TABLE t_outdated_premove (a UInt64, b String) ENGINE = MergeTree ORDER BY a
        SETTINGS old_parts_lifetime = 600, merge_tree_clear_old_parts_interval_seconds = 600
        """
    )
    node.query("INSERT INTO t_outdated_premove SELECT number, toString(number) FROM numbers(1000)")
    node.query("INSERT INTO t_outdated_premove SELECT number, toString(number) FROM numbers(1000, 1000)")
    node.query("INSERT INTO t_outdated_premove SELECT number, toString(number) FROM numbers(2000, 1000)")
    node.query("OPTIMIZE TABLE t_outdated_premove FINAL")

    data_path = node.query(
        "SELECT arrayElement(data_paths, 1) FROM system.tables WHERE name = 't_outdated_premove'"
    ).strip()
    # Corrupt one inactive (outdated) part so the loader marks it broken and routes it through the
    # directory-moving renameToDetached cleanup step (where the pre-move failpoint fires).
    outdated_part = node.query(
        "SELECT name FROM system.parts WHERE table = 't_outdated_premove' AND active = 0 ORDER BY name LIMIT 1"
    ).strip()
    assert outdated_part
    node.exec_in_container(
        ["mv", f"{data_path}/{outdated_part}/columns.txt", f"{data_path}/{outdated_part}/columns.txt.bak"]
    )

    interrupts_before = int(node.count_in_log(OUTDATED_INTERRUPT_LOG))

    # Inject a retryable error in the PRE-move phase of the cleanup step. The directory is still in place, so
    # the loader must reschedule (log the interrupt line) and keep the server alive, NOT terminate.
    node.query(f"SYSTEM ENABLE FAILPOINT {PART_CLEANUP_PRE_MOVE_FAILPOINT}")
    node.query("DETACH TABLE t_outdated_premove")
    node.query("ATTACH TABLE t_outdated_premove")

    # The interrupt line must grow (the part was requeued and the task rescheduled); the server stays up.
    wait_for_log_count_above(node, OUTDATED_INTERRUPT_LOG, interrupts_before)
    assert node.query("SELECT 1").strip() == "1"

    # Clear the transient condition; the same background task finishes the (still-present) part.
    node.query(f"SYSTEM DISABLE FAILPOINT {PART_CLEANUP_PRE_MOVE_FAILPOINT}")
    node.query("SYSTEM WAIT LOADING PARTS t_outdated_premove", timeout=60)
    assert node.query("SELECT count() FROM t_outdated_premove").strip() == "3000"

    node.query("DROP TABLE t_outdated_premove SYNC")


def test_retryable_exception_before_unexpected_cleanup_move_reschedules():
    # Same pre-move boundary for loadUnexpectedDataParts: a retryable error from the pre-move work of the
    # broken part's renameToDetached must reschedule and keep retrying (the directory is still in place), not
    # fail fast. Only a retryable error after the move has begun fails fast (the separate post-cleanup-move
    # case above).
    table = "t_unexpected_premove"
    zk_path = "/clickhouse/tables/t_unexpected_premove"
    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node.query(
        f"""
        CREATE TABLE {table} (key UInt64) ENGINE = ReplicatedMergeTree('{zk_path}', '1') ORDER BY key
        SETTINGS max_suspicious_broken_parts = 0, replicated_max_ratio_of_wrong_parts = 0
        """
    )
    node.query(f"INSERT INTO {table} SELECT number FROM numbers(1000)")
    node.query(f"INSERT INTO {table} SELECT number FROM numbers(1000, 1000)")
    node.query(f"OPTIMIZE TABLE {table} FINAL")

    data_path = node.query(
        f"SELECT arrayElement(data_paths, 1) FROM system.tables WHERE name = '{table}'"
    ).strip()
    # Make all_0_0_0 an unexpected BROKEN part (drop from ZK, remove count.txt) so it routes through the
    # directory-moving renameToDetached cleanup step where the pre-move failpoint fires.
    zk = cluster.get_kazoo_client("zoo1")
    zk.delete(os.path.join(zk_path, "replicas/1/parts", "all_0_0_0"))
    node.exec_in_container(
        ["mv", f"{data_path}/all_0_0_0/count.txt", f"{data_path}/all_0_0_0/count.txt.bak"]
    )

    interrupts_before = int(node.count_in_log(UNEXPECTED_INTERRUPT_LOG))

    node.query(f"SYSTEM ENABLE FAILPOINT {PART_CLEANUP_PRE_MOVE_FAILPOINT}")
    node.query(f"DETACH TABLE {table}")
    # The replicated attach thread blocks until unexpected parts finish loading, so run ATTACH async; the
    # server must stay up (reschedule), not terminate.
    attach = node.get_query_request(f"ATTACH TABLE {table}")

    # The loader keeps retrying the pre-move failure while the failpoint fires; require several NEW retries
    # and that the server is still alive.
    retries = 0
    for _ in range(60):
        retries = int(node.count_in_log(UNEXPECTED_INTERRUPT_LOG)) - interrupts_before
        if retries >= 3:
            break
        time.sleep(0.5)
    assert retries >= 3
    assert node.query("SELECT 1").strip() == "1"

    # Clear the transient condition; the broken part's detach finishes and ATTACH returns.
    node.query(f"SYSTEM DISABLE FAILPOINT {PART_CLEANUP_PRE_MOVE_FAILPOINT}")
    attach.get_answer()
    assert node.query(f"SELECT count() FROM {table}").strip() == "2000"

    node.query(f"DROP TABLE {table} SYNC")


def _make_unexpected_parts(table, zk_path):
    # Build a replicated table with several inactive (merge source) parts left on disk, then delete their
    # nodes from ZooKeeper so the next ATTACH sees them as UNEXPECTED parts (on disk, not expected). The
    # merged part keeps all the data, so the unexpected parts are covered and the row count stays correct.
    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node.query(
        f"""
        CREATE TABLE {table} (key UInt64) ENGINE = ReplicatedMergeTree('{zk_path}', '1') ORDER BY key
        SETTINGS replicated_max_ratio_of_wrong_parts = 1, max_suspicious_broken_parts = 100,
                 old_parts_lifetime = 600, merge_tree_clear_old_parts_interval_seconds = 600,
                 cleanup_delay_period = 600, max_cleanup_delay_period = 600
        """
    )
    node.query(f"INSERT INTO {table} SELECT number FROM numbers(1000)")
    node.query(f"INSERT INTO {table} SELECT number FROM numbers(1000, 1000)")
    node.query(f"INSERT INTO {table} SELECT number FROM numbers(2000, 1000)")
    # Merge the three inserted parts into one active part; the sources stay on disk as inactive parts.
    node.query(f"OPTIMIZE TABLE {table} FINAL")

    inactive = (
        node.query(
            f"SELECT name FROM system.parts WHERE table = '{table}' AND active = 0 ORDER BY name"
        )
        .strip()
        .splitlines()
    )
    zk = cluster.get_kazoo_client("zoo1")
    deleted = 0
    for name in inactive:
        zk_part = os.path.join(zk_path, "replicas/1/parts", name)
        if zk.exists(zk_part):
            zk.delete(zk_part)
            deleted += 1
    # The schedule-failure failpoint only fires once a worker has already been scheduled, so the test needs
    # at least two unexpected parts: one to schedule a worker, one whose scheduling then fails.
    assert deleted >= 2


def test_nonretryable_worker_error_not_masked_by_retryable_schedule_failure():
    # loadUnexpectedDataParts schedules one worker per unexpected part. If an already-scheduled worker stores
    # a non-retryable (genuinely inconsistent-part) error and then scheduling a LATER worker fails with a
    # retryable CANNOT_SCHEDULE_TASK, the loader must still fail fast on the worker error. Before the fix the
    # local runner's destructor only waited (did not rethrow) while the scheduling error unwound straight to
    # the function-level catch, which saw only the retryable error and rescheduled, masking the real
    # inconsistency. Now the runner is drained and the worker error rethrown before the scheduling error.
    table = "t_unexpected_mask"
    zk_path = "/clickhouse/tables/t_unexpected_mask"
    _make_unexpected_parts(table, zk_path)

    # Snapshot the reschedule-interrupt count; with the fix it must NOT grow (fail fast on the worker error).
    interrupts_before = int(node.count_in_log(UNEXPECTED_INTERRUPT_LOG))

    # First scheduled worker fails non-retryably; scheduling the next worker then fails retryably.
    node.query(f"SYSTEM ENABLE FAILPOINT {UNEXPECTED_WORKER_NONRETRYABLE_FAILPOINT}")
    node.query(f"SYSTEM ENABLE FAILPOINT {UNEXPECTED_SCHEDULE_FAILPOINT}")
    node.query(f"DETACH TABLE {table}")
    # The replicated attach thread blocks until unexpected parts finish loading, so run ATTACH async; the
    # server fails fast and terminates underneath it.
    node.get_query_request(f"ATTACH TABLE {table}")

    node.wait_for_log_line(TERMINATE_LOG, timeout=90)
    wait_for_process_stop(node)
    # The retryable scheduling error must NOT have caused a reschedule: the worker's non-retryable error is
    # drained and rethrown first. The pre-fix code masked it (the scheduling error rescheduled) and would
    # have logged this line before looping forever instead of terminating.
    assert int(node.count_in_log(UNEXPECTED_INTERRUPT_LOG)) == interrupts_before

    # A restart with no failpoints loads the (valid, covered) unexpected parts cleanly; data is intact.
    node.start_clickhouse()
    assert node.query(f"SELECT count() FROM {table}").strip() == "3000"

    node.query(f"DROP TABLE {table} SYNC")


def test_nonretryable_worker_error_not_masked_by_retryable_worker_error():
    # loadUnexpectedDataParts runs one worker per unexpected part concurrently. If an earlier-scheduled worker
    # fails retryably (MEMORY_LIMIT_EXCEEDED) and a later worker fails non-retryably (a genuinely inconsistent
    # part), the loader must still fail fast. Before the fix the workers let retryable errors escape into their
    # futures, and the drain (waitForAllToFinishAndRethrowFirstError) rethrows the FIRST future error in
    # scheduling order - the earlier worker's retryable one - so the function-level catch saw a retryable error
    # and rescheduled, masking the later non-retryable inconsistency. Now a worker catches its own retryable
    # error and only sets a retry flag, so only the non-retryable error escapes and is rethrown by the drain.
    table = "t_unexpected_worker_mask"
    zk_path = "/clickhouse/tables/t_unexpected_worker_mask"
    _make_unexpected_parts(table, zk_path)

    # Snapshot the reschedule-interrupt count; with the fix it must NOT grow (fail fast on the worker error).
    interrupts_before = int(node.count_in_log(UNEXPECTED_INTERRUPT_LOG))

    # The first scheduled worker fails retryably; every later worker fails non-retryably. The first worker's
    # retryable error is stored ahead of the non-retryable one in scheduling order.
    node.query(f"SYSTEM ENABLE FAILPOINT {UNEXPECTED_WORKER_RETRYABLE_FAILPOINT}")
    node.query(f"SYSTEM ENABLE FAILPOINT {UNEXPECTED_WORKER_NONRETRYABLE_FAILPOINT}")
    node.query(f"DETACH TABLE {table}")
    # The replicated attach thread blocks until unexpected parts finish loading, so run ATTACH async; the
    # server fails fast and terminates underneath it.
    node.get_query_request(f"ATTACH TABLE {table}")

    node.wait_for_log_line(TERMINATE_LOG, timeout=90)
    wait_for_process_stop(node)
    # The earlier worker's retryable error must NOT have caused a reschedule: it is caught inside the worker
    # (only a retry flag is set, no exception in its future), so the drain rethrows the later worker's
    # non-retryable error and the server terminates. The pre-fix code rethrew the retryable error first and
    # would have logged this reschedule line before looping forever instead of failing fast.
    assert int(node.count_in_log(UNEXPECTED_INTERRUPT_LOG)) == interrupts_before

    # A restart with no failpoints loads the (valid, covered) unexpected parts cleanly; data is intact.
    node.start_clickhouse()
    assert node.query(f"SELECT count() FROM {table}").strip() == "3000"

    node.query(f"DROP TABLE {table} SYNC")


def test_retryable_schedule_failure_while_loading_unexpected_parts_reschedules():
    # The other half of the same contract: pure scheduling pressure (a retryable CANNOT_SCHEDULE_TASK with
    # no worker error) must still reschedule the task and eventually load every part, not fail fast. This is
    # the legitimate retry path the masking fix above must not break.
    table = "t_unexpected_sched"
    zk_path = "/clickhouse/tables/t_unexpected_sched"
    _make_unexpected_parts(table, zk_path)

    interrupts_before = int(node.count_in_log(UNEXPECTED_INTERRUPT_LOG))

    node.query(f"SYSTEM ENABLE FAILPOINT {UNEXPECTED_SCHEDULE_FAILPOINT}")
    node.query(f"DETACH TABLE {table}")
    # ATTACH blocks until loading finishes; the loader reschedules on each scheduling failure and makes
    # progress one part per pass, so run it async and watch for the reschedule line.
    attach = node.get_query_request(f"ATTACH TABLE {table}")

    # The reschedule path fired (server stayed up instead of terminating).
    wait_for_log_count_above(node, UNEXPECTED_INTERRUPT_LOG, interrupts_before)
    assert node.query("SELECT 1").strip() == "1"

    # Clear the scheduling pressure; the remaining parts schedule in one pass and ATTACH returns.
    node.query(f"SYSTEM DISABLE FAILPOINT {UNEXPECTED_SCHEDULE_FAILPOINT}")
    attach.get_answer()
    assert node.query(f"SELECT count() FROM {table}").strip() == "3000"

    node.query(f"DROP TABLE {table} SYNC")


def test_cancellation_while_worker_requeues_does_not_deadlock():
    # A retrying worker requeues its part by taking outdated_data_parts_mutex. The dispatch loop's
    # cancellation branch must NOT wait for the runner workers while holding that same mutex: a worker
    # blocked on the mutex would never finish, the loader would wait for it forever, and the thread that
    # canceled (server shutdown / table drop, via deactivate()) would wait for the loader -> shutdown/drop
    # hangs. The fix drains the runner after the dispatch loop, outside the mutex. This test pins a worker
    # in its retryable catch (just before it locks the mutex) and the loop just before its cancel check,
    # then cancels via DROP and resumes both; the DROP must complete instead of hanging.
    table = "t_outdated_cancel"
    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    node.query(
        f"""
        CREATE TABLE {table} (a UInt64, b String) ENGINE = MergeTree ORDER BY a
        SETTINGS old_parts_lifetime = 600, merge_tree_clear_old_parts_interval_seconds = 600
        """
    )
    # Several outdated parts so the dispatch loop schedules a worker and then loops again (where it parks).
    node.query(f"INSERT INTO {table} SELECT number, toString(number) FROM numbers(1000)")
    node.query(f"INSERT INTO {table} SELECT number, toString(number) FROM numbers(1000, 1000)")
    node.query(f"INSERT INTO {table} SELECT number, toString(number) FROM numbers(2000, 1000)")
    node.query(f"OPTIMIZE TABLE {table} FINAL")

    # The worker hits a retryable error (so it reaches the requeue path that takes outdated_data_parts_mutex)
    # and pauses just before locking it. The dispatch loop pauses just before the cancel check (also under
    # that mutex) once a worker has been scheduled.
    node.query(f"SYSTEM ENABLE FAILPOINT {WORKER_FAILPOINT}")
    node.query(f"SYSTEM ENABLE FAILPOINT {WORKER_RETRYABLE_CATCH_PAUSE}")
    node.query(f"SYSTEM ENABLE FAILPOINT {BEFORE_CANCEL_CHECK_PAUSE}")

    node.query(f"DETACH TABLE {table}")
    node.query(f"ATTACH TABLE {table}")

    # Wait until both the worker (in its retryable catch) and the dispatch loop (before the cancel check)
    # have parked. After this the worker holds no lock and the loop holds no lock; both are about to take
    # outdated_data_parts_mutex.
    node.query(f"SYSTEM WAIT FAILPOINT {WORKER_RETRYABLE_CATCH_PAUSE} PAUSE", timeout=60)
    node.query(f"SYSTEM WAIT FAILPOINT {BEFORE_CANCEL_CHECK_PAUSE} PAUSE", timeout=60)

    # Cancel the loading task the same way shutdown/drop does. DROP calls shutdown() ->
    # stopOutdatedAndUnexpectedDataPartsLoadingTask() which sets the cancel flag and then deactivate()s the
    # task (waiting for this in-flight loader run). Run it async: pre-fix it deadlocks and never returns. Do
    # not use `with` here: on the deadlock path the DROP thread never returns, and the executor's exit would
    # block forever joining it, so shut down with wait=False on every exit.
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    try:
        drop = executor.submit(lambda: node.query(f"DROP TABLE {table}", timeout=120))

        # Give the DROP a moment to set the cancel flag and block in deactivate() waiting for the loader.
        time.sleep(3)

        # Resume the dispatch loop FIRST: it takes the mutex, sees the cancel flag, and (pre-fix) waits for
        # the runner while still holding the mutex; (post-fix) breaks out and drains after releasing it. The
        # worker is still parked, so the loop's mutex acquisition is uncontended here.
        node.query(f"SYSTEM DISABLE FAILPOINT {BEFORE_CANCEL_CHECK_PAUSE}")
        # Give the (resumed) loop time to reach the cancel branch and, pre-fix, block in
        # waitForAllToFinish while holding the mutex. Without this the worker could win the mutex first,
        # requeue, and finish before the loop waits, hiding the pre-fix deadlock (false pass on the buggy
        # binary). The worker stays parked until the disable below, so this ordering is deterministic.
        time.sleep(3)
        # Resume the worker: it now tries to take the mutex to requeue its part. Pre-fix the loop holds the
        # mutex and waits for this worker -> deadlock; post-fix the loop already released the mutex.
        node.query(f"SYSTEM DISABLE FAILPOINT {WORKER_RETRYABLE_CATCH_PAUSE}")

        try:
            drop.result(timeout=90)
        except (concurrent.futures.TimeoutError, QueryTimeoutExceedException) as e:
            raise AssertionError(
                "DROP TABLE hung: cancellation deadlocked with the worker requeue path "
                "(the loader waited for the runner while holding outdated_data_parts_mutex)"
            ) from e
    finally:
        executor.shutdown(wait=False)

    # The server is still responsive (it never deadlocked its background pool / shutdown path).
    assert node.query("SELECT 1").strip() == "1"

    node.query(f"SYSTEM DISABLE FAILPOINT {WORKER_FAILPOINT}")
    node.query(f"DROP TABLE IF EXISTS {table} SYNC")
