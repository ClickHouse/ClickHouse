import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_logs_contain_with_retry

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True)

# Two ways a retryable error can interrupt background loading of outdated parts:
#   - thrown by the worker while loading the part (loadDataPartWithRetries)
#   - thrown by the runner while scheduling the worker (ThreadPool::scheduleOrThrow)
# Both must reschedule the task and keep the failed part in the queue, not drop it.
WORKER_FAILPOINT = "mergetree_load_outdated_parts_inject_retryable_exception"
SCHEDULE_FAILPOINT = "mergetree_load_outdated_parts_inject_schedule_failure"


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.mark.parametrize("failpoint", [WORKER_FAILPOINT, SCHEDULE_FAILPOINT])
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

    # Inject a retryable failure into the outdated-parts loader, then force the loader to run.
    node.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
    node.query("DETACH TABLE t_outdated")
    node.query("ATTACH TABLE t_outdated")

    # The injected exception is caught and the loading task is rescheduled instead of terminating.
    # The loader runs on a background scheduling-pool thread, so poll the log instead of checking once.
    assert_logs_contain_with_retry(
        node, "Loading of outdated parts was interrupted by a retryable error"
    )

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

    node.query("DROP TABLE t_outdated SYNC")
