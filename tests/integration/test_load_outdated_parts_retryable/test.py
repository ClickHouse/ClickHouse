import os
import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_logs_contain_with_retry

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


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


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
        ["bash", "-c", f"mv {data_path}/all_0_0_0/columns.txt {data_path}/all_0_0_0/columns.txt.bak"]
    )

    node.query(f"SYSTEM ENABLE FAILPOINT {UNEXPECTED_POST_LOAD_FAILPOINT}")
    node.query(f"DETACH TABLE {table}")
    # The replicated attach thread waits for the unexpected parts to be loaded, so ATTACH blocks while
    # the failpoint keeps firing. Run it asynchronously and clear the failpoint once retries are seen.
    attach = node.get_query_request(f"ATTACH TABLE {table}")

    # The injected exception is caught and the unexpected-parts loading task is rescheduled instead of
    # terminating the server.
    assert_logs_contain_with_retry(
        node, "Loading of unexpected parts was interrupted by a retryable error"
    )

    # While the failpoint keeps firing the loader must KEEP retrying the same part (it backs off and
    # reschedules every ~100ms). The done-marker is `finished`, so the part is re-attempted every pass.
    # With the pre-fix `load_state.part` marker the retry would skip the part and mark loading finished
    # after a single failure, so the interrupt log would stop at one occurrence. Require several retries.
    retries = 0
    for _ in range(60):
        retries = int(
            node.count_in_log(
                "Loading of unexpected parts was interrupted by a retryable error"
            )
        )
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
