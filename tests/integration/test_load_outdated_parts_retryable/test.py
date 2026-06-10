import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True)

FAILPOINT = "mergetree_load_outdated_parts_inject_retryable_exception"


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_retryable_exception_while_loading_outdated_parts_does_not_terminate():
    # A transient retryable error (MEMORY_LIMIT_EXCEEDED, network, ...) thrown while loading
    # outdated parts in the background must not take down the whole server: the catch(...) in
    # MergeTreeData::loadOutdatedDataParts used to call std::terminate() unconditionally.
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
    assert int(node.query("SELECT count() FROM system.parts WHERE table = 't_outdated' AND active = 0")) > 0

    # Inject a retryable failure into the outdated-parts loader, then force the loader to run.
    node.query(f"SYSTEM ENABLE FAILPOINT {FAILPOINT}")
    node.query("DETACH TABLE t_outdated")
    node.query("ATTACH TABLE t_outdated")

    # The injected exception is caught and the loading task is rescheduled instead of terminating.
    assert node.contains_in_log("Loading of outdated parts was interrupted by a retryable error")

    # The server must still be alive and serving queries.
    assert node.query("SELECT count() FROM t_outdated").strip() == "3000"

    # Once the transient condition clears, outdated-parts loading completes normally.
    node.query(f"SYSTEM DISABLE FAILPOINT {FAILPOINT}")
    node.query("DETACH TABLE t_outdated")
    node.query("ATTACH TABLE t_outdated")
    assert node.query("SELECT count() FROM t_outdated").strip() == "3000"

    node.query("DROP TABLE t_outdated SYNC")
