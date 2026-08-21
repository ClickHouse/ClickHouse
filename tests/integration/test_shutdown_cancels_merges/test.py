#!/usr/bin/env python3

import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance("node", stay_alive=True)
node1 = cluster.add_instance("node1", with_zookeeper=True)
node2 = cluster.add_instance("node2", with_zookeeper=True, stay_alive=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_shutdown_cancels_running_merge(started_cluster):
    """A graceful shutdown must cancel in-flight merges instead of waiting for
    them to finish. The `merge_tree_sequential_source_sleep_before_read`
    failpoint makes every reading step of the merge sleep for 10 seconds, so
    the merge of 16 parts runs for at least 160 seconds. Without cancellation
    the server cannot stop before the timeout below (the background executor's
    `wait` does not interrupt running tasks, and per-storage cancellation
    happens only after that wait). See the `Possible deadlock on shutdown`
    stress test failures where a merge applying patch parts blocked shutdown:
    https://s3.amazonaws.com/clickhouse-test-reports/json.html?REF=master&sha=a013c79d6ac80373b820381527ea85823ab03834&name_0=MasterCI&name_1=Stress%20test%20%28azure%2C%20amd_msan%29
    """
    node.query(
        """
        CREATE TABLE t_shutdown_merge (k UInt64, v UInt64)
        ENGINE = MergeTree ORDER BY k
        SETTINGS min_parts_to_merge_at_once = 16
        """
    )

    node.query("SYSTEM STOP MERGES t_shutdown_merge")

    for i in range(16):
        node.query(f"INSERT INTO t_shutdown_merge SELECT number + {i} * 10000, number FROM numbers(10000)")

    node.query("SYSTEM ENABLE FAILPOINT merge_tree_sequential_source_sleep_before_read")
    node.query("SYSTEM START MERGES t_shutdown_merge")

    # Wait until the background merge picks up the parts. With
    # min_parts_to_merge_at_once = 16 the only merge the selector can assign
    # takes all 16 parts at once.
    for _ in range(300):
        num_parts = node.query("SELECT num_parts FROM system.merges WHERE table = 't_shutdown_merge'").strip()
        if num_parts:
            assert int(num_parts) == 16
            break
        time.sleep(0.2)
    else:
        raise Exception("The background merge did not start")

    start = time.time()
    assert node.stop_clickhouse(stop_wait_sec=90) is True
    elapsed = time.time() - start
    assert elapsed < 90, f"Shutdown took {elapsed} seconds"

    node.start_clickhouse()

    # The merge was aborted, the source parts and the data are intact.
    assert node.query("SELECT count() FROM t_shutdown_merge") == "160000\n"

    node.query("DROP TABLE t_shutdown_merge SYNC")


def test_shutdown_cancels_running_fetch(started_cluster):
    """A graceful shutdown must also cancel in-flight fetches of parts from
    other replicas: a fetch runs inside a single step of a background task, so
    the executors' `wait` would otherwise block for the whole download. The
    sender is throttled to 1 MiB/s and the part is ~150 MB of incompressible
    data, so without cancellation the fetch holds the shutdown for ~150
    seconds.
    """
    node1.query(
        """
        CREATE TABLE t_shutdown_fetch (k UInt64, v String)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/t_shutdown_fetch', 'r1')
        ORDER BY k
        SETTINGS max_replicated_sends_network_bandwidth = 1048576
        """
    )
    node1.query("INSERT INTO t_shutdown_fetch SELECT number, randomString(1000) FROM numbers(150000)")

    node2.query(
        """
        CREATE TABLE t_shutdown_fetch (k UInt64, v String)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/t_shutdown_fetch', 'r2')
        ORDER BY k
        """
    )

    for _ in range(300):
        fetches = node2.query("SELECT count() FROM system.replicated_fetches WHERE table = 't_shutdown_fetch'").strip()
        if fetches != "0":
            break
        time.sleep(0.2)
    else:
        raise Exception("The fetch did not start")

    start = time.time()
    assert node2.stop_clickhouse(stop_wait_sec=90) is True
    elapsed = time.time() - start
    assert elapsed < 90, f"Shutdown took {elapsed} seconds"

    # The fetch was aborted by the cancellation of the `ReplicatedFetchList`
    # entry (the message includes the part name), not by anything else.
    assert node2.contains_in_log("Fetching of part all_")

    node2.start_clickhouse()

    # After the restart the fetch is retried; dropping the table cancels it
    # again, through the per-storage blockers.
    node2.query("DROP TABLE t_shutdown_fetch SYNC")
    node1.query("DROP TABLE t_shutdown_fetch SYNC")
