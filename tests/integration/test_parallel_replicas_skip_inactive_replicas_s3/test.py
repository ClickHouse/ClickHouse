import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

nodes = [
    cluster.add_instance(
        f"node{i}",
        main_configs=["configs/config.xml"],
        macros={"shard": 1, "replica": i},
        with_zookeeper=True,
        with_minio=True,
        stay_alive=True,
    )
    for i in (1, 2, 3)
]
node1, node2, node3 = nodes


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_inactive_replica_excluded_from_total_query_nodes_on_s3(start_cluster):
    # Remote-disk companion to `test_parallel_replicas_skip_inactive_replicas`. On S3 (and other remote disks)
    # `calculateMinMarksPerTask` divides `sum_marks` by `threads * total_query_nodes`, and that term feeds one
    # half of the mark-segment-size heuristic (`chooseSegmentSize`). If `total_query_nodes` stays keyed off the
    # registered replica count (`getAllNodeCount()`) it diverges from the reading coordinator, which is sized by
    # the *active* replica count. A local-disk test cannot catch this because the remote-disk branch never runs.
    db = "pr_db_s3"
    for i, node in enumerate(nodes, start=1):
        node.query(
            f"CREATE DATABASE {db} ENGINE = Replicated('/test/databases/{db}', 'shard1', 'replica{i}')"
        )

    node1.query(
        f"CREATE TABLE {db}.tt (key Int64, value String) ENGINE = ReplicatedMergeTree ORDER BY key "
        f"SETTINGS storage_policy = 's3'"
    )
    node1.query(
        f"INSERT INTO {db}.tt SELECT number, toString(number) FROM numbers(100000)"
    )
    # Make sure the surviving replica holds a full local copy of the data.
    node2.query(f"SYSTEM SYNC REPLICA {db}.tt")

    # All three replicas are registered and active.
    assert_eq_with_retry(
        node1,
        f"SELECT count() FROM system.clusters WHERE cluster = '{db}' AND is_active = 1",
        "3\n",
    )

    # Stop one replica gracefully: it stays registered in the cluster definition but becomes inactive.
    node3.stop_clickhouse()
    try:
        assert_eq_with_retry(
            node1,
            f"SELECT count() FROM system.clusters WHERE cluster = '{db}' AND is_active = 1",
            "2\n",
            retry_count=60,
            sleep_time=1,
        )
        assert node1.query(f"SELECT count() FROM system.clusters WHERE cluster = '{db}'") == "3\n"

        # Read data with parallel replicas over the S3-backed table so the remote-disk task-size heuristic runs.
        result = node1.query(
            f"SELECT sum(key) FROM {db}.tt",
            settings={
                "enable_parallel_replicas": 1,
                "max_parallel_replicas": 3,
                "cluster_for_parallel_replicas": db,
                # Force the remote-reading task-size path that consumes `total_query_nodes`.
                "merge_tree_min_bytes_per_task_for_remote_reading": 0,
            },
        )
        assert result == "4999950000\n"

        # `total_query_nodes` must be the active-and-capped count (2), matching the coordinator, not the 3
        # registered replicas. This is the value that flows into `calculateMinMarksPerTask` on remote disks.
        assert node1.contains_in_log("total_query_nodes=2")
        assert not node1.contains_in_log("total_query_nodes=3")
    finally:
        node3.start_clickhouse()
