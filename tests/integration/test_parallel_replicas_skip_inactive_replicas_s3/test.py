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


def test_inactive_replica_changes_min_marks_per_task_on_s3(start_cluster):
    # Remote-disk companion to `test_parallel_replicas_skip_inactive_replicas`. On remote disks
    # `calculateMinMarksPerTask` computes
    #     heuristic_min_marks = min(sum_marks / (threads * total_query_nodes) / 2, min_bytes_per_task / avg_mark_bytes)
    # and uses it for `min_marks_per_task` when it exceeds the floor. That value feeds one half of the mark
    # segment size heuristic (`chooseSegmentSize`). If `total_query_nodes` stays keyed off the registered replica
    # count (`getAllNodeCount()`) it diverges from the reading coordinator, which is sized by the *active* count.
    #
    # To observe the divergence in the chosen `min_marks_per_task` (not just in a log line), the first term of the
    # `min()` must be the one that wins and must exceed the floor:
    #   * a large `merge_tree_min_bytes_per_task_for_remote_reading` keeps the byte term out of the `min()`,
    #   * `max_threads = 1` fixes `threads` so `min_marks_per_task * threads == min_marks_per_task`,
    #   * a small `index_granularity` (with adaptive granularity off) gives enough marks that
    #     `sum_marks / total_query_nodes / 2` stays well above the floor for both replica counts.
    # A local-disk test cannot reach this branch because the remote-disk path never runs there.
    db = "pr_db_s3"
    for i, node in enumerate(nodes, start=1):
        node.query(
            f"CREATE DATABASE {db} ENGINE = Replicated('/test/databases/{db}', 'shard1', 'replica{i}')"
        )

    node1.query(
        f"CREATE TABLE {db}.tt (key Int64, value String) ENGINE = ReplicatedMergeTree ORDER BY key "
        f"SETTINGS storage_policy = 's3', index_granularity = 128, index_granularity_bytes = 0"
    )
    node1.query(
        f"INSERT INTO {db}.tt SELECT number, toString(number) FROM numbers(100000)"
    )
    # One part with a predictable mark count.
    node1.query(f"OPTIMIZE TABLE {db}.tt FINAL")
    # Make sure the surviving replica holds a full local copy of the data.
    node2.query(f"SYSTEM SYNC REPLICA {db}.tt")

    # All three replicas are registered and active.
    assert_eq_with_retry(
        node1,
        f"SELECT count() FROM system.clusters WHERE cluster = '{db}' AND is_active = 1",
        "3\n",
    )

    # `min_marks_per_task` mirrors `calculateMinMarksPerTask`: floor is `merge_tree_min_read_task_size` (8) since
    # the remote concurrent-read row/byte settings default to 0; the heuristic term is `sum_marks / (threads *
    # total_query_nodes) / 2` with `threads == 1`. Read the real mark count so nothing is hard-coded.
    sum_marks = int(
        node1.query(
            f"SELECT sum(marks) FROM system.parts WHERE database = '{db}' AND table = 'tt' AND active"
        ).strip()
    )
    floor = 8

    def expected_min_marks(total_query_nodes):
        return max(floor, (sum_marks // total_query_nodes) // 2)

    min_marks_active = expected_min_marks(2)  # coordinator: 2 active replicas
    min_marks_registered = expected_min_marks(3)  # buggy: 3 registered replicas
    # The chosen setup must actually distinguish the two replica counts, otherwise the test proves nothing.
    assert min_marks_active != min_marks_registered, (
        f"setup does not distinguish replica counts: sum_marks={sum_marks}, "
        f"active={min_marks_active}, registered={min_marks_registered}"
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
        # `max_parallel_replicas = 3` is required: it must stay above the active count so the pre-fix registered
        # count (3) and the fixed active count (2) actually differ (a cap of 2 would collapse both to 2).
        result = node1.query(
            f"SELECT sum(key) FROM {db}.tt",
            settings={
                "enable_parallel_replicas": 1,
                "max_parallel_replicas": 3,
                "cluster_for_parallel_replicas": db,
                "max_threads": 1,
                # Keep the byte term out of the min(), so `total_query_nodes` drives `min_marks_per_task`.
                "merge_tree_min_bytes_per_task_for_remote_reading": 10 * 1024 * 1024 * 1024,
            },
        )
        assert result == "4999950000\n"

        # The remote-disk `min_marks_per_task` must be computed from the active-and-capped count (2), matching the
        # coordinator, not the 3 registered replicas. `.` matches the literal `*` in the trace line. This is the
        # observable outcome the previous `merge_tree_min_bytes_per_task_for_remote_reading = 0` test could not
        # reach: with a zero byte threshold the min() collapsed to 0 and `total_query_nodes` changed nothing.
        assert node1.contains_in_log(
            f"min_marks_per_task.threads={min_marks_active},"
        ), node1.grep_in_log("min_marks_per_task")
        assert not node1.contains_in_log(
            f"min_marks_per_task.threads={min_marks_registered},"
        )

        # And the denominator itself must be the active count, matching the coordinator on remote-disk reads.
        assert node1.contains_in_log("total_query_nodes=2")
        assert not node1.contains_in_log("total_query_nodes=3")
    finally:
        node3.start_clickhouse()
