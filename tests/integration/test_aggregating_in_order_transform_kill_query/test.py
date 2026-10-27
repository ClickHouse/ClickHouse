import uuid

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
)

FAULT_NAME = "aggregating_in_order_transform_cancel_mid_loop"

# A `MergeTree` table sorted on the GROUP BY key is required: `buildInputOrderInfo` only
# accepts `ReadFromMergeTree` / `ReadFromMerge` / `ReadFromObjectStorageStep`, so a `numbers`
# source silently builds a plain `AggregatingTransform` and never reaches the failpoint.
QUERY = """SELECT k, count()
FROM t_agg_in_order_cancel
GROUP BY k
FORMAT Null
SETTINGS optimize_aggregation_in_order = 1, max_threads = 1, max_block_size = 100,
         enable_parallel_replicas = 0"""


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        node1.query(
            "CREATE TABLE t_agg_in_order_cancel (k UInt64) ENGINE = MergeTree ORDER BY k"
        )
        node1.query("INSERT INTO t_agg_in_order_cancel SELECT number FROM numbers(100)")
        yield cluster
    finally:
        cluster.shutdown()


def failpoint_enabled():
    return node1.query(
        f"SELECT enabled FROM system.fail_points WHERE name = '{FAULT_NAME}'"
    ).strip()


def test_kill_query_mid_loop(started_cluster):
    query_id = str(uuid.uuid4())

    # The failpoint cancels the query in place, the same way `KILL QUERY` does, so the query runs
    # on this thread and no `SYSTEM WAIT FAILPOINT ... PAUSE` parks a pipeline worker inside
    # `IProcessor::work()`.
    node1.query(f"SYSTEM ENABLE FAILPOINT {FAULT_NAME}")
    try:
        assert failpoint_enabled() == "1"

        _, error = node1.query_and_get_answer_with_error(QUERY, query_id=query_id)
        assert "DB::Exception: Query was cancelled" in error

        # `enabled` went 1 -> 0 with no DISABLE in between, which only a fire can do; `0` on its
        # own is also what an un-armed failpoint reads.
        assert failpoint_enabled() == "0"
    finally:
        node1.query(f"SYSTEM DISABLE FAILPOINT {FAULT_NAME}")

    result = node1.query(
        f"SELECT count(*) FROM system.processes WHERE query_id='{query_id}'"
    )
    assert int(result.strip()) == 0

    cancel_log = node1.grep_in_log(query_id)
    assert "QUERY_WAS_CANCELLED" in cancel_log
    # The marker is inside the cancellation branch, which is re-entered on every
    # iteration while the flag stays set, so exactly one line proves the transform
    # returned instead of finishing the remaining intervals.
    marker_lines = cancel_log.count("Cancelled between key intervals")
    assert marker_lines == 1, f"expected 1 marker line, got {marker_lines}"
