import concurrent.futures
import threading
import uuid

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
)

FAULT_NAME = "aggregating_in_order_transform_mid_loop_pause"

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


def test_kill_query_mid_loop(started_cluster):
    query_id = str(uuid.uuid4())

    node1.query(f"SYSTEM ENABLE FAILPOINT {FAULT_NAME}")

    thread_error = [None]

    def execute_query():
        try:
            _, error = node1.query_and_get_answer_with_error(QUERY, query_id=query_id)
            assert "DB::Exception: Query was cancelled" in error
        except Exception as e:
            thread_error[0] = e

    query_thread = threading.Thread(target=execute_query)
    query_thread.start()

    try:
        pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        wait_future = pool.submit(
            node1.query,
            f"SYSTEM WAIT FAILPOINT {FAULT_NAME} PAUSE",
        )
        done, _ = concurrent.futures.wait([wait_future], timeout=60)
        if not done:
            pool.shutdown(wait=False, cancel_futures=True)
            assert False, f"Failpoint {FAULT_NAME} not triggered within 60 s"
        pool.shutdown(wait=False)
        wait_future.result()

        node1.http_query(f"KILL QUERY WHERE query_id='{query_id}'")
    finally:
        node1.query(f"SYSTEM DISABLE FAILPOINT {FAULT_NAME}")

    query_thread.join()
    if thread_error[0] is not None:
        raise thread_error[0]

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
