import concurrent.futures
import threading
import uuid

import pytest

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance("node1")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def run_kill_query_failpoint_test(query, failpoint_name, query_id=None):
    if query_id is None:
        query_id = str(uuid.uuid4())

    node1.query(f"SYSTEM ENABLE FAILPOINT {failpoint_name}")

    thread_error = [None]

    def execute_query():
        try:
            _, error = node1.query_and_get_answer_with_error(
                query, query_id=query_id,
            )
            assert "DB::Exception: Query was cancelled" in error
        except Exception as e:
            thread_error[0] = e

    query_thread = threading.Thread(target=execute_query)
    query_thread.start()

    try:
        pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        wait_future = pool.submit(
            node1.query,
            f"SYSTEM WAIT FAILPOINT {failpoint_name} PAUSE",
        )
        done, _ = concurrent.futures.wait([wait_future], timeout=60)
        if not done:
            pool.shutdown(wait=False, cancel_futures=True)
            assert False, f"Failpoint {failpoint_name} not triggered within 60s"
        pool.shutdown(wait=False)

        node1.http_query(f"KILL QUERY WHERE query_id='{query_id}'")
    finally:
        node1.query(f"SYSTEM DISABLE FAILPOINT {failpoint_name}")

    query_thread.join()
    if thread_error[0] is not None:
        raise thread_error[0]

    result = node1.query(
        f"SELECT count(*) FROM system.processes WHERE query_id='{query_id}'"
    )
    assert int(result.strip()) == 0

    cancel_log = node1.grep_in_log(query_id)
    assert "QUERY_WAS_CANCELLED" in cancel_log


def test_sorted_stream_kill_query(started_cluster):
    query = (
        "SELECT DISTINCT number "
        "FROM numbers(10000) "
        "ORDER BY number "
        "FORMAT Null "
        "SETTINGS max_block_size=10000, max_threads=1, max_rows_to_read=0"
    )
    run_kill_query_failpoint_test(query, "distinct_sorted_stream_transform_pause")


def test_sorted_stream_kill_query_long_runs(started_cluster):
    node1.query("DROP TABLE IF EXISTS test_runs")
    node1.query("CREATE TABLE test_runs (k UInt32) ENGINE = MergeTree() ORDER BY k")
    node1.query("INSERT INTO test_runs SELECT intDiv(number, 5000) FROM numbers(10000)")
    try:
        query = (
            "SELECT DISTINCT k "
            "FROM test_runs "
            "ORDER BY k "
            "FORMAT Null "
            "SETTINGS max_block_size=10000, max_threads=1, max_rows_to_read=0"
        )
        run_kill_query_failpoint_test(query, "distinct_sorted_stream_transform_pause")
    finally:
        node1.query("DROP TABLE IF EXISTS test_runs")


def test_sorted_stream_kill_query_other_columns(started_cluster):
    node1.query("DROP TABLE IF EXISTS test_other_columns")
    node1.query("CREATE TABLE test_other_columns (k UInt32, v UInt32) ENGINE = MergeTree() ORDER BY k")
    node1.query("INSERT INTO test_other_columns SELECT intDiv(number, 5000), number FROM numbers(10000)")
    try:
        query = (
            "SELECT DISTINCT k, v "
            "FROM test_other_columns "
            "ORDER BY k "
            "FORMAT Null "
            "SETTINGS max_block_size=10000, max_threads=1, max_rows_to_read=0"
        )
        run_kill_query_failpoint_test(query, "distinct_sorted_stream_transform_pause")
    finally:
        node1.query("DROP TABLE IF EXISTS test_other_columns")
