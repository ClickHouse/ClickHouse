import concurrent.futures
import pytest
import uuid
import threading
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
)

HASHMAP_QUERY = """SELECT
    number % 100000000 AS category,
    number AS value
FROM numbers(100)
LIMIT 1 BY category
FORMAT Null
SETTINGS max_block_size=100, max_threads=1, max_rows_to_read=0"""

SORTED_QUERY = """SELECT
    number AS key1,
    number + 1 AS key2,
    number AS value
FROM numbers(100)
ORDER BY key1 ASC, key2 ASC
LIMIT 1 BY key1, key2
FORMAT Null
SETTINGS max_block_size=100, max_threads=1, max_rows_to_read=0"""


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


HASHMAP_FAULT_NAME = "limit_by_transform_pause"
SORTED_FAULT_NAME = "limit_by_sorted_stream_transform_pause"
HASHMAP_AFTER_LOOP_FAULT_NAME = "limit_by_transform_after_loop_pause"
SORTED_AFTER_LOOP_FAULT_NAME = "limit_by_sorted_stream_transform_after_loop_pause"
HASHMAP_MID_LOOP_FAULT_NAME = "limit_by_transform_mid_loop_pause"
SORTED_MID_LOOP_FAULT_NAME = "limit_by_sorted_stream_transform_mid_loop_pause"


def run_kill_query_failpoint_test(query, fault_name, query_id=None):
    if query_id is None:
        query_id = str(uuid.uuid4())

    node1.query(f"SYSTEM ENABLE FAILPOINT {fault_name}")

    thread_error = [None]

    def execute_query():
        try:
            _, error = node1.query_and_get_answer_with_error(
                query,
                query_id=query_id,
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
            f"SYSTEM WAIT FAILPOINT {fault_name} PAUSE",
        )
        done, _ = concurrent.futures.wait([wait_future], timeout=60)
        if not done:
            pool.shutdown(wait=False, cancel_futures=True)
            assert False, f"Failpoint {fault_name} not triggered within 60 s"
        pool.shutdown(wait=False)
        wait_future.result()

        node1.http_query(f"KILL QUERY WHERE query_id='{query_id}'")
    finally:
        node1.query(f"SYSTEM DISABLE FAILPOINT {fault_name}")

    query_thread.join()
    if thread_error[0] is not None:
        raise thread_error[0]

    result = node1.query(
        f"SELECT count(*) FROM system.processes WHERE query_id='{query_id}'"
    )
    assert int(result.strip()) == 0

    cancel_log = node1.grep_in_log(query_id)
    assert "QUERY_WAS_CANCELLED" in cancel_log
    assert "Cancelled during row processing" in cancel_log


def test_hashmap_kill_query(started_cluster):
    run_kill_query_failpoint_test(
        HASHMAP_QUERY,
        HASHMAP_FAULT_NAME,
    )


def test_sorted_kill_query(started_cluster):
    run_kill_query_failpoint_test(
        SORTED_QUERY,
        SORTED_FAULT_NAME,
    )


def run_kill_query_failpoint_after_loop_test(query, fault_name, expected_log_message, query_id=None):
    if query_id is None:
        query_id = str(uuid.uuid4())

    node1.query(f"SYSTEM ENABLE FAILPOINT {fault_name}")

    thread_error = [None]

    def execute_query():
        try:
            _, error = node1.query_and_get_answer_with_error(
                query,
                query_id=query_id,
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
            f"SYSTEM WAIT FAILPOINT {fault_name} PAUSE",
        )
        done, _ = concurrent.futures.wait([wait_future], timeout=60)
        if not done:
            pool.shutdown(wait=False, cancel_futures=True)
            assert False, f"Failpoint {fault_name} not triggered within 60 s"
        pool.shutdown(wait=False)
        wait_future.result()

        node1.http_query(f"KILL QUERY WHERE query_id='{query_id}'")
    finally:
        node1.query(f"SYSTEM DISABLE FAILPOINT {fault_name}")

    query_thread.join()
    if thread_error[0] is not None:
        raise thread_error[0]

    result = node1.query(
        f"SELECT count(*) FROM system.processes WHERE query_id='{query_id}'"
    )
    assert int(result.strip()) == 0

    cancel_log = node1.grep_in_log(query_id)
    assert "QUERY_WAS_CANCELLED" in cancel_log
    assert expected_log_message in cancel_log
    assert "Cancelled during row processing" not in cancel_log


def test_hashmap_kill_query_after_loop(started_cluster):
    run_kill_query_failpoint_after_loop_test(
        HASHMAP_QUERY,
        HASHMAP_AFTER_LOOP_FAULT_NAME,
        "Cancelled after processing chunk",
    )


def test_sorted_kill_query_after_loop(started_cluster):
    run_kill_query_failpoint_after_loop_test(
        SORTED_QUERY,
        SORTED_AFTER_LOOP_FAULT_NAME,
        "Cancelled after processing runs",
    )


def test_hashmap_kill_query_mid_loop(started_cluster):
    run_kill_query_failpoint_test(
        HASHMAP_QUERY,
        HASHMAP_MID_LOOP_FAULT_NAME,
    )


def test_sorted_kill_query_mid_loop(started_cluster):
    run_kill_query_failpoint_test(
        SORTED_QUERY,
        SORTED_MID_LOOP_FAULT_NAME,
    )
