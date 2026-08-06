import concurrent.futures
import pytest
import time
import uuid
import threading
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


HASHMAP_FAULT_NAME = "distinct_transform_pause"
LC_FAULT_NAME = "distinct_transform_lc_pause"


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
        ## An exception in SYSTEM WAIT FAILPOINT ... PAUSE would leave the query running
        ## unsynchronized (done is still non-empty); re-raise it so the test fails loudly.
        wait_future.result()
        pool.shutdown(wait=False)

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


def test_hashmap_kill_query(started_cluster):
    query = """SELECT DISTINCT number % 10000000
FROM numbers(10000)
FORMAT Null
SETTINGS max_block_size=10000, max_threads=1, max_rows_to_read=0"""

    run_kill_query_failpoint_test(
        query,
        HASHMAP_FAULT_NAME,
    )


def test_lc_kill_query(started_cluster):
    node1.query("CREATE TABLE IF NOT EXISTS lc_test (key LowCardinality(String)) ENGINE = Memory")
    node1.query("TRUNCATE TABLE IF EXISTS lc_test")
    node1.query("INSERT INTO lc_test SELECT toString(number % 1000) FROM numbers(10000)")

    query = """SELECT DISTINCT key FROM lc_test
FORMAT Null
SETTINGS max_block_size=10000, max_threads=1, max_rows_to_read=0"""

    run_kill_query_failpoint_test(
        query,
        LC_FAULT_NAME,
    )


def run_soft_timeout_failpoint_test(query, fault_name, query_id=None, hold_seconds=30):
    """A soft `max_execution_time` with `timeout_overflow_mode = 'break'` must stop the query
    cleanly (no exception) when the deadline passes while the DISTINCT transform is paused at a
    failpoint inside a single `transform` call. The transform is held past the deadline, then the
    failpoint is released; the soft-timeout latch stops the inner loop and the query finishes.
    """
    if query_id is None:
        query_id = str(uuid.uuid4())

    node1.query(f"SYSTEM ENABLE FAILPOINT {fault_name}")

    thread_error = [None]
    query_error = [None]

    def execute_query():
        try:
            _, error = node1.query_and_get_answer_with_error(
                query,
                query_id=query_id,
            )
            query_error[0] = error
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
        ## An exception in SYSTEM WAIT FAILPOINT ... PAUSE would leave the query running
        ## unsynchronized (done is still non-empty); re-raise it so the test fails loudly.
        wait_future.result()
        pool.shutdown(wait=False)

        ## Hold the transform paused past the max_execution_time deadline, then release it.
        time.sleep(hold_seconds)
        node1.query(f"SYSTEM DISABLE FAILPOINT {fault_name}")
    finally:
        node1.query(f"SYSTEM DISABLE FAILPOINT {fault_name}")

    query_thread.join(timeout=60)
    assert not query_thread.is_alive(), "query did not terminate after the soft timeout"
    if thread_error[0] is not None:
        raise thread_error[0]

    ## In break mode the soft timeout must not surface any error to the client.
    assert query_error[0] == "", f"break-mode soft timeout raised an error: {query_error[0]}"

    result = node1.query(
        f"SELECT count(*) FROM system.processes WHERE query_id='{query_id}'"
    )
    assert int(result.strip()) == 0


def test_hashmap_soft_timeout(started_cluster):
    query = """SELECT DISTINCT number % 10000000
FROM numbers(10000)
FORMAT Null
SETTINGS max_block_size=10000, max_threads=1, max_execution_time=5,
    timeout_overflow_mode='break', max_rows_to_read=0"""

    run_soft_timeout_failpoint_test(
        query,
        HASHMAP_FAULT_NAME,
    )


def test_lc_soft_timeout(started_cluster):
    node1.query("CREATE TABLE IF NOT EXISTS lc_test (key LowCardinality(String)) ENGINE = Memory")
    node1.query("TRUNCATE TABLE IF EXISTS lc_test")
    node1.query("INSERT INTO lc_test SELECT toString(number % 1000) FROM numbers(10000)")

    query = """SELECT DISTINCT key FROM lc_test
FORMAT Null
SETTINGS max_block_size=10000, max_threads=1, max_execution_time=5,
    timeout_overflow_mode='break', max_rows_to_read=0"""

    run_soft_timeout_failpoint_test(
        query,
        LC_FAULT_NAME,
    )
