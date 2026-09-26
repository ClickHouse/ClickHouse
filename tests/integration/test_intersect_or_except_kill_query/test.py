import concurrent.futures
import pytest
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


SET_PAUSE = "intersect_or_except_transform_pause"
COUNTS_PAUSE = "intersect_or_except_transform_counts_pause"


def run_kill_query_failpoint_test(query, fault_name, query_id=None):
    """A `KILL QUERY` sent while a single right-side chunk is being hashed must stop the
    IntersectOrExceptTransform at the paused row, not after it has hashed the rest of the chunk.

    The transform pauses at the first 4096-row boundary inside the build loop (the `PAUSEABLE_ONCE`
    failpoint auto-disables after that one pause). After the kill the failpoint is re-armed and
    the loop is resumed. Intra-loop polling (the PR) makes the loop return at the paused row, so
    the re-armed failpoint at the next 4096-row boundary is never hit and a second
    `SYSTEM WAIT FAILPOINT ... PAUSE` times out. If the mid-loop cancellation check were removed,
    the loop would keep hashing rows, hit the re-armed failpoint at the next boundary, and the
    second WAIT would return -- so this test would fail then instead of false-passing.
    """
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
        pool = concurrent.futures.ThreadPoolExecutor(max_workers=2)

        def wait_failpoint(timeout=60):
            wait_future = pool.submit(
                node1.query,
                f"SYSTEM WAIT FAILPOINT {fault_name} PAUSE",
            )
            done, _ = concurrent.futures.wait([wait_future], timeout=timeout)
            if not done:
                assert False, f"Failpoint {fault_name} not triggered within {timeout} s"
            ## An exception in SYSTEM WAIT FAILPOINT ... PAUSE would leave the query running
            ## unsynchronized (done is still non-empty); re-raise it so the test fails loudly.
            wait_future.result()

        try:
            wait_failpoint()

            node1.http_query(f"KILL QUERY WHERE query_id='{query_id}'")

            ## Re-arm the one-shot failpoint, then resume the loop. With the fix the loop
            ## returns at the paused row; without it the loop hits the next boundary and pauses
            ## again, which the second WAIT below observes.
            node1.query(f"SYSTEM ENABLE FAILPOINT {fault_name}")
            node1.query(f"SYSTEM NOTIFY FAILPOINT {fault_name}")

            second_pause_future = pool.submit(
                node1.query,
                f"SYSTEM WAIT FAILPOINT {fault_name} PAUSE",
            )
            done, _ = concurrent.futures.wait([second_pause_future], timeout=10)
            if done:
                second_pause_future.result()
                assert False, "loop reached the re-armed failpoint: intra-loop cancellation is broken"
        finally:
            pool.shutdown(wait=False, cancel_futures=True)
    finally:
        node1.query(f"SYSTEM DISABLE FAILPOINT {fault_name}")

    ## Bound the join so a regression that keeps the query running fails cleanly here instead of
    ## hanging the integration shard.
    query_thread.join(timeout=60)
    assert not query_thread.is_alive(), "killed query did not terminate within 60 s"
    if thread_error[0] is not None:
        raise thread_error[0]

    result = node1.query(
        f"SELECT count(*) FROM system.processes WHERE query_id='{query_id}'"
    )
    assert int(result.strip()) == 0

    cancel_log = node1.grep_in_log(query_id)
    assert "QUERY_WAS_CANCELLED" in cancel_log


def test_intersect_distinct_kill_query(started_cluster):
    query = """SELECT number FROM numbers(10000000)
INTERSECT DISTINCT
SELECT number FROM numbers(10000000)
FORMAT Null
SETTINGS max_block_size=10000000, max_threads=1, max_rows_to_read=0"""

    run_kill_query_failpoint_test(
        query,
        SET_PAUSE,
    )


def test_intersect_all_kill_query(started_cluster):
    query = """SELECT number FROM numbers(10000000)
INTERSECT ALL
SELECT number FROM numbers(10000000)
FORMAT Null
SETTINGS max_block_size=10000000, max_threads=1, max_rows_to_read=0"""

    run_kill_query_failpoint_test(
        query,
        COUNTS_PAUSE,
    )


def test_except_distinct_kill_query(started_cluster):
    query = """SELECT number FROM numbers(10000000)
EXCEPT DISTINCT
SELECT number FROM numbers(10000000)
FORMAT Null
SETTINGS max_block_size=10000000, max_threads=1, max_rows_to_read=0"""

    run_kill_query_failpoint_test(
        query,
        SET_PAUSE,
    )


def test_except_all_kill_query(started_cluster):
    query = """SELECT number FROM numbers(10000000)
EXCEPT ALL
SELECT number FROM numbers(10000000)
FORMAT Null
SETTINGS max_block_size=10000000, max_threads=1, max_rows_to_read=0"""

    run_kill_query_failpoint_test(
        query,
        COUNTS_PAUSE,
    )
