import concurrent.futures
import pytest
import uuid
import threading
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
)

TOTALS_QUERY = """SELECT
    intDiv(number, 10) AS k,
    count() AS cnt,
    sum(number) AS total,
    uniq(number) AS uniq_val
FROM numbers(1000)
GROUP BY k WITH TOTALS
FORMAT Null
SETTINGS max_block_size=100, max_threads=1, max_rows_to_read=0"""


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


FAULT_NAME = "totals_having_transform_pause"


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


def test_totals_having_kill_query(started_cluster):
    run_kill_query_failpoint_test(
        TOTALS_QUERY,
        FAULT_NAME,
    )
