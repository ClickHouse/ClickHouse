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
FROM numbers(100000000)
LIMIT 1 BY category
FORMAT Null
SETTINGS max_block_size=100000000, max_threads=1, max_rows_to_read=0"""

SORTED_QUERY = """SELECT
    number AS key1,
    number + 1 AS key2,
    number AS value
FROM numbers(50000000)
ORDER BY key1 ASC, key2 ASC
LIMIT 1 BY key1, key2
FORMAT Null
SETTINGS max_block_size=50000000, max_threads=1, max_rows_to_read=0"""


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def run_kill_query_test(query, log_line_pattern, log_timeout=30):
    query_id = str(uuid.uuid4())
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

    node1.wait_for_log_line(log_line_pattern, timeout=log_timeout)

    node1.http_query(f"KILL QUERY WHERE query_id='{query_id}' SYNC")

    query_thread.join()
    if thread_error[0] is not None:
        raise thread_error[0]

    # Verify that query was successfully cancelled in ClickHouse server
    result = node1.query(
        f"SELECT count(*) FROM system.processes WHERE query_id='{query_id}'"
    )
    assert int(result.strip()) == 0

    cancel_log = node1.grep_in_log(query_id)
    assert "QUERY_WAS_CANCELLED" in cancel_log
    assert "Cancelled during row processing" in cancel_log


def test_hashmap_kill_query(started_cluster):
    run_kill_query_test(
        HASHMAP_QUERY,
        "Transform a chunk in LimitByTransform",
    )


def test_sorted_kill_query(started_cluster):
    run_kill_query_test(
        SORTED_QUERY,
        "Transform a chunk in LimitBySortedStreamTransform",
        log_timeout=180,
    )
