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

    node1.query(f"SYSTEM WAIT FAILPOINT {fault_name} PAUSE")

    node1.http_query(f"KILL QUERY WHERE query_id='{query_id}'")

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
