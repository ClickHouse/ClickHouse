import pytest
import uuid
import threading
import time
import os

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
)


SELECT_SQL = """SELECT sipHash64(sipHash64(sipHash64(sipHash64(sipHash64(sipHash64(sipHash64(sipHash64(sipHash64(sipHash64(sipHash64(sipHash64(sipHash64(sipHash64(sipHash64(sipHash64(sipHash64(sipHash64(sipHash64(sipHash64(number))))))))))))))))))))
FROM numbers(350000000)
FORMAT `Null`
SETTINGS max_block_size = 350000000, max_threads = 1"""


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_kill_query(started_cluster):
    query_id = str(uuid.uuid4())

    def execute_query():
        _, error = node1.query_and_get_answer_with_error(
            SELECT_SQL,
            query_id=query_id,
        )
        assert "DB::Exception: Query was cancelled" in error

    query_thread = threading.Thread(target=execute_query)
    query_thread.start()

    node1.wait_for_log_line("Make expression transform")
    time.sleep(1)

    node1.query(f"KILL QUERY WHERE query_id='{query_id}' SYNC")

    query_thread.join()

    # Verify that query was successfully cancelled in ClickHouse server
    result = node1.query(
        "SELECT count(*) FROM system.processes WHERE query_id='{query_id}'"
    )
    assert int(result.strip()) == 0

    assert node1.contains_in_log("QUERY_WAS_CANCELLED")


def test_cancel_query(started_cluster):
    def execute_query():
        node1.exec_in_container(
            [
                "bash",
                "-c",
                f"""clickhouse client --query '{SELECT_SQL}' """,
            ]
        )

    query_thread = threading.Thread(target=execute_query)
    query_thread.start()
    node1.wait_for_log_line("Make expression transform", look_behind_lines=0)
    time.sleep(1)

    node1.stop_clickhouse_client()
    node1.wait_for_log_line("Received 'Cancel' packet from the client")
    time.sleep(1)

    query_thread.join()
    assert node1.contains_in_log("QUERY_WAS_CANCELLED_BY_CLIENT")
