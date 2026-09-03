import concurrent.futures
import threading

import pymysql
import pytest

from helpers.cluster import ClickHouseCluster

MYSQL_PORT = 9001

FAULT_NAME = "mysql_output_format_mid_loop_pause"

ROW_COUNT = 10

# The row index `MySQLOutputFormat::consume` parks on when the failpoint is enabled.
PAUSE_AT_ROW = 5

# `max_block_size` equal to the row count clamps `numbers` to one stream, so the whole result
# arrives as one chunk and only the per-row cancellation check can leave the row loop early.
SELECT_FROM_NUMBERS = f"""SELECT toString(number), repeat('x', 160000) FROM numbers({ROW_COUNT})
SETTINGS max_block_size = {ROW_COUNT}"""

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/mysql.xml"],
    user_configs=["configs/users.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        node.wait_for_log_line("MySQL compatibility protocol")
        yield cluster
    finally:
        cluster.shutdown()


def test_kill_query_during_output(started_cluster):
    """A query killed while its rows are being written to the MySQL wire must stop writing
    them and fail with `QUERY_WAS_CANCELLED`. Reporting the cancellation is not enough: the
    rest of the result set must not reach the client first."""

    thread_error = [None]
    client_error = [None]
    rows_seen = [0]

    def execute_query():
        try:
            conn = pymysql.connections.Connection(
                host=started_cluster.get_instance_ip("node"),
                port=MYSQL_PORT,
                user="default",
                password="123",
                database="default",
            )
            try:
                # An unbuffered cursor, so the rows that reached the client can be counted
                # instead of being discarded when the error arrives.
                with conn.cursor(pymysql.cursors.SSCursor) as cur:
                    cur.execute(SELECT_FROM_NUMBERS)
                    for _ in cur:
                        rows_seen[0] += 1
                raise AssertionError("the killed query returned a complete result set")
            # Only a driver error is an expected outcome, so the assertion above stays a
            # failure instead of being recorded as the error the test looks for.
            except pymysql.err.Error as e:
                client_error[0] = repr(e)
            finally:
                conn.close()
        except Exception as e:
            thread_error[0] = e

    node.query(f"SYSTEM ENABLE FAILPOINT {FAULT_NAME}", user="default", password="123")

    query_thread = threading.Thread(target=execute_query)
    query_thread.start()

    try:
        # `SYSTEM WAIT FAILPOINT ... PAUSE` blocks, so it needs its own thread to keep a
        # failpoint that never fires a failure rather than a hang.
        pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        wait_future = pool.submit(
            node.query,
            f"SYSTEM WAIT FAILPOINT {FAULT_NAME} PAUSE",
            user="default",
            password="123",
        )
        done, _ = concurrent.futures.wait([wait_future], timeout=60)
        if not done:
            pool.shutdown(wait=False, cancel_futures=True)
            assert False, f"Failpoint {FAULT_NAME} not triggered within 60 s"
        pool.shutdown(wait=False)
        wait_future.result()

        # The output loop is parked, so the query is registered and its server-assigned id is
        # unambiguous. The MySQL protocol gives the client no way to supply one.
        query_id = node.query(
            "SELECT query_id FROM system.processes WHERE query LIKE 'SELECT toString(number)%'",
            user="default",
            password="123",
        ).strip()
        assert query_id, "the paused query is not in system.processes"

        # `SYNC` would wait for a query that cannot finish until the failpoint is released.
        node.query(
            f"KILL QUERY WHERE query_id='{query_id}'", user="default", password="123"
        )
    finally:
        node.query(
            f"SYSTEM DISABLE FAILPOINT {FAULT_NAME}", user="default", password="123"
        )

    query_thread.join()
    if thread_error[0] is not None:
        raise thread_error[0]

    assert client_error[0] is not None, "the client did not observe an error"
    assert "Query was cancelled" in client_error[0], client_error[0]

    # Rows up to and including the parked one are already on their way and the next per-row
    # check ends the loop, so the count is exact. Without that check the whole chunk is
    # written and the client receives all ROW_COUNT rows before the same cancellation error.
    assert rows_seen[0] == PAUSE_AT_ROW + 1, rows_seen[0]

    result = node.query(
        f"SELECT count(*) FROM system.processes WHERE query_id='{query_id}'",
        user="default",
        password="123",
    )
    assert int(result.strip()) == 0

    cancel_log = node.grep_in_log(query_id)
    assert "QUERY_WAS_CANCELLED" in cancel_log
    # A second line would mean the failpoint parked in a later chunk, so the first one held
    # at most PAUSE_AT_ROW rows and the bound above would be measuring the chunk size rather
    # than the cancellation.
    chunks = cancel_log.count("Consume a chunk")
    assert chunks == 1, f"expected a single chunk, got {chunks}"
