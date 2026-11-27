import pymysql
import pytest

from helpers.cluster import ClickHouseCluster

MYSQL_PORT = 9001

FAULT_NAME = "mysql_output_format_cancel_mid_loop"

ROW_COUNT = 10

# The row index `MySQLOutputFormat::consume` cancels the query on when the failpoint is enabled.
CANCEL_AT_ROW = 5

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


def failpoint_enabled():
    return node.query(
        f"SELECT enabled FROM system.fail_points WHERE name = '{FAULT_NAME}'",
        user="default",
        password="123",
    ).strip()


def test_kill_query_during_output(started_cluster):
    """A query cancelled while its rows are being written to the MySQL wire must stop writing
    them and fail with `QUERY_WAS_CANCELLED`. Reporting the cancellation is not enough: the
    rest of the result set must not reach the client first."""

    client_error = [None]
    rows_seen = [0]

    # The failpoint cancels the query in place, the same way `KILL QUERY` does, so the query runs
    # on this thread and no `SYSTEM WAIT FAILPOINT ... PAUSE` parks a pipeline worker inside
    # `IProcessor::work()`.
    node.query(f"SYSTEM ENABLE FAILPOINT {FAULT_NAME}", user="default", password="123")
    try:
        assert failpoint_enabled() == "1"

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
            raise AssertionError("the cancelled query returned a complete result set")
        # Only a driver error is an expected outcome, so the assertion above stays a
        # failure instead of being recorded as the error the test looks for.
        except pymysql.err.Error as e:
            client_error[0] = repr(e)
        finally:
            conn.close()

        # `enabled` went 1 -> 0 with no DISABLE in between, which only a fire can do; `0` on its
        # own is also what an un-armed failpoint reads.
        assert failpoint_enabled() == "0"
    finally:
        node.query(
            f"SYSTEM DISABLE FAILPOINT {FAULT_NAME}", user="default", password="123"
        )

    assert client_error[0] is not None, "the client did not observe an error"
    assert "Query was cancelled" in client_error[0], client_error[0]

    # Rows up to and including the one the cancel fired on are already on their way and the next
    # per-row check ends the loop, so the count is exact. Without that check the whole chunk is
    # written and the client receives all ROW_COUNT rows before the same cancellation error.
    assert rows_seen[0] == CANCEL_AT_ROW + 1, rows_seen[0]

    # The MySQL protocol gives the client no way to supply a query id, and the query is gone from
    # `system.processes` by now, so recover the server-assigned one from the log.
    node.query("SYSTEM FLUSH LOGS query_log", user="default", password="123")
    query_id = node.query(
        """SELECT query_id FROM system.query_log
           WHERE query LIKE 'SELECT toString(number)%' AND type = 'ExceptionWhileProcessing'
           ORDER BY event_time_microseconds DESC LIMIT 1""",
        user="default",
        password="123",
    ).strip()
    assert query_id, "the cancelled query is not in system.query_log"

    result = node.query(
        f"SELECT count(*) FROM system.processes WHERE query_id='{query_id}'",
        user="default",
        password="123",
    )
    assert int(result.strip()) == 0

    cancel_log = node.grep_in_log(query_id)
    assert "QUERY_WAS_CANCELLED" in cancel_log
    # A second line would mean the failpoint fired in a later chunk, so the first one held
    # at most CANCEL_AT_ROW rows and the bound above would be measuring the chunk size rather
    # than the cancellation.
    chunks = cancel_log.count("Consume a chunk")
    assert chunks == 1, f"expected a single chunk, got {chunks}"
