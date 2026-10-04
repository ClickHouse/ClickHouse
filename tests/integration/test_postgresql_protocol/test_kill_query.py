import psycopg2
import pytest

from helpers.cluster import ClickHouseCluster

server_port = 5433

FAULT_NAME = "postgresql_output_format_cancel_mid_loop"

ROW_COUNT = 10

# The row index `PostgreSQLOutputFormat::consume` cancels the query on when the failpoint is enabled.
CANCEL_AT_ROW = 5

# `max_block_size` equal to the row count clamps `numbers` to one stream, so the whole result
# arrives as one chunk and only the per-row cancellation check can leave the row loop early.
SELECT_FROM_NUMBERS = f"""SELECT toString(number), repeat('x', 160000) FROM numbers({ROW_COUNT})
SETTINGS max_block_size = {ROW_COUNT}"""

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/postgresql.xml"],
    user_configs=["configs/default_passwd.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        node.wait_for_log_line("PostgreSQL compatibility protocol")
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
    """A query cancelled while its rows are being written to the PostgreSQL wire must stop
    writing them and fail with `QUERY_WAS_CANCELLED`. Reporting the cancellation is not
    enough: the rest of the result set must not reach the client first."""

    client_error = [None]

    # The failpoint cancels the query in place, the same way `KILL QUERY` does, so the query runs
    # on this thread and no `SYSTEM WAIT FAILPOINT ... PAUSE` parks a pipeline worker inside
    # `IProcessor::work()`.
    node.query(f"SYSTEM ENABLE FAILPOINT {FAULT_NAME}", user="default", password="123")
    try:
        assert failpoint_enabled() == "1"

        conn = psycopg2.connect(
            host=started_cluster.get_instance_ip("node"),
            port=server_port,
            user="default",
            password="123",
            dbname="default",
        )
        try:
            with conn.cursor() as cur:
                cur.execute(SELECT_FROM_NUMBERS)
                for _ in cur:
                    pass
            raise AssertionError("the cancelled query returned a complete result set")
        # Only a driver error is an expected outcome, so the assertion above stays a
        # failure instead of being recorded as the error the test looks for.
        except psycopg2.Error as e:
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

    # The handler sends an `ErrorResponse` and then tears the connection down without a
    # following `ReadyForQuery`, so `psycopg2` reports the connection loss rather than the
    # server's message. The message is therefore not asserted; the bytes below are.
    assert client_error[0] is not None, "the client did not observe an error"

    node.query("SYSTEM FLUSH LOGS", user="default", password="123")
    # The PostgreSQL protocol gives the client no way to supply a query id, and the query is gone
    # from `system.processes` by now, so recover the server-assigned one from the log.
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

    # Counts only bytes flushed while the query context was attached; the rows the loop
    # buffered reach the wire later, once the `QueryScope` has unwound. One row beyond the
    # cancelled one overflows the 1 MiB socket buffer, so 0 bounds the loop at CANCEL_AT_ROW + 1.
    sent_bytes = node.query(
        "SELECT ProfileEvents['NetworkSendBytes'] FROM system.query_log "
        f"WHERE query_id='{query_id}' AND type = 'ExceptionWhileProcessing'",
        user="default",
        password="123",
    )
    assert int(sent_bytes.strip()) == 0, sent_bytes

    cancel_log = node.grep_in_log(query_id)
    assert "QUERY_WAS_CANCELLED" in cancel_log
    # A second line would mean the failpoint fired in a later chunk, so the first one held
    # at most CANCEL_AT_ROW rows and the byte count above would be measuring the chunk size
    # rather than the cancellation.
    chunks = cancel_log.count("Consume a chunk")
    assert chunks == 1, f"expected a single chunk, got {chunks}"
