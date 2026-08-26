import concurrent.futures
import threading

import psycopg2
import pytest

from helpers.cluster import ClickHouseCluster

server_port = 5433

FAULT_NAME = "postgresql_output_format_mid_loop_pause"

ROW_COUNT = 1000

# `max_block_size` equal to the row count clamps `numbers` to a single stream, so the whole
# result reaches the output format as one chunk. That is what the test needs: with one chunk
# the per-row cancellation check is the only way to leave the row loop early, and the check
# made once at chunk entry cannot satisfy the test instead.
SELECT_FROM_NUMBERS = f"""SELECT toString(number), repeat('x', 100) FROM numbers({ROW_COUNT})
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


def test_kill_query_during_output(started_cluster):
    """A query killed while its rows are being written to the PostgreSQL wire must stop
    writing them and fail with `QUERY_WAS_CANCELLED`. Reporting the cancellation is not
    enough: the rest of the result set must not reach the client first."""

    thread_error = [None]
    client_error = [None]

    def execute_query():
        try:
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
                raise AssertionError("the killed query returned a complete result set")
            # Only a driver error is an expected outcome, so the assertion above stays a
            # failure instead of being recorded as the error the test looks for.
            except psycopg2.Error as e:
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
        # unambiguous. The PostgreSQL protocol gives the client no way to supply one.
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

    # `PostgreSQLHandler::processQuery` lets an exception raised after the first `DataRow`
    # escape without an `ErrorResponse`, so the client sees a closed connection rather than
    # the server's message, and `psycopg2` buffers a result set instead of exposing the rows
    # that arrived. What the driver reports is therefore not asserted; the bytes the query put
    # on the wire are, below.
    assert client_error[0] is not None, "the client did not observe an error"

    result = node.query(
        f"SELECT count(*) FROM system.processes WHERE query_id='{query_id}'",
        user="default",
        password="123",
    )
    assert int(result.strip()) == 0

    node.query("SYSTEM FLUSH LOGS", user="default", password="123")
    # The rows are buffered and flushed after the loop, so a loop ended by the per-row check
    # sends nothing. Without that check the whole chunk is written and flushed, which is over
    # a hundred kilobytes for ROW_COUNT rows.
    sent_bytes = node.query(
        "SELECT ProfileEvents['NetworkSendBytes'] FROM system.query_log "
        f"WHERE query_id='{query_id}' AND type = 'ExceptionWhileProcessing'",
        user="default",
        password="123",
    )
    assert int(sent_bytes.strip()) == 0, sent_bytes

    cancel_log = node.grep_in_log(query_id)
    assert "QUERY_WAS_CANCELLED" in cancel_log
    # A second line would mean the failpoint parked in a later chunk, so the first one held
    # at most five rows and the byte count above would be measuring the chunk size rather
    # than the cancellation.
    chunks = cancel_log.count("Consume a chunk")
    assert chunks == 1, f"expected a single chunk, got {chunks}"
