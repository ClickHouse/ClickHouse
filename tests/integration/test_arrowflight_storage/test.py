import pytest
import time
import uuid

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import arrowflight_user, arrowflight_pass
from helpers.test_tools import TSV, assert_eq_with_retry

# The STALL_* datasets of the test Flight server block for 120s. Every query against one must
# finish well inside that, and far inside the harness's own 600s client timeout, so a regression
# shows up as a failed assertion instead of a killed run. The request timeout must also stay below
# SLOW_SCHEMA_ANSWER_SECONDS in ci/docker/integration/arrowflight/flight_server.py, because that
# gap is what makes a deadline fired later than configured end its query in a success.
STALL_QUERY_BOUND_SEC = 60
STALL_REQUEST_TIMEOUT_SEC = 3

# SLOW_DOGET_THEN_STALL withholds its stream this long, and then never sends a first message. Kept
# in step with SLOW_DOGET_SECONDS in ci/docker/integration/arrowflight/flight_server.py.
DOGET_DELAY_SEC = 15
# Cancellation lands inside DoGet: after the read is issued, well before the stub answers.
DOGET_CANCEL_AT_SEC = 5
# Far enough above DOGET_DELAY_SEC that a query ending on its deadline is separable from one
# ending when DoGet returns.
DOGET_REQUEST_TIMEOUT_SEC = 45
# Midway between the two outcomes: DOGET_DELAY_SEC when the published reader is aborted, and
# DOGET_REQUEST_TIMEOUT_SEC when it is left running.
DOGET_STOP_BOUND_SEC = (DOGET_DELAY_SEC + DOGET_REQUEST_TIMEOUT_SEC) // 2

# The deadline of the query that holds a shared connection's handshake. Long enough that the second
# query's much shorter one is what ends it, short enough to keep the test brief.
SHARED_HOLDER_TIMEOUT_SEC = 20
# Midway between the second query's two outcomes: its own deadline, or the holder's deadline
# followed by its own, which is what a handshake on the connection's locked path costs it.
SHARED_SECOND_QUERY_BOUND_SEC = (
    2 * STALL_REQUEST_TIMEOUT_SEC + SHARED_HOLDER_TIMEOUT_SEC
) // 2

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/remote_host_filter.xml"],
    with_arrowflight=True,
    stay_alive=True,
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_table_function():
    result = node.query("SELECT * FROM arrowFlight('arrowflight1:5005', 'ABC')")
    assert result == TSV(
        [
            ["test_value_1", "data1"],
            ["abcadbc", "text_text_text"],
            ["123456789", "data3"],
        ]
    )
    
    # test that dataset_name is being sent correctly to the arrowflight server
    result = node.query("SELECT * FROM arrowFlight('arrowflight1:5005', 'XYZ')")
    assert result == TSV(
        [
            ["1", "4"],
            ["2", "5"],
            ["3", "6"],
        ]
    )


def test_table_function_old_name():
    # "arrowflight" is an obsolete name.
    result = node.query("SELECT * FROM arrowflight('arrowflight1:5005', 'ABC')")
    assert result == TSV(
        [
            ["test_value_1", "data1"],
            ["abcadbc", "text_text_text"],
            ["123456789", "data3"],
        ]
    )
    
    result = node.query("SELECT * FROM arrowflight('arrowflight1:5005', 'XYZ')")
    assert result == TSV(
        [
            ["1", "4"],
            ["2", "5"],
            ["3", "6"],
        ]
    )


def test_table_function_with_auth():
    result = node.query(
        f"SELECT * FROM arrowFlight('arrowflight1:5006', 'ABC', '{arrowflight_user}', '{arrowflight_pass}')"
    )
    assert result == TSV(
        [
            ["test_value_1", "data1"],
            ["abcadbc", "text_text_text"],
            ["123456789", "data3"],
        ]
    )

    assert "No credentials supplied" in node.query_and_get_error(
        "SELECT * FROM arrowFlight('arrowflight1:5006', 'ABC')"
    )
    assert "Unknown user" in node.query_and_get_error(
        "SELECT * FROM arrowFlight('arrowflight1:5006', 'ABC', 'default', '')"
    )
    assert "Wrong password" in node.query_and_get_error(
        f"SELECT * FROM arrowFlight('arrowflight1:5006', 'ABC', '{arrowflight_user}', 'qwe123')"
    )


def test_arrowflight_storage():
    dataset = uuid.uuid4().hex

    node.query(
        f"""
        CREATE TABLE arrow_test (
            column1 String,
            column2 String
        ) ENGINE=ArrowFlight('arrowflight1:5005', '{dataset}')
        """
    )

    assert node.query("SELECT * FROM arrow_test") == ""

    node.query(
        "INSERT INTO arrow_test VALUES ('a','data_a'), ('b','data_b'), ('c','data_c')"
    )

    result = node.query("SELECT * FROM arrow_test ORDER BY column1")
    assert result == TSV(
        [
            ["a", "data_a"],
            ["b", "data_b"],
            ["c", "data_c"],
        ]
    )

    node.query("INSERT INTO arrow_test VALUES ('x','data_x'), ('y','data_y')")

    new_result = node.query("SELECT * FROM arrow_test ORDER BY column1")
    assert new_result == TSV(
        [
            ["a", "data_a"],
            ["b", "data_b"],
            ["c", "data_c"],
            ["x", "data_x"],
            ["y", "data_y"],
        ]
    )

    table_func_result = node.query(
        f"SELECT * FROM arrowFlight('arrowflight1:5005', '{dataset}') ORDER BY column1"
    )
    assert table_func_result == TSV(
        [
            ["a", "data_a"],
            ["b", "data_b"],
            ["c", "data_c"],
            ["x", "data_x"],
            ["y", "data_y"],
        ]
    )

    node.query("DROP TABLE arrow_test")


def test_arrowflight_storage_virtual_column_table():
    dataset = uuid.uuid4().hex

    node.query(
        f"""
        CREATE TABLE arrow_virtual_test (
            column1 String,
            column2 String
        ) ENGINE=ArrowFlight('arrowflight1:5005', '{dataset}')
        """
    )

    node.query(
        "INSERT INTO arrow_virtual_test VALUES ('a','data_a'), ('b','data_b')"
    )

    # Select only the _table virtual column
    result = node.query("SELECT _table FROM arrow_virtual_test ORDER BY column1")
    assert result == TSV(
        [
            ["arrow_virtual_test"],
            ["arrow_virtual_test"],
        ]
    )

    # Select physical and virtual columns together
    result = node.query(
        "SELECT column1, _table FROM arrow_virtual_test ORDER BY column1"
    )
    assert result == TSV(
        [
            ["a", "arrow_virtual_test"],
            ["b", "arrow_virtual_test"],
        ]
    )

    # Select all columns plus virtual
    result = node.query(
        "SELECT *, _table FROM arrow_virtual_test ORDER BY column1"
    )
    assert result == TSV(
        [
            ["a", "data_a", "arrow_virtual_test"],
            ["b", "data_b", "arrow_virtual_test"],
        ]
    )

    node.query("DROP TABLE arrow_virtual_test")


def test_table_function_with_named_collection():
    """Test that ArrowFlight table function works with named collections and dataset parameter."""
    # Create a named collection for ArrowFlight
    node.query("""
        CREATE NAMED COLLECTION arrowflight_test_collection AS
        host = 'arrowflight1',
        port = 5005,
        dataset = 'ABC',
        use_basic_authentication = False
    """)
    
    # Test that the table function works with the named collection
    result = node.query("SELECT * FROM arrowFlight(arrowflight_test_collection)")
    assert result == TSV(
        [
            ["test_value_1", "data1"],
            ["abcadbc", "text_text_text"],
            ["123456789", "data3"],
        ]
    )
    
    # Test that different dataset param causes different data to be returned
    result_xyz = node.query("SELECT * FROM arrowFlight(arrowflight_test_collection, dataset = 'XYZ')")
    assert result_xyz == TSV(
        [
            ["1", "4"],
            ["2", "5"],
            ["3", "6"],
        ]
    )
    
    
    
    # Clean up
    node.query("DROP NAMED COLLECTION arrowflight_test_collection")


def test_table_function_with_named_collection_auth():
    """Test that ArrowFlight table function works with named collections including authentication."""
    # Create a named collection with authentication
    node.query(f"""
        CREATE NAMED COLLECTION arrowflight_auth_collection AS
        host = 'arrowflight1',
        port = 5006,
        dataset = 'ABC',
        username = '{arrowflight_user}',
        password = '{arrowflight_pass}',
        use_basic_authentication = True
    """)
    
    # Test that the table function works with the named collection
    result = node.query("SELECT * FROM arrowFlight(arrowflight_auth_collection)")
    assert result == TSV(
        [
            ["test_value_1", "data1"],
            ["abcadbc", "text_text_text"],
            ["123456789", "data3"],
        ]
    )
    
    result_xyz = node.query("SELECT * FROM arrowFlight(arrowflight_auth_collection, dataset = 'XYZ')")
    assert result_xyz == TSV(
        [
            ["1", "4"],
            ["2", "5"],
            ["3", "6"],
        ]
    )
    
    # Clean up
    node.query("DROP NAMED COLLECTION arrowflight_auth_collection")


def test_arrowflight_storage_with_named_collection():
    """Test that ArrowFlight storage engine works with named collections and dataset parameter."""
    dataset1 = uuid.uuid4().hex
    dataset2 = uuid.uuid4().hex
    # Create a named collection for ArrowFlight storage
    node.query(f"""
        CREATE NAMED COLLECTION arrowflight_storage_collection AS
        host = 'arrowflight1',
        port = 5005,
        dataset = '{dataset1}',
        use_basic_authentication = False
    """)
    
    # Create table using the named collection
    node.query(f"""
        CREATE TABLE arrow_test_named (
            column1 String,
            column2 String
        ) ENGINE=ArrowFlight(arrowflight_storage_collection, dataset = '{dataset1}')
    """)
    
    node.query(f"""
        CREATE TABLE arrow_test_named_2 (
            column1 String,
            column2 String
        ) ENGINE=ArrowFlight(arrowflight_storage_collection, dataset = '{dataset2}')
    """)
    
    # Insert data
    node.query(
        "INSERT INTO arrow_test_named VALUES ('a','data_a'), ('b','data_b'), ('c','data_c')"
    )
    
    node.query(
        "INSERT INTO arrow_test_named_2 VALUES ('x','data_x'), ('y','data_y')"
    )
    
    # Verify data can be read
    result = node.query("SELECT * FROM arrow_test_named ORDER BY column1")
    assert result == TSV(
        [
            ["a", "data_a"],
            ["b", "data_b"],
            ["c", "data_c"],
        ]
    )
    
    result_xyz = node.query("SELECT * FROM arrow_test_named_2 ORDER BY column1")
    assert result_xyz == TSV(
        [
            ["x", "data_x"],
            ["y", "data_y"],
        ]
    )
    
    # Test table function with the same dataset
    table_func_result = node.query(
        f"SELECT * FROM arrowFlight('arrowflight1:5005', '{dataset1}') ORDER BY column1"
    )
    assert table_func_result == TSV(
        [
            ["a", "data_a"],
            ["b", "data_b"],
            ["c", "data_c"],
        ]
    )
    
    table_func_result_xyz = node.query(
        f"SELECT * FROM arrowFlight('arrowflight1:5005', '{dataset2}') ORDER BY column1"
    )
    assert table_func_result_xyz == TSV(
        [
            ["x", "data_x"],
            ["y", "data_y"],
        ]
    )
    
    # Clean up
    node.query("DROP TABLE arrow_test_named")
    node.query("DROP TABLE arrow_test_named_2")
    node.query("DROP NAMED COLLECTION arrowflight_storage_collection")


def test_remote_host_filter():
    # Only "arrowflight1" is allow-listed in configs/remote_host_filter.xml,
    # so a connection to any other host must be rejected before it is opened.
    error = node.query_and_get_error(
        "SELECT * FROM arrowFlight('127.0.0.1:5005', 'ABC')"
    )
    assert "not allowed in configuration file" in error

    error = node.query_and_get_error(
        """
        CREATE TABLE arrow_blocked (column1 String, column2 String)
        ENGINE=ArrowFlight('127.0.0.1:5005', 'ABC')
        """
    )
    assert "not allowed in configuration file" in error

    # The named-collection branch of getConfiguration is also guarded: creating
    # the collection is harmless (no connection), but using it must be rejected.
    node.query(
        """
        CREATE NAMED COLLECTION arrowflight_blocked_collection AS
        host = '127.0.0.1',
        port = 5005,
        dataset = 'ABC',
        use_basic_authentication = False
        """
    )
    try:
        error = node.query_and_get_error(
            "SELECT * FROM arrowFlight(arrowflight_blocked_collection)"
        )
        assert "not allowed in configuration file" in error
    finally:
        node.query("DROP NAMED COLLECTION arrowflight_blocked_collection")


def assert_query_timed_out_quickly(query, expected_error_code):
    start = time.time()
    error = node.query_and_get_error(
        query,
        settings={"arrow_flight_request_timeout_sec": STALL_REQUEST_TIMEOUT_SEC},
    )
    elapsed = time.time() - start
    # The error code identifies which RPC the deadline fired on, so each caller pins a
    # different blocking frame. "TimedOut" is Arrow's own reason and separates a deadline
    # from any other failure against the same dataset.
    assert expected_error_code in error, error
    assert "TimedOut" in error, error
    # elapsed is measured around the whole client invocation, which strictly contains the
    # deadline window, and a deadline cannot fire before its absolute time. So the configured
    # value brackets the run from below and a mis-scaled deadline cannot pass unnoticed.
    assert (
        elapsed >= STALL_REQUEST_TIMEOUT_SEC - 0.5
    ), f"query returned after {elapsed:.1f}s, before its {STALL_REQUEST_TIMEOUT_SEC}s deadline"
    assert elapsed < STALL_QUERY_BOUND_SEC, f"query took {elapsed:.1f}s"


def assert_timed_out_quickly(dataset, expected_error_code):
    assert_query_timed_out_quickly(
        f"SELECT * FROM arrowFlight('arrowflight1:5005', '{dataset}')",
        expected_error_code,
    )


def test_stalled_flight_server_metadata_does_not_hang():
    # GetSchema, issued while the query is still being analysed.
    assert_timed_out_quickly("STALL_SCHEMA", "ARROWFLIGHT_FETCH_SCHEMA_ERROR")


def test_stalled_flight_server_endpoints_do_not_hang():
    # GetFlightInfo, issued while the read pipeline is built.
    assert_timed_out_quickly("STALL_FLIGHT_INFO", "ARROWFLIGHT_FETCH_SCHEMA_ERROR")


def test_stalled_flight_server_handshake_does_not_hang():
    # The Handshake of AuthenticateBasicToken, before any dataset is named. The stub stalls
    # ahead of its credential check, so the user need not exist.
    assert_query_timed_out_quickly(
        "SELECT * FROM arrowFlight('arrowflight1:5006', 'ABC', 'stall_handshake', 'x')",
        "ARROWFLIGHT_CONNECTION_FAILURE",
    )


def test_timed_out_handshake_does_not_poison_a_reused_connection():
    # The engine keeps one connection for the table's lifetime, so a handshake that timed out must
    # leave nothing behind: the next query has to attempt its own handshake rather than reuse a
    # client whose options carry no authentication header.
    node.query(
        """
        CREATE TABLE arrow_stall_handshake_reuse (
            column1 String,
            column2 String
        ) ENGINE=ArrowFlight('arrowflight1:5006', 'ABC', 'stall_handshake', 'x')
        """
    )
    try:
        for attempt in range(2):
            start = time.time()
            error = node.query_and_get_error(
                "SELECT * FROM arrow_stall_handshake_reuse",
                settings={"arrow_flight_request_timeout_sec": STALL_REQUEST_TIMEOUT_SEC},
            )
            elapsed = time.time() - start
            assert "ARROWFLIGHT_CONNECTION_FAILURE" in error, (attempt, error)
            assert "TimedOut" in error, (attempt, error)
            assert (
                elapsed >= STALL_REQUEST_TIMEOUT_SEC - 0.5
            ), f"attempt {attempt} returned after {elapsed:.1f}s"
            assert elapsed < STALL_QUERY_BOUND_SEC, f"attempt {attempt} took {elapsed:.1f}s"
    finally:
        node.query("DROP TABLE arrow_stall_handshake_reuse")


def test_request_timeout_is_per_query_on_a_shared_connection():
    # Every query on an engine table uses the table's one connection, so a handshake run while that
    # connection is locked would decide how long the other queries wait: they reach their own RPC,
    # and the deadline on it, only once the holder is done. Neither waiter can be interrupted there,
    # in ISink::work for an INSERT and while the read pipeline is built for a SELECT.
    query_id = uuid.uuid4().hex
    node.query(
        """
        CREATE TABLE arrow_stall_handshake_shared (
            column1 String,
            column2 String
        ) ENGINE=ArrowFlight('arrowflight1:5006', 'ABC', 'stall_handshake', 'x')
        """
    )
    holder = node.get_query_request(
        "SELECT * FROM arrow_stall_handshake_shared",
        query_id=query_id,
        settings={"arrow_flight_request_timeout_sec": SHARED_HOLDER_TIMEOUT_SEC},
        timeout=SHARED_HOLDER_TIMEOUT_SEC + 30,
        ignore_error=True,
    )
    try:
        # Wait for the holder to have been executing a while, so its handshake is already in flight:
        # nothing else that query does takes anywhere near a second.
        assert_eq_with_retry(
            node,
            f"SELECT elapsed >= 2 FROM system.processes WHERE query_id = '{query_id}'",
            "1",
            retry_count=SHARED_HOLDER_TIMEOUT_SEC,
            sleep_time=1,
        )

        start = time.time()
        error = node.query_and_get_error(
            "SELECT * FROM arrow_stall_handshake_shared",
            settings={"arrow_flight_request_timeout_sec": STALL_REQUEST_TIMEOUT_SEC},
        )
        elapsed = time.time() - start
        holders_left = node.query(
            f"SELECT count() FROM system.processes WHERE query_id = '{query_id}'"
        )

        assert "ARROWFLIGHT_CONNECTION_FAILURE" in error, error
        assert "TimedOut" in error, error
        assert (
            elapsed >= STALL_REQUEST_TIMEOUT_SEC - 0.5
        ), f"query returned after {elapsed:.1f}s, before its own deadline"
        assert (
            elapsed < SHARED_SECOND_QUERY_BOUND_SEC
        ), f"query took {elapsed:.1f}s, so it waited for the holder's deadline"
        # Only meaningful with the bounds above: the holder outlived this query, so the connection
        # really was still unpublished throughout it. Had the holder finished first, this query
        # would have been the one running the handshake and bounded by its own deadline anyway.
        assert (
            holders_left == "1\n"
        ), "the holder was already gone, so this query did not share an unfinished handshake"
    finally:
        holder.get_answer_and_error()
        node.query("DROP TABLE arrow_stall_handshake_shared")


def test_stalled_flight_server_insert_does_not_hang():
    node.query(
        """
        CREATE TABLE arrow_stall_insert (
            column1 String,
            column2 String
        ) ENGINE=ArrowFlight('arrowflight1:5005', 'STALL_DOPUT')
        """
    )
    try:
        start = time.time()
        error = node.query_and_get_error(
            "INSERT INTO arrow_stall_insert VALUES ('a','data_a')",
            settings={"arrow_flight_request_timeout_sec": STALL_REQUEST_TIMEOUT_SEC},
        )
        elapsed = time.time() - start
        # Every DoPut-family failure reports ARROWFLIGHT_WRITE_ERROR, so the frame is named
        # by the message rather than the code.
        assert "ARROWFLIGHT_WRITE_ERROR" in error, error
        assert "TimedOut" in error, error
        assert (
            elapsed >= STALL_REQUEST_TIMEOUT_SEC - 0.5
        ), f"insert returned after {elapsed:.1f}s, before its {STALL_REQUEST_TIMEOUT_SEC}s deadline"
        assert elapsed < STALL_QUERY_BOUND_SEC, f"insert took {elapsed:.1f}s"
    finally:
        node.query("DROP TABLE arrow_stall_insert")


def test_stalled_flight_server_before_first_message_does_not_hang():
    # Inside DoGet, which reads the stream's first message before handing back a reader.
    assert_timed_out_quickly("STALL_DOGET", "ARROWFLIGHT_CONNECTION_FAILURE")


def test_stalled_flight_server_mid_stream_does_not_hang():
    # A read of the open stream, i.e. inside ISource::work.
    assert_timed_out_quickly("STALL_STREAM", "ARROWFLIGHT_INTERNAL_ERROR")


def test_request_timeout_magnitude_is_honored():
    # A second, larger bound: the deadline is the only thing that can end this query, so its
    # elapsed time is bracketed by the configured value on both sides. A deadline scaled up
    # rather than honored lands outside STALL_QUERY_BOUND_SEC here while the 3s tests, whose
    # inflated value is still inside it, stay green.
    start = time.time()
    error = node.query_and_get_error(
        "SELECT * FROM arrowFlight('arrowflight1:5005', 'STALL_SCHEMA')",
        settings={"arrow_flight_request_timeout_sec": 10},
    )
    elapsed = time.time() - start
    assert "ARROWFLIGHT_FETCH_SCHEMA_ERROR" in error, error
    assert "TimedOut" in error, error
    assert elapsed >= 9.5, f"query returned after {elapsed:.1f}s, before its 10s deadline"
    assert elapsed < STALL_QUERY_BOUND_SEC, f"query took {elapsed:.1f}s"


def test_request_timeout_is_not_lengthened():
    # SLOW_SCHEMA_THEN_ANSWER answers its GetSchema after SLOW_SCHEMA_ANSWER_SECONDS (5s in the
    # stub), so with a 3s bound the deadline has to fire first. A deadline stretched past the stub's
    # delay turns this query into a success returning no rows, which query_and_get_error rejects.
    # Both instants are absolute and start from the same RPC, so the margin does not depend on how
    # loaded the runner is.
    assert_timed_out_quickly("SLOW_SCHEMA_THEN_ANSWER", "ARROWFLIGHT_FETCH_SCHEMA_ERROR")


def test_kill_query_interrupts_a_stalled_flight_read():
    query_id = uuid.uuid4().hex

    # The deadline is switched off, so the cancellation path is the only thing that can end
    # this query. STALL_STREAM is the only stalling dataset with a reader to cancel.
    request = node.get_query_request(
        "SELECT * FROM arrowFlight('arrowflight1:5005', 'STALL_STREAM')",
        query_id=query_id,
        settings={"arrow_flight_request_timeout_sec": 0},
        timeout=STALL_QUERY_BOUND_SEC + 30,
        ignore_error=True,
    )
    count_query = f"SELECT count() FROM system.processes WHERE query_id = '{query_id}'"
    try:
        # Wait for the stub's first batch to be read, not merely for the query to appear: a kill
        # that lands earlier is reported before execution starts and never reaches the read.
        assert_eq_with_retry(
            node,
            f"SELECT read_rows > 0 FROM system.processes WHERE query_id = '{query_id}'",
            "1",
            retry_count=STALL_QUERY_BOUND_SEC,
            sleep_time=1,
        )

        node.query(f"KILL QUERY WHERE query_id = '{query_id}' ASYNC")

        assert_eq_with_retry(
            node, count_query, "0", retry_count=STALL_QUERY_BOUND_SEC, sleep_time=1
        )
    finally:
        _, stderr = request.get_answer_and_error()

    assert "QUERY_WAS_CANCELLED" in stderr, stderr


def test_max_execution_time_interrupts_a_stalled_flight_read():
    # The deadline is switched off, so only cancellation can end this query, and
    # max_execution_time reaches it through CancellationChecker, not the KILL QUERY interpreter.
    start = time.time()
    error = node.query_and_get_error(
        "SELECT * FROM arrowFlight('arrowflight1:5005', 'STALL_STREAM')",
        settings={"arrow_flight_request_timeout_sec": 0, "max_execution_time": 5},
    )
    elapsed = time.time() - start
    assert "TIMEOUT_EXCEEDED" in error, error
    assert elapsed < STALL_QUERY_BOUND_SEC, f"query took {elapsed:.1f}s"


def test_cancellation_during_doget_is_not_lost():
    # A cancellation raised while DoGet is in flight finds no reader to abort, so the reader DoGet
    # goes on to return has to be aborted where it is published. Otherwise the read that follows
    # blocks until the request deadline instead of ending with the cancellation.
    start = time.time()
    error = node.query_and_get_error(
        "SELECT * FROM arrowFlight('arrowflight1:5005', 'SLOW_DOGET_THEN_STALL')",
        settings={
            "arrow_flight_request_timeout_sec": DOGET_REQUEST_TIMEOUT_SEC,
            "max_execution_time": DOGET_CANCEL_AT_SEC,
        },
    )
    elapsed = time.time() - start
    assert "TIMEOUT_EXCEEDED" in error, error
    # The query cannot return before DoGet does, so an earlier return means the cancellation landed
    # before the read was ever issued and the run says nothing about this frame.
    assert (
        elapsed >= DOGET_DELAY_SEC - 0.5
    ), f"query returned after {elapsed:.1f}s, before DoGet could return"
    assert elapsed < DOGET_STOP_BOUND_SEC, f"query took {elapsed:.1f}s"


def test_huge_request_timeout_does_not_break_a_healthy_read():
    # The largest value the setting accepts must still leave a usable future deadline, so a
    # healthy read completes rather than failing as if its deadline had already passed.
    result = node.query(
        "SELECT * FROM arrowFlight('arrowflight1:5005', 'ABC')",
        settings={"arrow_flight_request_timeout_sec": 18446744073709551615},
    )
    assert result == TSV(
        [
            ["test_value_1", "data1"],
            ["abcadbc", "text_text_text"],
            ["123456789", "data3"],
        ]
    )
