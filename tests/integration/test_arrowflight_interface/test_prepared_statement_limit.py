# coding: utf-8

import pytest
import pyarrow as pa
import pyarrow.flight as flight
import random
import string
import decimal
import datetime

from .flight_sql_client import FlightSQLClient

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/flight_port.xml",
        "configs/small_prepared_statements_limit.xml",
    ],
    user_configs=["configs/users_prepared_statements.xml"],
)


def get_client(username=None, password=None):
    session_id = ''.join(random.choices(string.ascii_letters + string.digits, k=16))
    return FlightSQLClient(
        host=node.ip_address,
        port=8888,
        insecure=True,
        disable_server_verification=True,
        username=username,
        password=password,
        metadata={'x-clickhouse-session-id': session_id},
        features={'metadata-reflection': 'true'},
    )


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        node.wait_until_port_is_ready(8888, timeout=10)
        yield cluster
    finally:
        cluster.shutdown()


def test_prepared_statement_limit_exceeded():
    """Creating more prepared statements than the per-user limit should fail."""
    client = get_client()

    stmts = []
    # Limit is 3; create exactly 3 — should succeed.
    for i in range(3):
        stmt = client.prepare(f"SELECT {i + 1}")
        stmts.append(stmt)

    # The 4th should fail with a capacity error.
    with pytest.raises(flight.FlightServerError, match="Too many prepared statements"):
        client.prepare("SELECT 4")

    # Clean up
    for stmt in stmts:
        stmt.close()


def test_prepared_statement_limit_after_close():
    """After closing a prepared statement, a new one can be created within the limit."""
    client = get_client()

    stmts = []
    for i in range(3):
        stmts.append(client.prepare(f"SELECT {i + 1}"))

    # Close one to free a slot.
    stmts[0].close()
    stmts.pop(0)

    # Now creating one more should succeed.
    stmts.append(client.prepare("SELECT 42"))

    # But exceeding again should fail.
    with pytest.raises(flight.FlightServerError, match="Too many prepared statements"):
        client.prepare("SELECT 99")

    for stmt in stmts:
        stmt.close()


def test_prepared_statement_limit_is_per_user():
    """Different users each get their own limit."""
    client1 = get_client("user_ps1", "pass1")
    client2 = get_client("user_ps2", "pass2")

    stmts1 = []
    stmts2 = []

    # Each user can create up to 3.
    for i in range(3):
        stmts1.append(client1.prepare(f"SELECT {i + 1}"))
        stmts2.append(client2.prepare(f"SELECT {i + 10}"))

    # Both should fail on the 4th.
    with pytest.raises(flight.FlightServerError, match="Too many prepared statements"):
        client1.prepare("SELECT 100")
    with pytest.raises(flight.FlightServerError, match="Too many prepared statements"):
        client2.prepare("SELECT 200")

    for stmt in stmts1 + stmts2:
        stmt.close()


def test_prepared_statement_accessible_across_sessions():
    """A prepared statement created in one session can be used from another session of the same user."""
    client1 = get_client()
    client2 = get_client()

    # Create and bind a prepared statement in the first session.
    stmt1 = client1.prepare("SELECT ? + ?")
    params = pa.record_batch(
        [pa.array([10], type=pa.int32()), pa.array([20], type=pa.int32())],
        names=["0", "1"],
    )
    stmt1.bind_parameters(params)

    # Execute the same prepared statement from the second session.
    result = client2.execute(stmt1)
    assert result.column(0).to_pylist() == [30]

    # Close from either session.
    client2.close_prepared_statement(stmt1.handle)


def test_prepared_statement_not_accessible_by_other_user():
    """A prepared statement created by one user cannot be accessed by another user."""
    client1 = get_client("user_ps1", "pass1")
    client2 = get_client("user_ps2", "pass2")

    stmt = client1.prepare("SELECT 1")

    # Another user cannot execute it.
    with pytest.raises(pa.lib.ArrowKeyError, match="Prepared statement handle not found"):
        client2.execute(stmt)

    # Another user cannot close it (silently ignored per Flight SQL spec).
    client2.close_prepared_statement(stmt.handle)
    # Verify it's still alive for the owner.
    result = client1.execute(stmt)
    assert result.column(0).to_pylist() == [1]

    # The owner can still use and close it.
    stmt.close()


def test_prepared_statement_execute_without_bind():
    """Executing a prepared statement with parameters without binding should fail."""
    client = get_client()

    stmt = client.prepare("SELECT ?")

    # Execute without binding parameters — must error, not silently substitute NULLs.
    with pytest.raises(pa.lib.ArrowInvalid, match="Parameters were not bound"):
        client.execute(stmt)

    # After binding, execution should succeed.
    params = pa.record_batch([pa.array([42], type=pa.int32())], names=["0"])
    stmt.bind_parameters(params)
    result = stmt.execute()
    assert result.column(0).to_pylist() == [42]

    stmt.close()


def test_prepared_statement_execute_without_bind_no_params():
    """Executing a parameterless prepared statement without binding should succeed."""
    client = get_client()

    stmt = client.prepare("SELECT 1")
    result = stmt.execute()
    assert result.column(0).to_pylist() == [1]

    stmt.close()


@pytest.mark.parametrize("test_case", [
    # (arrow_type, arrow_value, expected_result, description)
    # Boolean
    ("bool_true", pa.array([True], type=pa.bool_()), 1),
    ("bool_false", pa.array([False], type=pa.bool_()), 0),
    # Integer types
    ("int8", pa.array([42], type=pa.int8()), 42),
    ("int16", pa.array([1000], type=pa.int16()), 1000),
    ("int32", pa.array([100000], type=pa.int32()), 100000),
    ("int64", pa.array([2**40], type=pa.int64()), 2**40),
    ("uint8", pa.array([200], type=pa.uint8()), 200),
    ("uint16", pa.array([60000], type=pa.uint16()), 60000),
    ("uint32", pa.array([3000000000], type=pa.uint32()), 3000000000),
    ("uint64", pa.array([2**50], type=pa.uint64()), 2**50),
    # Floating-point types
    ("float32", pa.array([3.14], type=pa.float32()), pytest.approx(3.14, abs=1e-5)),
    ("float64", pa.array([2.718281828], type=pa.float64()), pytest.approx(2.718281828, abs=1e-9)),
    # String types
    ("string", pa.array(["hello"], type=pa.string()), "hello"),
    ("string_with_quotes", pa.array(["it's a \"test\""], type=pa.string()), "it's a \"test\""),
    ("string_with_backslash", pa.array(["path\\to\\file"], type=pa.string()), "path\\to\\file"),
    ("large_string", pa.array(["large"], type=pa.large_string()), "large"),
    # Binary types
    ("binary", pa.array([b"\x01\x02\x03"], type=pa.binary()), b"\x01\x02\x03"),
    ("large_binary", pa.array([b"\xaa\xbb"], type=pa.large_binary()), b"\xaa\xbb"),
    # Date types — Arrow date32 ToString produces "YYYY-MM-DD" which must be quoted
    ("date32", pa.array([datetime.date(2021, 6, 15)], type=pa.date32()), datetime.date(2021, 6, 15)),
    # Timestamp
    ("timestamp_us", pa.array([datetime.datetime(2023, 3, 14, 12, 30, 45)], type=pa.timestamp("us")), datetime.datetime(2023, 3, 14, 12, 30, 45)),
    # Decimal
    ("decimal128", pa.array([decimal.Decimal("123.45")], type=pa.decimal128(10, 2)), decimal.Decimal("123.45")),
    # NULL
    ("null_int", pa.array([None], type=pa.int32()), None),
    ("null_string", pa.array([None], type=pa.string()), None),
], ids=lambda x: x if isinstance(x, str) else "")
def test_prepared_statement_parameter_types(test_case):
    """Verify that all Arrow parameter types are correctly converted to SQL literals."""
    name, arr, expected = test_case
    client = get_client()

    stmt = client.prepare("SELECT ? AS result")
    params = pa.record_batch([arr], names=["0"])
    stmt.bind_parameters(params)
    result = stmt.execute()

    if isinstance(expected, bytes):
        # ClickHouse returns binary data in a String column, which Arrow maps to utf8.
        # Cast to binary to avoid pyarrow's UTF-8 decoding.
        actual = result.column("result").cast(pa.binary()).to_pylist()[0]
    else:
        actual = result.column("result").to_pylist()[0]

    if isinstance(expected, bytes):
        assert actual == expected, f"[{name}] expected {expected!r}, got {actual!r}"
    elif expected is None:
        assert actual is None, f"[{name}] expected None, got {actual!r}"
    elif isinstance(expected, datetime.datetime):
        if isinstance(actual, str):
            actual = datetime.datetime.fromisoformat(actual)
        assert actual == expected, f"[{name}] expected {expected}, got {actual}"
    elif isinstance(expected, datetime.date):
        if isinstance(actual, str):
            actual = datetime.date.fromisoformat(actual)
        assert actual == expected, f"[{name}] expected {expected}, got {actual}"
    elif isinstance(expected, decimal.Decimal):
        assert decimal.Decimal(str(actual)) == expected, f"[{name}] expected {expected}, got {actual}"
    else:
        assert actual == expected, f"[{name}] expected {expected!r}, got {actual!r}"

    stmt.close()


def test_prepared_statement_schema_returned_on_prepare():
    """When NULL substitution succeeds, prepare should return dataset_schema."""
    client = get_client()

    stmt = client.prepare("SELECT ? AS result")
    assert stmt.dataset_schema is not None
    assert len(stmt.dataset_schema) == 1
    assert stmt.dataset_schema.field(0).name == "result"
    stmt.close()


def test_prepared_statement_no_schema_when_null_invalid():
    """When NULL substitution fails (e.g. numbers(?)), prepare should still succeed
    but dataset_schema should be None."""
    client = get_client()

    stmt = client.prepare("SELECT * FROM numbers(?)")
    assert stmt.dataset_schema is None
    assert stmt.parameter_schema is not None
    assert len(stmt.parameter_schema) == 1

    # Executing with a real value should work.
    params = pa.record_batch([pa.array([5], type=pa.int64())], names=["0"])
    stmt.bind_parameters(params)
    result = stmt.execute()
    assert result.column("number").to_pylist() == [0, 1, 2, 3, 4]
    stmt.close()


def test_close_all_prepared_statements():
    """With a session, close-all only closes statements in that session."""
    client1 = get_client("user_ps1", "pass1")
    client2 = get_client("user_ps1", "pass1")  # same user, different session

    # Create 2 in session 1, 1 in session 2 (limit is 3 total per user).
    stmt_s1_a = client1.prepare("SELECT 1")
    stmt_s1_b = client1.prepare("SELECT 2")
    stmt_s2 = client2.prepare("SELECT 3")

    # Limit reached — 4th should fail from either session.
    with pytest.raises(flight.FlightServerError, match="Too many prepared statements"):
        client1.prepare("SELECT 99")

    # Close all in session 1 only.
    client1.close_all_prepared_statements()

    # Session 1's statements are gone.
    with pytest.raises(pa.lib.ArrowKeyError, match="Prepared statement handle not found"):
        client1.execute(stmt_s1_a)

    # Session 2's statement is still alive.
    result = client2.execute(stmt_s2)
    assert result.column(0).to_pylist() == [3]

    # Can create new statements again (2 slots freed).
    stmt_new = client1.prepare("SELECT 42")
    result = client1.execute(stmt_new)
    assert result.column(0).to_pylist() == [42]
    stmt_new.close()
    stmt_s2.close()


def test_close_all_does_not_affect_other_user():
    """Closing all statements for one user does not affect another user's statements."""
    client1 = get_client("user_ps1", "pass1")
    client2 = get_client("user_ps2", "pass2")

    stmt1 = client1.prepare("SELECT 1")
    stmt2 = client2.prepare("SELECT 2")

    # User 1 closes all their statements.
    client1.close_all_prepared_statements()

    # User 1's statement is gone.
    with pytest.raises(pa.lib.ArrowKeyError, match="Prepared statement handle not found"):
        client1.execute(stmt1)

    # User 2's statement is still alive.
    result = client2.execute(stmt2)
    assert result.column(0).to_pylist() == [2]
    stmt2.close()


def test_close_all_sessionless():
    """Without a session, close-all closes only session-less statements, not session-bound ones."""
    def get_sessionless_client(username=None, password=None):
        return FlightSQLClient(
            host=node.ip_address,
            port=8888,
            insecure=True,
            disable_server_verification=True,
            username=username,
            password=password,
            features={'metadata-reflection': 'true'},
        )

    session_client = get_client("user_ps1", "pass1")  # has a session
    sessionless_client = get_sessionless_client("user_ps1", "pass1")

    # Create 1 statement with a session, 2 without.
    stmt_session = session_client.prepare("SELECT 1")
    stmt_no_session_a = sessionless_client.prepare("SELECT 2")
    stmt_no_session_b = sessionless_client.prepare("SELECT 3")

    # Limit reached (3 total for user_ps1).
    with pytest.raises(flight.FlightServerError, match="Too many prepared statements"):
        sessionless_client.prepare("SELECT 99")

    # Close all session-less statements.
    sessionless_client.close_all_prepared_statements()

    # Session-less statements are gone.
    with pytest.raises(pa.lib.ArrowKeyError, match="Prepared statement handle not found"):
        sessionless_client.execute(stmt_no_session_a)
    with pytest.raises(pa.lib.ArrowKeyError, match="Prepared statement handle not found"):
        sessionless_client.execute(stmt_no_session_b)

    # Session-bound statement is still alive.
    result = session_client.execute(stmt_session)
    assert result.column(0).to_pylist() == [1]
    stmt_session.close()


def test_session_close_does_not_affect_other_user_same_session_id():
    """When two users share the same session_id, closing one user's session
    must not delete the other user's prepared statements."""
    shared_session_id = 'shared_session_' + ''.join(random.choices(string.ascii_letters, k=8))

    client1 = FlightSQLClient(
        host=node.ip_address,
        port=8888,
        insecure=True,
        disable_server_verification=True,
        username="user_ps1",
        password="pass1",
        metadata={'x-clickhouse-session-id': shared_session_id},
        features={'metadata-reflection': 'true'},
    )
    client2 = FlightSQLClient(
        host=node.ip_address,
        port=8888,
        insecure=True,
        disable_server_verification=True,
        username="user_ps2",
        password="pass2",
        metadata={'x-clickhouse-session-id': shared_session_id},
        features={'metadata-reflection': 'true'},
    )

    # Both users create a prepared statement in the same-named session.
    stmt1 = client1.prepare("SELECT 1")
    stmt2 = client2.prepare("SELECT 2")

    # User 1 closes their session by issuing a request with session-close header.
    close_client = FlightSQLClient(
        host=node.ip_address,
        port=8888,
        insecure=True,
        disable_server_verification=True,
        username="user_ps1",
        password="pass1",
        metadata={
            'x-clickhouse-session-id': shared_session_id,
            'x-clickhouse-session-close': '1',
        },
        features={'metadata-reflection': 'true'},
    )
    # Any call triggers the session close in CallCompleted.
    close_client.execute_update("SELECT 1")

    # User 1's prepared statement should be gone (session closed).
    with pytest.raises(pa.lib.ArrowKeyError, match="Prepared statement handle not found"):
        client1.execute(stmt1)

    # User 2's prepared statement must still be alive.
    result = client2.execute(stmt2)
    assert result.column(0).to_pylist() == [2]
    stmt2.close()
