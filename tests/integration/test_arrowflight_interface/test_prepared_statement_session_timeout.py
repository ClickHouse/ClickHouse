# coding: utf-8

import pytest
import pyarrow as pa
import time
import random
import string

from .flight_sql_client import FlightSQLClient

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/flight_port.xml",
        "configs/session_timeout_prepared_statements.xml",
    ],
    user_configs=["configs/users_prepared_statements.xml"],
)


def get_client(username, password, session_id, session_timeout=None):
    metadata = {'x-clickhouse-session-id': session_id}
    if session_timeout is not None:
        metadata['x-clickhouse-session-timeout'] = str(session_timeout)
    return FlightSQLClient(
        host=node.ip_address,
        port=8888,
        insecure=True,
        disable_server_verification=True,
        username=username,
        password=password,
        metadata=metadata,
        features={'metadata-reflection': 'true'},
    )


def random_session_id():
    return ''.join(random.choices(string.ascii_letters + string.digits, k=16))


PROBE_POLL_INTERVAL = 0.1
PROBE_EXPIRE_DEADLINE = 30.0
PROBE_ALIVE_WINDOW = 3.0


def _make_probe_client():
    # A same-user client on its own session. Every request on a prepared
    # statement's owning session refreshes its expiration
    # (AuthMiddleware::CallCompleted -> refreshSessionPreparedStatements),
    # so the handle may only be observed from a foreign session.
    return get_client("user_ps1", "pass1", random_session_id())


def _handle_gone(probe, handle):
    try:
        probe.get_prepared_statement_schema(handle)
    except pa.lib.ArrowKeyError as e:
        if "Prepared statement handle not found" in str(e):
            return True
        raise
    return False


def wait_prepared_statement_expires(handle, deadline=PROBE_EXPIRE_DEADLINE):
    probe = _make_probe_client()
    end = time.monotonic() + deadline
    while time.monotonic() < end:
        if _handle_gone(probe, handle):
            return
        time.sleep(PROBE_POLL_INTERVAL)
    pytest.fail(f"prepared statement {handle!r} was not expired within {deadline} s")


def assert_prepared_statement_alive(handle, seconds=PROBE_ALIVE_WINDOW):
    probe = _make_probe_client()
    start = time.monotonic()
    end = start + seconds
    while time.monotonic() < end:
        if _handle_gone(probe, handle):
            pytest.fail(
                f"prepared statement {handle!r} disappeared after "
                f"{time.monotonic() - start:.1f} s of the {seconds} s window"
            )
        time.sleep(PROBE_POLL_INTERVAL)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        node.wait_until_port_is_ready(8888, timeout=10)
        yield cluster
    finally:
        cluster.shutdown()


def test_session_timeout_drives_prepared_statement_expiration():
    """With use_session_timeout_for_ps_lifetime, the session timeout sets the prepared statement lifetime."""
    client = get_client("user_ps1", "pass1", random_session_id(), session_timeout=1)

    stmt = client.prepare("SELECT 1")

    # Immediately usable.
    result = client.execute(stmt)
    assert result.column(0).to_pylist() == [1]

    wait_prepared_statement_expires(stmt.handle)

    with pytest.raises(pa.lib.ArrowKeyError, match="Prepared statement handle not found"):
        client.execute(stmt)


def test_do_put_early_release_keeps_refresh_order():
    """A DoPut with a short session timeout releases the session early; its prepared-statement
    expiration refresh must stay ordered before the next request on the same session, so a
    following request with a longer timeout leaves the longer expiration in effect."""
    session_id = random_session_id()
    client_long = get_client("user_ps1", "pass1", session_id, session_timeout=30)
    client_short = get_client("user_ps1", "pass1", session_id, session_timeout=1)

    stmt = client_long.prepare("SELECT 1")

    # DoPut releases the session before writing its response metadata.
    client_short.execute_update(
        "CREATE TABLE IF NOT EXISTS ps_refresh_order (x UInt8) ENGINE=Memory"
    )
    try:
        # The next request on the session refreshes the expiration to now + 30s.
        client_long.execute("SELECT 1")

        assert_prepared_statement_alive(stmt.handle)

        result = client_long.execute(stmt)
        assert result.column(0).to_pylist() == [1]
    finally:
        client_long.execute_update("DROP TABLE IF EXISTS ps_refresh_order")
    stmt.close()
