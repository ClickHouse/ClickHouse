# coding: utf-8

import pytest
import pyarrow as pa
import random
import string

from .flight_sql_client import FlightSQLClient

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/flight_port.xml",
    ],
    user_configs=["configs/users_prepared_statements.xml"],
)


def get_client(username, password, session_id):
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


def get_sessionless_client(username, password):
    """Build a client that omits `x-clickhouse-session-id` entirely, so the
    Arrow Flight handler creates an ephemeral session per RPC. This is the
    code path where the prior shape of the `RESET SESSION` callback (with a
    captured empty `session_id`) would have erased *every* sessionless
    prepared statement for the user.
    """
    return FlightSQLClient(
        host=node.ip_address,
        port=8888,
        insecure=True,
        disable_server_verification=True,
        username=username,
        password=password,
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


def test_reset_session_closes_prepared_statements():
    """`RESET SESSION` must close every Arrow Flight prepared statement the
    session holds, so a pooled connection cannot continue resolving the old
    handle via `CommandPreparedStatementQuery` after the reset.
    """
    session_id = ''.join(random.choices(string.ascii_letters + string.digits, k=16))
    client = get_client("user_ps1", "pass1", session_id)

    stmt = client.prepare("SELECT 1")

    # Pre-reset: the prepared statement resolves.
    assert client.execute(stmt).column(0).to_pylist() == [1]

    # `RESET SESSION` returns no result, so route it through `execute_update`
    # (the `CommandStatementUpdate` / `do_put` path) rather than the
    # result-bearing `CommandStatementQuery` / `get_flight_info` path.
    client.execute_update("RESET SESSION")

    # Post-reset: the old handle is gone. The Arrow Flight server reports
    # this as a missing-handle key error.
    with pytest.raises(pa.lib.ArrowKeyError, match="Prepared statement handle not found"):
        client.execute(stmt)


def test_reset_session_does_not_touch_other_sessionless_clients():
    """Two sessionless Arrow Flight clients on the same user must not be able
    to invalidate each other's prepared statements via `RESET SESSION`. The
    Arrow Flight auth middleware only registers the reset callback for named
    sessions; an empty `x-clickhouse-session-id` would otherwise capture an
    empty `session_id` into the callback and
    `CallsData::closeAllPreparedStatements(username, "")` would erase every
    sessionless handle for that user.
    """
    client_a = get_sessionless_client("user_ps1", "pass1")
    client_b = get_sessionless_client("user_ps1", "pass1")

    stmt_a = client_a.prepare("SELECT 1")
    stmt_b = client_b.prepare("SELECT 2")

    # Both handles resolve before reset.
    assert client_a.execute(stmt_a).column(0).to_pylist() == [1]
    assert client_b.execute(stmt_b).column(0).to_pylist() == [2]

    # Client A runs `RESET SESSION`. Because the call is sessionless, the
    # auth middleware does not register a reset callback at all, so the
    # reset is a no-op for prepared-statement state on `CallsData`. Both
    # handles must still resolve afterwards — in particular client B's
    # handle must survive client A's reset.
    client_a.execute_update("RESET SESSION")

    assert client_a.execute(stmt_a).column(0).to_pylist() == [1]
    assert client_b.execute(stmt_b).column(0).to_pylist() == [2]
