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
