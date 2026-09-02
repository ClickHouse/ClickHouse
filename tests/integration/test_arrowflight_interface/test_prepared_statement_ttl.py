# coding: utf-8

import pytest
import pyarrow as pa
import pyarrow.flight as flight
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
        "configs/small_prepared_statements_lifetime.xml",
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


def test_prepared_statement_expires_after_ttl():
    """A prepared statement becomes unavailable after the configured TTL."""
    client = get_client("user_ps1", "pass1")

    stmt = client.prepare("SELECT 1")

    # Immediately usable.
    result = client.execute(stmt)
    assert result.column(0).to_pylist() == [1]

    # Wait for TTL to expire (configured as 2 seconds, wait 3 to be safe).
    time.sleep(3)

    # Statement should be expired now.
    with pytest.raises(pa.lib.ArrowKeyError, match="Prepared statement handle not found"):
        client.execute(stmt)


def test_prepared_statement_ttl_frees_capacity():
    """After TTL expiration, the per-user limit slot is freed."""
    client = get_client("user_ps1", "pass1")

    # Fill up the limit (3).
    for i in range(3):
        client.prepare(f"SELECT {i + 1}")

    # 4th should fail.
    with pytest.raises(flight.FlightServerError, match="Too many prepared statements"):
        client.prepare("SELECT 99")

    # Wait for TTL to expire.
    time.sleep(3)

    # Slots are freed — can create new ones.
    stmt = client.prepare("SELECT 42")
    result = client.execute(stmt)
    assert result.column(0).to_pylist() == [42]
    stmt.close()


def test_prepared_statement_ttl_does_not_expire_before_time():
    """A prepared statement remains usable before the TTL elapses."""
    client = get_client("user_ps1", "pass1")

    stmt = client.prepare("SELECT 123")

    # Wait less than TTL.
    time.sleep(1)

    # Should still be usable.
    result = client.execute(stmt)
    assert result.column(0).to_pylist() == [123]
    stmt.close()
