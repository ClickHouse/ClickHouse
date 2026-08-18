# coding: utf-8

import time

import pytest
import pyarrow.flight as flight

from helpers.cluster import ClickHouseCluster


# Must match configs/bearer_token_timeout.xml
BEARER_TOKEN_TTL_SECONDS = 3

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/flight_port.xml",
        "configs/bearer_token_timeout.xml",
    ],
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        node.wait_until_port_is_ready(8888, timeout=10)
        yield cluster
    finally:
        cluster.shutdown()


def _do_select_one(client, options):
    ticket = flight.Ticket(b"SELECT 1")
    reader = client.do_get(ticket, options)
    table = reader.read_all()
    assert table.column(0)[0].as_py() == 1


def test_bearer_token_timeout_from_server_config():
    """Bearer token TTL follows arrowflight.bearer_token_timeout_seconds with sliding expiration."""
    client = flight.FlightClient(f"grpc://{node.ip_address}:8888")
    token_pair = client.authenticate_basic_token(b"default", b"")
    options = flight.FlightCallOptions(headers=[token_pair])

    _do_select_one(client, options)

    # Each Bearer-authenticated request refreshes expiry; stay within the sliding window.
    time.sleep(BEARER_TOKEN_TTL_SECONDS - 1)
    _do_select_one(client, options)

    time.sleep(BEARER_TOKEN_TTL_SECONDS - 1)
    _do_select_one(client, options)

    # No Bearer use since the last refresh; wait past the configured TTL.
    time.sleep(BEARER_TOKEN_TTL_SECONDS + 1)
    with pytest.raises(flight.FlightUnauthenticatedError, match="Bearer token expired or not authenticated"):
        _do_select_one(client, options)
