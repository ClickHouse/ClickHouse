# coding: utf-8

import base64

import pyarrow.flight as flight
import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/flight_port.xml",
    ],
    user_configs=["configs/users.xml"],
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        node.wait_until_port_is_ready(8888, timeout=10)
        yield cluster
    finally:
        cluster.shutdown()


class _HeaderRecorder(flight.ClientMiddleware):
    def __init__(self, store):
        self.store = store

    def received_headers(self, headers):
        self.store.append({k.lower(): v for k, v in headers.items()})


class _HeaderRecorderFactory(flight.ClientMiddlewareFactory):
    def __init__(self):
        self.headers = []

    def start_call(self, info):
        return _HeaderRecorder(self.headers)


def _basic_auth_options():
    credentials = base64.b64encode(b"user1:qwe123").decode()
    return flight.FlightCallOptions(headers=[(b"authorization", f"Basic {credentials}".encode())])


def _run_query(client, options):
    descriptor = flight.FlightDescriptor.for_command("SELECT 1")
    info = client.get_flight_info(descriptor, options)
    reader = client.do_get(info.endpoints[0].ticket, options)
    return reader.read_all()


def test_handshake_mints_token():
    """The Handshake RPC exchanges Basic credentials for a bearer token, and the token works."""
    client = flight.FlightClient(f"grpc://{node.ip_address}:8888")
    header, value = client.authenticate_basic_token("user1", "qwe123")
    assert header == b"authorization"
    assert value.startswith(b"Bearer ")

    options = flight.FlightCallOptions(headers=[(header, value)])
    table = _run_query(client, options)
    assert table.num_rows == 1


def test_no_token_minted_outside_handshake():
    """A client that authenticates every call with Basic must not be handed a bearer token.

    Minting a token on every authenticated RPC made clients that adopt server-sent
    authorization headers (e.g. the arrow-adbc Flight SQL driver) silently replace their
    per-call credentials with a short-lived in-memory token; once it expired (idle timeout
    or server restart) such clients could not recover without reconnecting.
    """
    recorder = _HeaderRecorderFactory()
    client = flight.FlightClient(f"grpc://{node.ip_address}:8888", middleware=[recorder])
    options = _basic_auth_options()

    for _ in range(3):
        table = _run_query(client, options)
        assert table.num_rows == 1

    offered = [h["authorization"] for h in recorder.headers if "authorization" in h]
    assert offered == [], f"server must not push authorization headers outside Handshake, got: {offered}"


def test_per_call_basic_auth_is_stateless():
    """Per-call Basic authentication keeps working on a fresh connection (no session capture)."""
    client = flight.FlightClient(f"grpc://{node.ip_address}:8888")
    options = _basic_auth_options()
    assert _run_query(client, options).num_rows == 1

    # A second, completely fresh connection with the same credentials must also work:
    # nothing about the first connection's calls may be required for the second.
    client2 = flight.FlightClient(f"grpc://{node.ip_address}:8888")
    assert _run_query(client2, options).num_rows == 1
