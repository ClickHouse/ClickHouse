# coding: utf-8

import pytest
import pyarrow as pa
import pyarrow.flight as flight
import time
from datetime import datetime, timezone

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/flight_port.xml",
        "configs/small_ticket_lifetime.xml",
    ],
    user_configs=["configs/users.xml"],
)


def get_client():
    client = flight.FlightClient(f"grpc://{node.ip_address}:8888")
    token = client.authenticate_basic_token("user1", "qwe123")
    options = flight.FlightCallOptions(headers=[token])
    return client, options


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        node.wait_until_port_is_ready(8888, timeout=10)
        yield cluster
    finally:
        cluster.shutdown()


def test_ticket_expiration():
    node.query("CREATE TABLE mytable (id Int64, name String) ORDER BY id")
    node.query("INSERT INTO mytable VALUES (10, 'abc'), (20, 'cde')")

    client, options = get_client()

    descriptor = flight.FlightDescriptor.for_path("mytable")
    flight_info = client.get_flight_info(descriptor, options)
    assert len(flight_info.endpoints) == 1
    ticket = flight_info.endpoints[0].ticket
    expiration_time = flight_info.endpoints[0].expiration_time.as_py()

    ticket_lifetime_sec = (expiration_time - datetime.now(timezone.utc)).total_seconds()
    assert 0 < ticket_lifetime_sec and ticket_lifetime_sec < 2

    expected_schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.string(), nullable=False),
        ]
    )
    expected_ids = pa.chunked_array([pa.array([10, 20], type=pa.int64())])
    expected_names = pa.chunked_array([pa.array(["abc", "cde"], type=pa.string())])

    reader = client.do_get(ticket, options)
    actual = reader.read_all()
    assert actual.schema == expected_schema
    assert actual.column("id").equals(expected_ids)
    assert actual.column("name").equals(expected_names)

    # The same ticket can be used again.
    reader = client.do_get(ticket, options)
    actual = reader.read_all()
    assert actual.schema == expected_schema
    assert actual.column("id").equals(expected_ids)
    assert actual.column("name").equals(expected_names)

    time.sleep(3)

    # The ticket is expected to be expired now.
    try:
        reader = client.do_get(ticket, options)
        assert False, "Ticket is expected to expire, but still valid"
    except pa.lib.ArrowKeyError as e:
        expected_error = f"Ticket '{ticket.ticket.decode()}' not found"
        assert expected_error in str(e)

    node.query("DROP TABLE mytable SYNC")
