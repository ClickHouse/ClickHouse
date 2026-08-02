import pyarrow.flight
import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/config.xml"],
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        node.wait_until_port_is_ready(8888, timeout=10)
        yield cluster
    finally:
        cluster.shutdown()


def test_arrowflight_sessions_in_session_log():
    """An Arrow Flight login must produce a readable `system.session_log` row:
    the `interface` column is an `Enum8`, and a value missing from the
    enumeration would make any `SELECT` touching the row throw."""
    client = pyarrow.flight.FlightClient(f"grpc+tcp://{node.ip_address}:8888")
    try:
        ticket = pyarrow.flight.Ticket(b"SELECT 1")
        table = client.do_get(ticket).read_all()
        assert table.column(0)[0].as_py() == 1
    finally:
        client.close()

    node.query("SYSTEM FLUSH LOGS session_log")
    interfaces = node.query("SELECT DISTINCT toString(interface) FROM system.session_log").splitlines()
    assert "ArrowFlight" in interfaces
