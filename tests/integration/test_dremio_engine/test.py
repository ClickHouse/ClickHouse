import pytest
from helpers.client import QueryRuntimeException

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import dremio_user, dremio_pass

cluster = ClickHouseCluster(__file__)
clickhouse_node = cluster.add_instance(
    "node1",
    with_dremio26=True,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_dremio_descriptor_type(started_cluster):

    with pytest.raises(QueryRuntimeException) as exc:
        clickhouse_node.query(
            f"SELECT * FROM arrowFlight('{cluster.dremio26_host}:{cluster.dremio26_port}', 'sys.memory', '{dremio_user}', '{dremio_pass}');"
        )
    assert "ARROWFLIGHT_FETCH_SCHEMA_ERROR" in str(exc.value)

    with pytest.raises(QueryRuntimeException) as exc:
        clickhouse_node.query(
            f"SELECT * FROM arrowFlight('{cluster.dremio26_host}:{cluster.dremio26_port}', 'sys.memory', '{dremio_user}', '{dremio_pass}') SETTINGS arrow_flight_request_descriptor_type = 'path';"
        )
    assert "ARROWFLIGHT_FETCH_SCHEMA_ERROR" in str(exc.value)

    clickhouse_node.query(f"SELECT * FROM arrowFlight('{cluster.dremio26_host}:{cluster.dremio26_port}', 'sys.memory', '{dremio_user}', '{dremio_pass}') SETTINGS arrow_flight_request_descriptor_type = 'command';")
