import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance("instance")


@pytest.fixture(scope="module", autouse=True)
def setup_nodes():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_http_native_returns_timezone():
    # No timezone when no protocol version sent
    query = "SELECT toDateTime(1676369730, 'Asia/Shanghai') as dt FORMAT Native"
    raw = instance.http_query(query, content=True)
    assert raw.hex(" ", 2) == "0101 0264 7408 4461 7465 5469 6d65 425f eb63"

    # Timezone available when protocol version sent
    raw = instance.http_query(
        query, params={"client_protocol_version": 54337}, content=True
    )
    ch_type = raw[14:39].decode()
    assert ch_type == "DateTime('Asia/Shanghai')"
