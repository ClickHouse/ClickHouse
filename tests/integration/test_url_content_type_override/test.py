import os

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.mock_servers import start_mock_servers

SERVER_HOSTNAME = "localhost"
SERVER_PORT = 5001

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance("instance")


def start_server():
    script_dir = os.path.join(os.path.dirname(__file__), "mock_server")
    start_mock_servers(
        cluster,
        script_dir,
        [
            (
                "simple_server.py",
                SERVER_HOSTNAME,
                SERVER_PORT,
            )
        ],
    )


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        start_server()
        yield
    finally:
        cluster.shutdown()


def test_url_content_type_override():
    assert (
        "200"
        == node.query(
            f"INSERT INTO FUNCTION url('http://{SERVER_HOSTNAME}:{SERVER_PORT}/', JSONEachRow, 'x UInt8', headers('X-Test-Answer' = 'application/x-ndjson; charset=UTF-8')) SELECT 1)"
        ).strip()
    )

    assert (
        "200"
        == node.query(
            f"INSERT INTO FUNCTION url('http://{SERVER_HOSTNAME}:{SERVER_PORT}/', JSONEachRow, 'x UInt8', headers('Content-Type' = 'upyachka', 'X-Test-Answer' = 'upyachka')) SELECT 1)"
        ).strip()
    )
