"""Test HTTP responses given by the TCP Handler."""
from pathlib import Path
import pytest
from helpers.cluster import ClickHouseCluster
import requests

cluster = ClickHouseCluster(__file__)

node_with_http = cluster.add_instance(
    "node_with_http", main_configs=["configs/config.d/http-port-31337.xml"]
)
HTTP_PORT = 31337

node_without_http = cluster.add_instance(
    "node_without_http", main_configs=["configs/config.d/no-http-port.xml"]
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_request_to_http_full_instance(start_cluster):
    response = requests.get(f"http://{node_with_http.ip_address}:9000")
    assert response.status_code == 400
    assert str(HTTP_PORT) in response.text


def test_request_to_http_less_instance(start_cluster):
    response = requests.post(f"http://{node_without_http.ip_address}:9000")
    assert response.status_code == 400
    assert str(HTTP_PORT) not in response.text
    assert "8123" not in response.text
