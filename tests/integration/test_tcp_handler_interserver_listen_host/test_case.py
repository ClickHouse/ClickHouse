"""Test Interserver responses on configured IP."""
from pathlib import Path
import pytest
from helpers.cluster import ClickHouseCluster
import requests
import socket
import time

cluster = ClickHouseCluster(__file__)

INTERSERVER_LISTEN_HOST = "10.0.0.10"
INTERSERVER_HTTP_PORT = 9009

node_with_interserver_listen_host = cluster.add_instance(
    "node_with_interserver_listen_host",
    main_configs=["configs/config.d/interserver-listen-host.xml"],
    ipv4_address=INTERSERVER_LISTEN_HOST,  # used to configure acc. interface in test container
    ipv6_address="2001:3984:3989::1:1000",
)

node_without_interserver_listen_host = cluster.add_instance(
    "node_without_interserver_listen_host",
    main_configs=["configs/config.d/no-interserver-listen-host.xml"],
    ipv6_address="2001:3984:3989::2:1000",
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_request_to_node_with_interserver_listen_host(start_cluster):
    time.sleep(5)  # waiting for interserver listener to start
    response_interserver = requests.get(
        f"http://{INTERSERVER_LISTEN_HOST}:{INTERSERVER_HTTP_PORT}"
    )
    response_client = requests.get(
        f"http://{node_without_interserver_listen_host.ip_address}:8123"
    )
    assert response_interserver.status_code == 200
    assert "Ok." in response_interserver.text
    assert response_client.status_code == 200


def test_request_to_node_without_interserver_listen_host(start_cluster):
    response = requests.get(
        f"http://{node_without_interserver_listen_host.ip_address}:{INTERSERVER_HTTP_PORT}"
    )
    assert response.status_code == 200
