"""Test Interserver responses on configured IP."""

import socket
import time
from pathlib import Path

import pytest
import requests

from helpers.cluster import ClickHouseCluster

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
        cluster.wait_for_url(
            f"http://{INTERSERVER_LISTEN_HOST}:{INTERSERVER_HTTP_PORT}"
        )
        cluster.wait_for_url(
            f"http://{node_without_interserver_listen_host.ip_address}:8123"
        )
        yield cluster

    finally:
        cluster.shutdown()


def requests_get(url, attempts=10, sleep=0.5):
    attempt = 0
    while True:
        attempt += 1
        try:
            return requests.get(url)
        except requests.exceptions.ConnectionError as e:
            if attempt >= attempts:
                raise
        time.sleep(sleep)


def test_request_to_node_with_interserver_listen_host(start_cluster):
    response_interserver = requests_get(
        f"http://{INTERSERVER_LISTEN_HOST}:{INTERSERVER_HTTP_PORT}"
    )
    response_client = requests_get(
        f"http://{node_without_interserver_listen_host.ip_address}:8123"
    )
    assert response_interserver.status_code == 200
    assert "Ok." in response_interserver.text
    assert response_client.status_code == 200


def test_request_to_node_without_interserver_listen_host(start_cluster):
    response = requests_get(
        f"http://{node_without_interserver_listen_host.ip_address}:{INTERSERVER_HTTP_PORT}"
    )
    assert response.status_code == 200
