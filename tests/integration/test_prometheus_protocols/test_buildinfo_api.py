"""Tests for the Prometheus /api/v1/status/buildinfo endpoint."""

import time

import pytest
import requests

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/prometheus.xml"],
)


def wait_for_prometheus_handlers(timeout=120):
    # cluster.start() waits for the native TCP port, and the Prometheus protocols port
    # can start accepting connections slightly later, so poll it before running the tests.
    deadline = time.monotonic() + timeout
    while True:
        try:
            requests.get(
                f"http://{node.ip_address}:9093/api/v1/status/buildinfo",
                timeout=5,
            )
            return
        except requests.exceptions.ConnectionError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.5)


@pytest.fixture(scope="module", autouse=True)
def setup():
    try:
        cluster.start()
        wait_for_prometheus_handlers()
        yield cluster
    finally:
        cluster.shutdown()


def test_buildinfo_returns_clickhouse_identity_without_table():
    response = requests.get(f"http://{node.ip_address}:9093/api/v1/status/buildinfo")
    assert response.status_code == 200, response.text

    data = response.json()
    assert data["status"] == "success"
    assert data["data"] == {
        "version": node.query("SELECT version()").strip(),
        "revision": node.query(
            "SELECT value FROM system.build_options WHERE name = 'GIT_HASH'"
        ).strip(),
        "branch": "",
        "buildUser": "",
        "buildDate": "",
        "goVersion": "",
    }
    assert "features" not in data["data"]


def test_buildinfo_supports_head_request():
    response = requests.head(f"http://{node.ip_address}:9093/api/v1/status/buildinfo")
    assert response.status_code == 200, response.text
    assert response.content == b""


def test_buildinfo_rejects_unsupported_methods():
    response = requests.post(f"http://{node.ip_address}:9093/api/v1/status/buildinfo")
    assert response.status_code == 405, response.text
    assert response.headers["Allow"] == "GET, HEAD"
