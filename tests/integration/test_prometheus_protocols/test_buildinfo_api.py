"""Tests for the Prometheus /api/v1/status/buildinfo endpoint."""

import pytest
import requests

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# The endpoint describes the server, so no TimeSeries table is created:
# the test also verifies that the endpoint works without one.
node = cluster.add_instance(
    "node",
    main_configs=["configs/prometheus.xml"],
)


def buildinfo_url():
    return f"http://{node.ip_address}:9093/api/v1/status/buildinfo"


@pytest.fixture(scope="module", autouse=True)
def setup():
    try:
        cluster.start()
        # cluster.start() waits for the native TCP port only; the Prometheus
        # protocols port can start accepting connections slightly later.
        cluster.wait_for_url(buildinfo_url())
        yield cluster
    finally:
        cluster.shutdown()


def test_buildinfo():
    response = requests.get(buildinfo_url())
    assert response.status_code == 200, response.text
    assert response.json() == {
        "status": "success",
        "data": {
            "version": node.query("SELECT version()").strip(),
            "revision": node.query(
                "SELECT value FROM system.build_options WHERE name = 'GIT_HASH'"
            ).strip(),
            "branch": "",
            "buildUser": "",
            "buildDate": "",
            "goVersion": "",
        },
    }
