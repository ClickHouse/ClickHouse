"""Tests for the Prometheus /api/v1/status/buildinfo endpoint."""

import pytest
import requests

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

PROMETHEUS_PORT = 9093

# The Prometheus API level reported as `version`, see PROMETHEUS_API_COMPATIBILITY_VERSION
# in src/Server/PrometheusRequestHandler.cpp. Clients gate features on this field, so it
# describes the implemented API and must not drift into being the ClickHouse version.
EXPECTED_API_VERSION = "2.24.0"

# The endpoint describes the server, so no TimeSeries table is created:
# the test also verifies that the endpoint works without one.
node = cluster.add_instance(
    "node",
    main_configs=["configs/prometheus.xml"],
)


def buildinfo_url(prefix=""):
    return f"http://{node.ip_address}:{PROMETHEUS_PORT}{prefix}/api/v1/status/buildinfo"


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
            "version": EXPECTED_API_VERSION,
            "revision": node.query(
                "SELECT value FROM system.build_options WHERE name = 'GIT_HASH'"
            ).strip(),
            "branch": "",
            "buildUser": "",
            "buildDate": "",
            "goVersion": "",
            "clickhouseVersion": node.query("SELECT version()").strip(),
        },
    }


def test_buildinfo_version_is_the_prometheus_api_level_not_the_clickhouse_version():
    # Grafana compares `version` against 2.24.0 to decide whether the label endpoints accept
    # `match[]`; a ClickHouse version string would lose that comparison and send it back to the
    # much more expensive /api/v1/series. Pin the two apart so they cannot be conflated again.
    data = requests.get(buildinfo_url()).json()["data"]
    clickhouse_version = node.query("SELECT version()").strip()

    assert data["version"] == EXPECTED_API_VERSION
    assert data["version"] != clickhouse_version
    # The backend is still identifiable from the response itself.
    assert data["clickhouseVersion"] == clickhouse_version


def test_buildinfo_works_on_a_prefix_routed_handler_without_a_table():
    # The handler for /no_table configures no table at all, which is how the documented
    # prefix-routed setup looks before a request supplies one.
    response = requests.get(buildinfo_url("/no_table"))
    assert response.status_code == 200, response.text
    assert response.json()["data"]["version"] == EXPECTED_API_VERSION
