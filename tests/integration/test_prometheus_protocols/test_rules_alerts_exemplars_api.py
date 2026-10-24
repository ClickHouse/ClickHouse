"""Tests for the Prometheus /api/v1/rules, /api/v1/alerts and /api/v1/query_exemplars endpoints."""

import time

import pytest
import requests

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# No TimeSeries table is created: the tests also verify that these endpoints work without one.
node = cluster.add_instance(
    "node",
    main_configs=["configs/prometheus.xml", "configs/http_port.xml"],
)

PROMETHEUS_PORT = 9093
HTTP_PORT = 8123

EXPECTED = {
    "rules": {"status": "success", "data": {"groups": []}},
    "alerts": {"status": "success", "data": {"alerts": []}},
    "query_exemplars": {"status": "success", "data": []},
}

# query_exemplars requires a PromQL expression, like in Prometheus.
PARAMS = {"rules": {}, "alerts": {}, "query_exemplars": {"query": "up"}}


def wait_for_prometheus_handlers(timeout=120):
    # cluster.start() waits for the native TCP port, and the Prometheus protocols port
    # can start accepting connections slightly later, so poll it before running the tests.
    deadline = time.monotonic() + timeout
    while True:
        try:
            requests.get(
                f"http://{node.ip_address}:{PROMETHEUS_PORT}/api/v1/rules", timeout=5
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


def check_response(response, endpoint):
    assert response.status_code == 200, response.text
    assert response.headers["Content-Type"] == "application/json"
    assert response.json() == EXPECTED[endpoint]


@pytest.mark.parametrize("endpoint", EXPECTED.keys())
def test_endpoint_without_table(endpoint):
    response = requests.get(
        f"http://{node.ip_address}:{PROMETHEUS_PORT}/api/v1/{endpoint}",
        params=PARAMS[endpoint],
    )
    check_response(response, endpoint)


@pytest.mark.parametrize("endpoint", EXPECTED.keys())
def test_endpoint_behind_prefix(endpoint):
    # The prometheus_api_v1 handler names a table which does not exist.
    response = requests.get(
        f"http://{node.ip_address}:{HTTP_PORT}/prometheus/api/v1/{endpoint}",
        params=PARAMS[endpoint],
    )
    check_response(response, endpoint)


def test_rules_ignores_filter_parameters():
    # Grafana Alerting sends these parameters, and they must not be treated as ClickHouse settings.
    params = {
        "type": "alert",
        "exclude_alerts": "true",
        "group_limit": "40",
        "file[]": "f",
        "rule_group[]": "g",
        "rule_name[]": "r",
        "match[]": '{job="x"}',
    }
    response = requests.get(
        f"http://{node.ip_address}:{PROMETHEUS_PORT}/api/v1/rules", params=params
    )
    check_response(response, "rules")


@pytest.mark.parametrize(
    "query",
    [
        "rate(up[5m])",
        "sum by (job) (rate(up[5m]))",
        "up[5m]",
        # Valid in Prometheus, but not supported by /api/v1/query yet.
        "sort(up)",
        "sort_desc(up)",
        "histogram_count(up)",
        "histogram_sum(up)",
        "histogram_fraction(0, 1, up)",
        "sort(up) + up",
        "sum(sort(rate(up[5m])))",
        "histogram_fraction(0, scalar(sort(up)), up)",
    ],
)
def test_query_exemplars_valid_query(query):
    response = requests.get(
        f"http://{node.ip_address}:{PROMETHEUS_PORT}/api/v1/query_exemplars",
        params={"query": query},
    )
    check_response(response, "query_exemplars")


@pytest.mark.parametrize("query", ["1 + 1", "vector(1)", "sort(vector(1))", '"abc"'])
def test_query_exemplars_without_selector(query):
    # Prometheus answers 204 No Content when the expression has no series selector.
    response = requests.get(
        f"http://{node.ip_address}:{PROMETHEUS_PORT}/api/v1/query_exemplars",
        params={"query": query},
    )
    assert response.status_code == 204, response.text
    assert response.content == b""


def test_query_exemplars_post():
    response = requests.post(
        f"http://{node.ip_address}:{PROMETHEUS_PORT}/api/v1/query_exemplars",
        data={"query": "up", "start": "1700000000", "end": "1700000600"},
    )
    check_response(response, "query_exemplars")


@pytest.mark.parametrize(
    "endpoint, params",
    [
        ("rules", {"type": "foo"}),
        ("rules", {"group_limit": "0"}),
        ("rules", {"group_limit": "abc"}),
        ("rules", {"group_next_token": "abc"}),
        ("rules", {"group_limit": "10", "group_next_token": "abc"}),
        ("rules", {"exclude_alerts": "wat"}),
        ("rules", {"match[]": "{"}),
        ("rules", {"match[]": "sum(up)"}),
        ("query_exemplars", {}),
        ("query_exemplars", {"query": "up{"}),
        ("query_exemplars", {"query": "rate(up)"}),
        ("query_exemplars", {"query": "abs()"}),
        ("query_exemplars", {"query": "sort()"}),
        ("query_exemplars", {"query": "sort(up, up)"}),
        ("query_exemplars", {"query": "sort(1)"}),
        ("query_exemplars", {"query": "sort(up[5m])"}),
        ("query_exemplars", {"query": "sort(rate(up))"}),
        ("query_exemplars", {"query": "sort(up) + rate(up)"}),
        ("query_exemplars", {"query": "histogram_count(1)"}),
        ("query_exemplars", {"query": "histogram_fraction(0, up)"}),
        ("query_exemplars", {"query": "histogram_fraction(up, 1, up)"}),
        ("query_exemplars", {"query": "histogram_fraction(0, scalar(rate(up)), up)"}),
        ("query_exemplars", {"query": "1 + abs()"}),
        ("query_exemplars", {"query": "up", "start": "abc"}),
        ("query_exemplars", {"query": "up", "start": "1700000600", "end": "1700000000"}),
    ],
)
def test_malformed_request(endpoint, params):
    response = requests.get(
        f"http://{node.ip_address}:{PROMETHEUS_PORT}/api/v1/{endpoint}", params=params
    )
    assert response.status_code == 400, response.text
    assert response.json()["errorType"] == "bad_data"
    # The request is rejected because it is invalid, not because ClickHouse can't evaluate it.
    assert "not implemented" not in response.json()["error"]
