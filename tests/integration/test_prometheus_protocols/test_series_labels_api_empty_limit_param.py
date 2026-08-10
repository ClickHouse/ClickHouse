"""Tests that the Prometheus HTTP API endpoints reject a present-but-empty `limit=` parameter with a
`bad_data` error, like Prometheus does, instead of treating it as an omitted parameter. Treating it as
absent would silently turn a malformed request into an unbounded response. An absent `limit` still
means "no limit", and a valid `limit` still truncates the result."""

import pytest
import requests

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
from .prometheus_test_utils import (
    convert_time_series_to_protobuf,
    send_protobuf_to_remote_write,
)


cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node_empty_limit_param",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
    handle_prometheus_remote_write=(9093, "/write"),
)


# The metadata endpoints and the query endpoints parse `limit` through the same handler helper, so
# cover one of each kind plus the remaining metadata endpoints for good measure.
ENDPOINTS = [
    ("/api/v1/series", {"match[]": "cpu_usage"}),
    ("/api/v1/labels", {}),
    ("/api/v1/label/host/values", {}),
    ("/api/v1/query", {"query": "cpu_usage", "time": "1000"}),
    ("/api/v1/query_range", {"query": "cpu_usage", "start": "0", "end": "1000", "step": "60"}),
]


def request_api(path, params=None):
    url = f"http://{node.ip_address}:9093{path}"
    print(f"Requesting {url} with params {params}")
    response = requests.get(url, params=params)
    print(f"Status code: {response.status_code}, Body: {response.text[:500]}")
    return response


@pytest.fixture(scope="module", autouse=True)
def setup(request):
    try:
        cluster.start()
        node.query("CREATE TABLE prometheus ENGINE=TimeSeries")
        time_series = [
            ({"__name__": "cpu_usage", "host": "server1"}, {1000000: 0.5}),
            ({"__name__": "cpu_usage", "host": "server2"}, {1000000: 0.7}),
        ]
        protobuf = convert_time_series_to_protobuf(time_series)
        send_protobuf_to_remote_write(node.ip_address, 9093, "/write", protobuf)
        assert_eq_with_retry(node, "SELECT count() > 0 FROM timeSeriesData(prometheus)", "1")
        yield cluster
    finally:
        cluster.shutdown()


@pytest.mark.parametrize("path,params", ENDPOINTS)
def test_empty_limit_param_is_rejected(path, params):
    """`limit=` (present but empty) must fail with 400 bad_data, not act as "no limit"."""
    response = request_api(path, params={**params, "limit": ""})
    assert response.status_code == 400, f"{path}: expected 400, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "error", f"{path}: unexpected body: {data}"
    assert data["errorType"] == "bad_data", f"{path}: unexpected body: {data}"
    assert "limit" in data["error"], f"{path}: unexpected error message: {data}"


@pytest.mark.parametrize("path,params", ENDPOINTS)
def test_absent_limit_param_still_means_no_limit(path, params):
    """Omitting `limit` entirely keeps working and returns the whole result."""
    response = request_api(path, params=params)
    assert response.status_code == 200, f"{path}: expected 200, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "success", f"{path}: unexpected body: {data}"


def test_valid_limit_still_truncates():
    """A valid `limit` keeps working alongside the empty-value rejection: two series are stored, so
    `limit=1` returns one of them and reports the truncation."""
    response = request_api("/api/v1/series", params={"match[]": "cpu_usage", "limit": "1"})
    assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "success", f"Unexpected body: {data}"
    assert len(data["data"]) == 1, f"Unexpected body: {data}"
    assert data.get("warnings"), f"Expected a truncation warning: {data}"
