"""Tests for Prometheus HTTP API endpoints: /api/v1/series, /api/v1/labels, /api/v1/label/<name>/values"""

import json
import pytest
import time

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
from .prometheus_test_utils import *


cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
    handle_prometheus_remote_write=(9093, "/write"),
)


def send_test_data():
    """Send test data with known labels for testing series/labels/label-values endpoints."""
    time_series = [
        (
            {"__name__": "cpu_usage", "host": "server1", "datacenter": "us-east"},
            {1000: 0.5, 1015: 0.6, 1030: 0.7},
        ),
        (
            {"__name__": "cpu_usage", "host": "server2", "datacenter": "us-west"},
            {1000: 0.3, 1015: 0.4, 1030: 0.5},
        ),
        (
            {"__name__": "memory_usage", "host": "server1", "datacenter": "us-east"},
            {1000: 0.8, 1015: 0.85, 1030: 0.9},
        ),
        (
            {"__name__": "http_requests_total", "host": "server1", "method": "GET", "status": "200"},
            {1000: 100, 1015: 150, 1030: 200},
        ),
    ]
    protobuf = convert_time_series_to_protobuf(time_series)
    send_protobuf_to_remote_write(node.ip_address, 9093, "/write", protobuf)


def get_json_from_api(path):
    """Make a GET request to the ClickHouse Prometheus API and return parsed JSON."""
    url = f"http://{node.ip_address}:9093{path}"
    print(f"Requesting {url}")
    response = requests.get(url)
    print(f"Status code: {response.status_code}, Body: {response.text[:500]}")
    assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "success", f"Expected success, got: {data}"
    return data["data"]


@pytest.fixture(scope="module", autouse=True)
def setup(request):
    try:
        cluster.start()
        node.query("CREATE TABLE prometheus ENGINE=TimeSeries")
        send_test_data()
        # Wait for data to be available
        assert_eq_with_retry(
            node, "SELECT count() > 0 FROM timeSeriesData(prometheus)", "1"
        )
        yield cluster
    finally:
        cluster.shutdown()


def test_labels_returns_all_label_names():
    """GET /api/v1/labels should return all unique label names including __name__."""
    data = get_json_from_api("/api/v1/labels")
    assert isinstance(data, list)
    assert "__name__" in data
    assert "host" in data
    assert "datacenter" in data


def test_label_values_for_name():
    """GET /api/v1/label/__name__/values should return all metric names."""
    data = get_json_from_api("/api/v1/label/__name__/values")
    assert isinstance(data, list)
    assert "cpu_usage" in data
    assert "memory_usage" in data
    assert "http_requests_total" in data


def test_label_values_for_host():
    """GET /api/v1/label/host/values should return all host values."""
    data = get_json_from_api("/api/v1/label/host/values")
    assert isinstance(data, list)
    assert "server1" in data
    assert "server2" in data


def test_label_values_for_datacenter():
    """GET /api/v1/label/datacenter/values should return datacenter values."""
    data = get_json_from_api("/api/v1/label/datacenter/values")
    assert isinstance(data, list)
    assert "us-east" in data
    assert "us-west" in data


def test_series_returns_metric_labels():
    """GET /api/v1/series should return series with their full label sets."""
    data = get_json_from_api("/api/v1/series")
    assert isinstance(data, list)
    assert len(data) > 0

    # Each entry should be a dict with __name__ and other labels
    metric_names = {entry["__name__"] for entry in data if "__name__" in entry}
    assert "cpu_usage" in metric_names
    assert "memory_usage" in metric_names


def test_label_values_for_nonexistent_label():
    """GET /api/v1/label/nonexistent/values should return empty list."""
    data = get_json_from_api("/api/v1/label/nonexistent/values")
    assert isinstance(data, list)
    assert len(data) == 0


def test_series_includes_real_labels():
    """GET /api/v1/series must return the full label set, not only __name__.

    The tags column is a Map, so each returned series should contain the real labels
    (e.g. host, datacenter) that were written, deduplicated by series identity.
    """
    data = get_json_from_api("/api/v1/series")
    cpu_series = [entry for entry in data if entry.get("__name__") == "cpu_usage"]
    assert len(cpu_series) == 2, f"Expected 2 cpu_usage series, got: {cpu_series}"

    # Each series must carry its full label set, not just __name__.
    for entry in cpu_series:
        assert "host" in entry
        assert "datacenter" in entry

    hosts = {entry["host"] for entry in cpu_series}
    assert hosts == {"server1", "server2"}


def test_series_match_filter():
    """GET /api/v1/series?match[]=<metric> should filter by metric name.

    Exercises that match[] is treated as an endpoint parameter (not a ClickHouse setting).
    """
    data = get_json_from_api("/api/v1/series?match[]=cpu_usage")
    assert len(data) > 0
    metric_names = {entry["__name__"] for entry in data if "__name__" in entry}
    assert metric_names == {"cpu_usage"}


def test_label_values_with_match_filter():
    """GET /api/v1/label/<name>/values?match[]=<metric> should route correctly with a query string.

    The query string must not break path routing (the endpoint used to fall through to 404).
    """
    data = get_json_from_api("/api/v1/label/host/values?match[]=memory_usage")
    assert isinstance(data, list)
    assert "server1" in data
    assert "server2" not in data


def test_series_deduplicated():
    """Re-writing the same series must not produce duplicate entries in /api/v1/series.

    The tags target is AggregatingMergeTree and stores a row per write, so the same
    series appears as multiple rows until parts are merged; /api/v1/series must
    deduplicate by series identity. Merges are stopped so the duplicate rows persist.
    """
    series = [
        (
            {"__name__": "dedup_metric", "host": "serverX"},
            {1000: 1.0},
        ),
    ]
    protobuf = convert_time_series_to_protobuf(series)

    node.query("SYSTEM STOP MERGES")
    try:
        send_protobuf_to_remote_write(node.ip_address, 9093, "/write", protobuf)
        send_protobuf_to_remote_write(node.ip_address, 9093, "/write", protobuf)
        # Wait until both writes have produced separate (un-merged) rows in the tags table.
        assert_eq_with_retry(
            node,
            "SELECT count() FROM timeSeriesTags(prometheus) WHERE metric_name = 'dedup_metric'",
            "2",
        )
        data = get_json_from_api("/api/v1/series?match[]=dedup_metric")
        assert len(data) == 1, f"Expected exactly 1 deduplicated series, got: {data}"
        assert data[0]["host"] == "serverX"
    finally:
        node.query("SYSTEM START MERGES")
