"""Tests for the Prometheus metadata endpoints when `filter_by_min_time_and_max_time = 0`.

`/api/v1/query` and `/api/v1/query_range` only use the tags-table `min_time`/`max_time` prefilter when
`filter_by_min_time_and_max_time` is enabled; otherwise they scope the result by exact filtering from the
samples table (see `StorageTimeSeriesSelector::readImpl`). The metadata endpoints query only the tags table
and have no exact samples-table fallback, so with the setting disabled they must not silently apply the
`min_time`/`max_time` overlap filter (which would diverge from the real query path). A metadata request that
specifies `start`/`end` is therefore rejected, while requests without a time range keep working."""

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
    "node_filter_disabled",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
    handle_prometheus_remote_write=(9093, "/write"),
)


def send_test_data():
    time_series = [
        (
            {"__name__": "cpu_usage", "host": "server1"},
            {1000: 0.5, 1015: 0.6, 1030: 0.7},
        ),
        (
            {"__name__": "memory_usage", "host": "server2"},
            {1000: 0.8, 1015: 0.85, 1030: 0.9},
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
        # `store_min_time_and_max_time` stays enabled (default), so the tags table still has the
        # `min_time`/`max_time` columns, but `filter_by_min_time_and_max_time` is disabled, so those
        # columns must not be used for filtering.
        node.query(
            "CREATE TABLE prometheus ENGINE=TimeSeries "
            "SETTINGS filter_by_min_time_and_max_time = 0"
        )
        send_test_data()
        assert_eq_with_retry(
            node, "SELECT count() > 0 FROM timeSeriesData(prometheus)", "1"
        )
        yield cluster
    finally:
        cluster.shutdown()


def test_metadata_endpoints_work_without_time_range():
    """Without `start`/`end`, the metadata endpoints work regardless of the disabled setting."""
    labels = get_json_from_api("/api/v1/labels")
    assert "__name__" in labels
    assert "host" in labels

    names = get_json_from_api("/api/v1/label/__name__/values")
    assert "cpu_usage" in names
    assert "memory_usage" in names

    series = get_json_from_api("/api/v1/series?match[]=cpu_usage")
    metric_names = {entry["__name__"] for entry in series if "__name__" in entry}
    assert metric_names == {"cpu_usage"}


@pytest.mark.parametrize(
    "path",
    [
        "/api/v1/series",
        "/api/v1/labels",
        "/api/v1/label/host/values",
    ],
)
def test_start_end_rejected_when_filtering_disabled(path):
    """With `filter_by_min_time_and_max_time = 0`, a metadata request specifying `start`/`end` must be
    rejected explicitly instead of silently applying a `min_time`/`max_time` filter that the real query
    path (`/api/v1/query`, `/api/v1/query_range`) would not use, which could return a different set of
    series, label names, or label values."""
    url = f"http://{node.ip_address}:9093{path}?start=1000&end=1030"
    response = requests.get(url)
    assert response.status_code == 400, f"Expected 400, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "error", f"Expected error status, got: {data}"
    assert (
        "filter_by_min_time_and_max_time" in data["error"]
    ), f"Unexpected error message: {data}"
