"""Tests for the Prometheus metadata endpoints when tags are moved into dedicated columns via the
`tags_to_columns` setting, and for explicitly rejecting the `start`/`end` time range when the table
does not store the time bounds (`store_min_time_and_max_time = 0`)."""

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
    "node_tags_to_columns",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
    handle_prometheus_remote_write=(9093, "/write"),
)


def send_test_data():
    """Write series whose tags are split between dedicated columns (host, datacenter) and the
    residual tags map (method, status)."""
    time_series = [
        (
            {"__name__": "cpu_usage", "host": "server1", "datacenter": "us-east"},
            {1000: 0.5},
        ),
        (
            {"__name__": "cpu_usage", "host": "server2", "datacenter": "us-west"},
            {1000: 0.3},
        ),
        (
            {"__name__": "http_requests_total", "host": "server1", "method": "GET", "status": "200"},
            {1000: 100},
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
        # `host` and `datacenter` are moved into dedicated columns of the tags table; `method` and
        # `status` stay in the residual `tags` map. `store_min_time_and_max_time` is disabled so the
        # tags table has no `min_time`/`max_time` columns, which is needed by the rejection test.
        node.query(
            "CREATE TABLE prometheus ENGINE=TimeSeries "
            "SETTINGS tags_to_columns = {'host': 'host', 'datacenter': 'datacenter'}, "
            "store_min_time_and_max_time = 0"
        )
        send_test_data()
        assert_eq_with_retry(
            node, "SELECT count() > 0 FROM timeSeriesData(prometheus)", "1"
        )
        yield cluster
    finally:
        cluster.shutdown()


def test_tags_to_columns_labels_included():
    """Labels moved into dedicated columns must still be reported by /api/v1/labels, together with
    the labels that remain in the `tags` map."""
    data = get_json_from_api("/api/v1/labels")
    assert "__name__" in data
    # host and datacenter live in dedicated columns (not in the tags map):
    assert "host" in data
    assert "datacenter" in data
    # method and status remain in the tags map:
    assert "method" in data
    assert "status" in data


def test_tags_to_columns_label_values_from_column():
    """GET /api/v1/label/<name>/values for a tag moved into a dedicated column reads that column."""
    data = get_json_from_api("/api/v1/label/host/values")
    assert "server1" in data
    assert "server2" in data


def test_tags_to_columns_label_values_from_map():
    """GET /api/v1/label/<name>/values for a tag that stays in the map keeps working."""
    data = get_json_from_api("/api/v1/label/method/values")
    assert "GET" in data


def test_tags_to_columns_series_includes_column_tags():
    """GET /api/v1/series must include tags stored in dedicated columns in each series' label set."""
    data = get_json_from_api("/api/v1/series?match[]=cpu_usage")
    assert len(data) == 2, f"Expected 2 cpu_usage series, got: {data}"
    for entry in data:
        assert "host" in entry
        assert "datacenter" in entry
    hosts = {entry["host"] for entry in data}
    assert hosts == {"server1", "server2"}


def test_start_end_rejected_without_min_max_time():
    """When the table does not store min_time/max_time, a request specifying start/end must fail
    explicitly instead of silently ignoring the time range and returning incomplete data."""
    url = f"http://{node.ip_address}:9093/api/v1/series?start=1000&end=1030"
    response = requests.get(url)
    assert response.status_code == 400, f"Expected 400, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "error", f"Expected error status, got: {data}"
    assert (
        "min_time" in data["error"] or "store_min_time_and_max_time" in data["error"]
    ), f"Unexpected error message: {data}"
