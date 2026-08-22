"""Tests for the Prometheus /api/v1/labels endpoint."""

import pytest
import requests

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
from .prometheus_test_utils import convert_time_series_to_protobuf, send_protobuf_to_remote_write


cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/prometheus.xml"],
    user_configs=[
        "configs/allow_experimental_time_series_table.xml",
        "configs/prometheus_metadata_users.xml",
    ],
    handle_prometheus_remote_write=(9093, "/write"),
)


def send_test_data():
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
            {
                "__name__": "http_requests_total",
                "0http.status_code": "200",
                "host": "server1",
                "http.status_code": "200",
                "method": "GET",
                "status": "200",
            },
            {1000: 100, 1015: 150, 1030: 200},
        ),
        (
            {"__name__": "metric_only"},
            {1000: 1, 1015: 2, 1030: 3},
        ),
    ]
    send_protobuf_to_remote_write(node.ip_address, 9093, "/write", convert_time_series_to_protobuf(time_series))


def get_json_from_api(path, **kwargs):
    response = requests.get(f"http://{node.ip_address}:9093{path}", **kwargs)
    assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "success", f"Expected success, got: {data}"
    return data


def get_bad_data_from_api(path, **kwargs):
    response = requests.get(f"http://{node.ip_address}:9093{path}", **kwargs)
    assert response.status_code == 400, f"Expected 400, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "error", f"Expected error, got: {data}"
    assert data["errorType"] == "bad_data", f"Expected bad_data, got: {data}"
    return data


@pytest.fixture(scope="module", autouse=True)
def setup():
    try:
        cluster.start()
        node.query("CREATE TABLE prometheus ENGINE=TimeSeries SETTINGS tags_to_columns = {'host': 'label_key'}")
        send_test_data()
        assert_eq_with_retry(node, "SELECT count() > 0 FROM timeSeriesData(prometheus)", "1")
        yield cluster
    finally:
        cluster.shutdown()


def test_labels_returns_distinct_names():
    data = get_json_from_api("/api/v1/labels")["data"]
    assert data == ["0http.status_code", "__name__", "datacenter", "host", "http.status_code", "method", "status"]


def test_labels_match_filter():
    data = get_json_from_api('/api/v1/labels?match[]={host="server1"}')["data"]
    assert data == ["0http.status_code", "__name__", "datacenter", "host", "http.status_code", "method", "status"]


def test_labels_repeated_match_is_union():
    data = get_json_from_api(
        "/api/v1/labels",
        params=[("match[]", "cpu_usage"), ("match[]", "memory_usage")],
    )["data"]
    assert data == ["__name__", "datacenter", "host"]


def test_labels_post_urlencoded_repeated_match_is_union_with_limit():
    response = requests.post(
        f"http://{node.ip_address}:9093/api/v1/labels",
        data=[("match[]", "cpu_usage"), ("match[]", "memory_usage"), ("limit", "2")],
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
    assert response.json() == {
        "status": "success",
        "data": ["__name__", "datacenter"],
        "warnings": ["results truncated due to limit"],
    }


def test_labels_with_no_matching_series_is_empty():
    assert get_json_from_api("/api/v1/labels?match[]=missing_metric")["data"] == []


def test_labels_metric_only_series_returns_virtual_name():
    assert get_json_from_api("/api/v1/labels?match[]=metric_only")["data"] == ["__name__"]


def test_labels_limit_reports_truncation():
    data = get_json_from_api("/api/v1/labels?limit=1")
    assert data["data"] == ["0http.status_code"]
    assert data["warnings"] == ["results truncated due to limit"]


def test_labels_limit_exactly_matching_result_is_not_truncated():
    data = get_json_from_api("/api/v1/labels?limit=7")
    assert data["data"] == ["0http.status_code", "__name__", "datacenter", "host", "http.status_code", "method", "status"]
    assert "warnings" not in data


def test_labels_zero_limit_is_unlimited():
    data = get_json_from_api("/api/v1/labels?limit=0")
    assert data["data"] == ["0http.status_code", "__name__", "datacenter", "host", "http.status_code", "method", "status"]
    assert "warnings" not in data


def test_labels_time_range_filters_series():
    assert get_json_from_api("/api/v1/labels?start=2000&end=3000")["data"] == []


@pytest.mark.parametrize("bound_name", ["start", "end"])
@pytest.mark.parametrize("bound_value", ["5m", "0x10"])
def test_labels_rejects_promql_timestamp_syntax_in_http_bounds(bound_name, bound_value):
    get_bad_data_from_api(
        "/api/v1/labels",
        params={bound_name: bound_value},
    )


@pytest.mark.parametrize("bound_name", ["start", "end"])
@pytest.mark.parametrize("bound_value", ["1_000", "0x1p4"])
def test_labels_accepts_go_float_syntax_in_http_bounds(bound_name, bound_value):
    get_json_from_api(
        "/api/v1/labels",
        params={bound_name: bound_value},
    )


@pytest.mark.parametrize("bound_value", ["1970-01-01", "1970-01-01 00:16:40"])
def test_labels_rejects_non_rfc3339_datetime_bounds(bound_value):
    get_bad_data_from_api(
        "/api/v1/labels",
        params={"start": bound_value},
    )


def test_labels_time_range_accepts_decimal_fraction_bounds():
    data = get_json_from_api(
        "/api/v1/labels",
        params={"start": "1000.125", "end": "1000.125"},
    )["data"]
    assert data


def test_labels_time_range_accepts_rfc3339_bounds():
    data = get_json_from_api(
        "/api/v1/labels",
        params={
            "start": "1970-01-01T00:16:40Z",
            "end": "1970-01-01T00:16:40Z",
        },
    )["data"]
    assert data


def test_labels_time_range_accepts_rfc3339_fraction_and_offset():
    data = get_json_from_api(
        "/api/v1/labels",
        params={
            "start": "1970-01-01T01:16:40.123456789+01:00",
            "end": "1970-01-01T01:16:40.123456789+01:00",
        },
    )["data"]
    assert data == []


def test_labels_numeric_time_range_rounds_to_milliseconds():
    data = get_json_from_api(
        "/api/v1/labels",
        params={"end": "999.9996"},
    )["data"]
    assert data


def test_labels_empty_optional_parameters_are_ignored():
    data = get_json_from_api(
        "/api/v1/labels",
        params={"start": "", "end": "", "limit": ""},
    )["data"]
    assert data == ["0http.status_code", "__name__", "datacenter", "host", "http.status_code", "method", "status"]


def test_labels_works_with_query_cache():
    params = {"use_query_cache": "1"}
    first = get_json_from_api("/api/v1/labels", params=params)
    second = get_json_from_api("/api/v1/labels", params=params)
    assert second == first


def test_labels_requires_select_on_configured_time_series_table():
    response = requests.get(
        f"http://{node.ip_address}:9093/api/v1/labels",
        auth=("metadata_temp_table_only", ""),
    )
    assert response.status_code == 400, response.text
    data = response.json()
    assert data["status"] == "error"
    assert data["errorType"] == "bad_data"
    assert "SELECT" in data["error"]
    assert "default.prometheus" in data["error"]


def test_labels_allows_select_on_configured_time_series_table():
    data = get_json_from_api(
        "/api/v1/labels",
        auth=("metadata_select_time_series", ""),
    )["data"]
    assert data == ["0http.status_code", "__name__", "datacenter", "host", "http.status_code", "method", "status"]


def test_labels_records_query_finish():
    query_id = "prometheus_labels_query_log_test"
    get_json_from_api("/api/v1/labels", headers={"X-ClickHouse-Query-Id": query_id})
    node.query("SYSTEM FLUSH LOGS query_log")
    assert_eq_with_retry(
        node,
        f"SELECT count() FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'",
        "1",
    )
