"""Tests for the Prometheus /api/v1/labels endpoint."""

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
    "node",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
    handle_prometheus_remote_write=(9093, "/write"),
)

ALL_LABELS = ["__name__", "datacenter", "host", "region", "service", "status"]


def send_test_data():
    time_series = [
        (
            {"__name__": "cpu_usage", "host": "server1", "datacenter": "us-east"},
            {1000: 0.5, 1030: 0.7},
        ),
        (
            {"__name__": "cpu_usage", "host": "server2", "datacenter": "us-west"},
            {1000: 0.3, 1030: 0.5},
        ),
        (
            {"__name__": "memory_usage", "host": "server1", "region": "eu"},
            {1000: 0.8},
        ),
        (
            {"__name__": "http_requests", "service": "web", "status": "200"},
            {1000: 1.0},
        ),
    ]
    send_protobuf_to_remote_write(
        node.ip_address, 9093, "/write", convert_time_series_to_protobuf(time_series)
    )


def get_json_from_api(path, **kwargs):
    response = requests.get(f"http://{node.ip_address}:9093{path}", **kwargs)
    assert (
        response.status_code == 200
    ), f"Expected 200, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "success", f"Expected success, got: {data}"
    return data


def get_bad_data_from_api(path, **kwargs):
    response = requests.get(f"http://{node.ip_address}:9093{path}", **kwargs)
    assert (
        response.status_code == 400
    ), f"Expected 400, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "error", f"Expected error, got: {data}"
    assert data["errorType"] == "bad_data", f"Expected bad_data, got: {data}"
    return data


@pytest.fixture(scope="module", autouse=True)
def setup():
    try:
        cluster.start()
        # The `host` tag is stored in a dedicated column: the endpoint must still report it
        # under its tag name.
        node.query(
            "CREATE TABLE prometheus ENGINE=TimeSeries "
            "SETTINGS tags_to_columns = {'host': 'host_column'}"
        )
        node.query(
            "CREATE TABLE prometheus_no_bounds ENGINE=TimeSeries "
            "SETTINGS store_min_time_and_max_time = 0"
        )
        node.query(
            "INSERT INTO prometheus_no_bounds (metric_name, tags, time_series) VALUES "
            "('cpu_usage', {'host': 'server1'}, [(toDateTime64(1000, 3), 0.5)])"
        )
        send_test_data()
        assert_eq_with_retry(node, "SELECT count() > 0 FROM timeSeriesData(prometheus)", "1")
        yield cluster
    finally:
        cluster.shutdown()


def test_labels_returns_all_label_names_sorted():
    # No `match[]` selectors: the label names of all the series, in sorted order.
    # `host` is stored in a dedicated column (`tags_to_columns`) but is reported under its tag name.
    assert get_json_from_api("/api/v1/labels")["data"] == ALL_LABELS


def test_labels_match_selector_filters():
    data = get_json_from_api("/api/v1/labels?match[]=cpu_usage")["data"]
    assert data == ["__name__", "datacenter", "host"]


def test_labels_label_matcher():
    data = get_json_from_api("/api/v1/labels", params={"match[]": '{host="server1"}'})["data"]
    assert data == ["__name__", "datacenter", "host", "region"]


def test_labels_repeated_match_is_union():
    data = get_json_from_api(
        "/api/v1/labels",
        params=[("match[]", "memory_usage"), ("match[]", "http_requests")],
    )["data"]
    assert data == ["__name__", "host", "region", "service", "status"]


def test_labels_no_matching_series_returns_empty():
    assert get_json_from_api("/api/v1/labels?match[]=missing_metric")["data"] == []


def test_labels_post_urlencoded_match():
    response = requests.post(
        f"http://{node.ip_address}:9093/api/v1/labels",
        data=[("match[]", "memory_usage"), ("match[]", "http_requests")],
    )
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["status"] == "success"
    assert data["data"] == ["__name__", "host", "region", "service", "status"]


@pytest.mark.parametrize(
    "selector",
    [
        "",  # explicitly empty value
        "{}",  # selector without matchers
        "cpu_usage[5m]",  # range selector, not an instant selector
        "rate(cpu_usage[5m])",  # PromQL expression, not a selector
        'cpu_usage{host="server1"',  # unparsable
    ],
)
def test_labels_rejects_invalid_selectors(selector):
    get_bad_data_from_api("/api/v1/labels", params={"match[]": selector})


def test_labels_time_range():
    # All test samples are within [1000, 1030].
    assert get_json_from_api("/api/v1/labels?start=990&end=1040")["data"] == ALL_LABELS
    assert get_json_from_api("/api/v1/labels?start=2000&end=3000")["data"] == []
    assert get_json_from_api("/api/v1/labels?start=0&end=500")["data"] == []


def test_labels_time_range_is_inclusive_and_supports_one_sided_bounds():
    # Only the cpu_usage series extend to 1030; the other series end at 1000.
    assert get_json_from_api("/api/v1/labels?start=1030")["data"] == [
        "__name__",
        "datacenter",
        "host",
    ]
    assert get_json_from_api("/api/v1/labels?end=1000")["data"] == ALL_LABELS
    assert get_json_from_api("/api/v1/labels?start=1031")["data"] == []
    assert get_json_from_api("/api/v1/labels?end=999")["data"] == []


def test_labels_rejects_inverted_time_range():
    get_bad_data_from_api("/api/v1/labels?start=1030&end=1000")


def test_labels_empty_optional_parameters_are_ignored():
    data = get_json_from_api("/api/v1/labels?start=&end=&limit=")["data"]
    assert data == ALL_LABELS


def test_labels_time_range_is_ignored_without_stored_time_bounds():
    # Without stored min_time/max_time the time range is ignored (a superset is allowed by Prometheus).
    data = get_json_from_api("/no_bounds/api/v1/labels?start=2000&end=3000")["data"]
    assert data == ["__name__", "host"]


def test_labels_limit_reports_truncation():
    data = get_json_from_api("/api/v1/labels?limit=2")
    assert data["data"] == ALL_LABELS[:2]
    assert data["warnings"] == ["results truncated due to limit"]


def test_labels_exact_limit_is_not_reported_as_truncated():
    data = get_json_from_api(f"/api/v1/labels?limit={len(ALL_LABELS)}")
    assert data["data"] == ALL_LABELS
    assert "warnings" not in data


def test_labels_zero_limit_is_unlimited():
    data = get_json_from_api("/api/v1/labels?limit=0")
    assert data["data"] == ALL_LABELS
    assert "warnings" not in data


@pytest.mark.parametrize("limit", ["-1", "abc", "1.5"])
def test_labels_rejects_invalid_limit(limit):
    get_bad_data_from_api(f"/api/v1/labels?limit={limit}")


def test_labels_records_query_finish():
    get_json_from_api("/api/v1/labels?match[]=http_requests")
    node.query("SYSTEM FLUSH LOGS query_log")
    assert (
        int(
            node.query(
                "SELECT count() FROM system.query_log WHERE type = 'QueryFinish' "
                "AND query LIKE '%groupUniqArrayArray%' AND query NOT LIKE '%query_log%'"
            )
        )
        > 0
    )
