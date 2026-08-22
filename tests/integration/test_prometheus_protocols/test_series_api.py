"""Tests for the Prometheus /api/v1/series endpoint."""

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
            {"__name__": "quoted_label_metric", "http.status_code": "200", "service": "web"},
            {1000: 1.0},
        ),
        (
            {"__name__": "regex_metric", "host": "server1x"},
            {1000: 1.0},
        ),
        (
            {"__name__": "regex_metric", "host": "server2"},
            {1000: 2.0},
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


def sorted_series(series_list):
    return sorted(series_list, key=lambda labels: sorted(labels.items()))


@pytest.fixture(scope="module", autouse=True)
def setup():
    try:
        cluster.start()
        node.query("CREATE TABLE prometheus ENGINE=TimeSeries")
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


def test_series_returns_metric_labels():
    data = get_json_from_api("/api/v1/series?match[]=cpu_usage")["data"]
    assert sorted_series(data) == sorted_series(
        [
            {"__name__": "cpu_usage", "host": "server1", "datacenter": "us-east"},
            {"__name__": "cpu_usage", "host": "server2", "datacenter": "us-west"},
        ]
    )


def test_series_selector_filters_labels():
    data = get_json_from_api('/api/v1/series?match[]=cpu_usage{host="server1"}')["data"]
    assert data == [
        {"__name__": "cpu_usage", "host": "server1", "datacenter": "us-east"}
    ]


@pytest.mark.parametrize(
    "selector,expected_hosts",
    [
        ('regex_metric{host="server2"}', ["server2"]),
        ('regex_metric{host!="server2"}', ["server1x"]),
        ('regex_metric{host=~"server[0-9]"}', ["server2"]),
        ('regex_metric{host!~"server[0-9]"}', ["server1x"]),
    ],
)
def test_series_selector_matcher_operators(selector, expected_hosts):
    data = get_json_from_api("/api/v1/series", params={"match[]": selector})["data"]
    assert sorted(labels["host"] for labels in data) == expected_hosts


def test_series_regex_matchers_are_anchored():
    # "server1" must not match host="server1x" (Prometheus anchors regexps on both sides).
    data = get_json_from_api('/api/v1/series?match[]=regex_metric{host=~"server1"}')["data"]
    assert data == []


def test_series_matches_without_metric_name():
    data = get_json_from_api('/api/v1/series?match[]={datacenter="us-east"}')["data"]
    assert sorted_series(data) == sorted_series(
        [
            {"__name__": "cpu_usage", "host": "server1", "datacenter": "us-east"},
            {"__name__": "memory_usage", "host": "server1", "datacenter": "us-east"},
        ]
    )


def test_series_supports_quoted_label_names():
    data = get_json_from_api(
        "/api/v1/series", params={"match[]": '{"http.status_code"="200"}'}
    )["data"]
    assert data == [
        {"__name__": "quoted_label_metric", "http.status_code": "200", "service": "web"}
    ]


def test_series_repeated_match_is_union():
    data = get_json_from_api(
        "/api/v1/series?match[]=memory_usage&match[]=quoted_label_metric"
    )["data"]
    assert sorted_series(data) == sorted_series(
        [
            {"__name__": "memory_usage", "host": "server1", "datacenter": "us-east"},
            {"__name__": "quoted_label_metric", "http.status_code": "200", "service": "web"},
        ]
    )


def test_series_repeated_match_deduplicates_overlapping_results():
    data = get_json_from_api(
        '/api/v1/series?match[]=memory_usage&match[]={datacenter="us-east"}'
    )["data"]
    assert sorted_series(data) == sorted_series(
        [
            {"__name__": "cpu_usage", "host": "server1", "datacenter": "us-east"},
            {"__name__": "memory_usage", "host": "server1", "datacenter": "us-east"},
        ]
    )


def test_series_post_urlencoded_match():
    response = requests.post(
        f"http://{node.ip_address}:9093/api/v1/series",
        data=[("match[]", "memory_usage"), ("match[]", "quoted_label_metric")],
    )
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["status"] == "success"
    assert len(data["data"]) == 2


def test_series_requires_match_selector():
    get_bad_data_from_api("/api/v1/series")


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
def test_series_rejects_invalid_selectors(selector):
    get_bad_data_from_api("/api/v1/series", params={"match[]": selector})


def test_series_rejects_an_invalid_selector_in_a_repeated_request():
    get_bad_data_from_api(
        "/api/v1/series", params=[("match[]", "cpu_usage"), ("match[]", "{}")]
    )


def test_series_time_range():
    # All test samples are within [1000, 1030].
    path = "/api/v1/series?match[]=cpu_usage"
    assert len(get_json_from_api(f"{path}&start=990&end=1040")["data"]) == 2
    assert get_json_from_api(f"{path}&start=2000&end=3000")["data"] == []
    assert get_json_from_api(f"{path}&start=0&end=500")["data"] == []


def test_series_time_range_is_inclusive_and_supports_one_sided_bounds():
    path = "/api/v1/series?match[]=cpu_usage"
    # The range [end of the series, ...) and (..., start of the series] both overlap the series.
    assert len(get_json_from_api(f"{path}&start=1030")["data"]) == 2
    assert len(get_json_from_api(f"{path}&end=1000")["data"]) == 2
    assert get_json_from_api(f"{path}&start=1031")["data"] == []
    assert get_json_from_api(f"{path}&end=999")["data"] == []


def test_series_rejects_inverted_time_range():
    get_bad_data_from_api("/api/v1/series?match[]=cpu_usage&start=1030&end=1000")


def test_series_empty_time_range_parameters_are_ignored():
    data = get_json_from_api("/api/v1/series?match[]=cpu_usage&start=&end=")["data"]
    assert len(data) == 2


def test_series_time_range_is_ignored_without_stored_time_bounds():
    # Without stored min_time/max_time the time range is ignored (a superset is allowed by Prometheus).
    data = get_json_from_api("/no_bounds/api/v1/series?match[]=cpu_usage&start=2000&end=3000")["data"]
    assert data == [{"__name__": "cpu_usage", "host": "server1"}]


def test_series_limit_reports_truncation():
    data = get_json_from_api("/api/v1/series?match[]=cpu_usage&limit=1")
    assert len(data["data"]) == 1
    assert data["warnings"] == ["results truncated due to limit"]


def test_series_exact_limit_is_not_reported_as_truncated():
    data = get_json_from_api("/api/v1/series?match[]=cpu_usage&limit=2")
    assert len(data["data"]) == 2
    assert "warnings" not in data


def test_series_zero_limit_is_unlimited():
    data = get_json_from_api("/api/v1/series?match[]=cpu_usage&limit=0")
    assert len(data["data"]) == 2
    assert "warnings" not in data


@pytest.mark.parametrize("limit", ["-1", "abc", "1.5"])
def test_series_rejects_invalid_limit(limit):
    get_bad_data_from_api(f"/api/v1/series?match[]=cpu_usage&limit={limit}")


def test_series_records_query_finish():
    get_json_from_api("/api/v1/series?match[]=memory_usage")
    node.query("SYSTEM FLUSH LOGS query_log")
    assert (
        int(
            node.query(
                "SELECT count() FROM system.query_log WHERE type = 'QueryFinish' "
                "AND query LIKE '%timeSeriesIdToTags%' AND query NOT LIKE '%query_log%'"
            )
        )
        > 0
    )
