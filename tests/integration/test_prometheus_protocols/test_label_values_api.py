"""Tests for the Prometheus /api/v1/label/<name>/values endpoint."""

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

ALL_METRIC_NAMES = ["cpu_usage", "http_requests", "memory_usage"]


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
        # A label name that is not a legacy Prometheus name ([a-zA-Z_][a-zA-Z0-9_]*):
        # the endpoint must find it both by the exact name and by its "U__..." escaped form.
        (
            {"__name__": "http_requests", "service": "web", "http.status": "200"},
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
        # The `host` tag is stored in a dedicated column: the endpoint must still report
        # its values under the tag name.
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


def test_label_values_metric_names():
    # The special label __name__ holds the metric names.
    assert get_json_from_api("/api/v1/label/__name__/values")["data"] == ALL_METRIC_NAMES


def test_label_values_returns_sorted_unique_values():
    assert get_json_from_api("/api/v1/label/datacenter/values")["data"] == [
        "us-east",
        "us-west",
    ]


def test_label_values_tag_stored_in_dedicated_column():
    assert get_json_from_api("/api/v1/label/host/values")["data"] == [
        "server1",
        "server2",
    ]


def test_label_values_unknown_label_returns_empty():
    assert get_json_from_api("/api/v1/label/no_such_label/values")["data"] == []


def test_label_values_match_selector_filters():
    data = get_json_from_api("/api/v1/label/host/values?match[]=memory_usage")["data"]
    assert data == ["server1"]
    data = get_json_from_api(
        "/api/v1/label/datacenter/values", params={"match[]": '{host="server2"}'}
    )["data"]
    assert data == ["us-west"]


def test_label_values_repeated_match_is_union():
    data = get_json_from_api(
        "/api/v1/label/host/values",
        params=[("match[]", "memory_usage"), ("match[]", '{datacenter="us-west"}')],
    )["data"]
    assert data == ["server1", "server2"]


def test_label_values_no_matching_series_returns_empty():
    assert (
        get_json_from_api("/api/v1/label/host/values?match[]=missing_metric")["data"]
        == []
    )


def test_label_values_post_urlencoded_match():
    response = requests.post(
        f"http://{node.ip_address}:9093/api/v1/label/host/values",
        data=[("match[]", "cpu_usage")],
    )
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["status"] == "success"
    assert data["data"] == ["server1", "server2"]


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
def test_label_values_rejects_invalid_selectors(selector):
    get_bad_data_from_api("/api/v1/label/host/values", params={"match[]": selector})


def test_label_values_escaped_utf8_name():
    # Prometheus escapes non-legacy label names with the "values" scheme: "http.status"
    # becomes "U__http_2e_status" ('.' is code point 0x2e).
    assert get_json_from_api("/api/v1/label/U__http_2e_status/values")["data"] == ["200"]
    # The exact name works too.
    assert get_json_from_api("/api/v1/label/http.status/values")["data"] == ["200"]
    # Legacy names can also be sent escaped.
    assert get_json_from_api("/api/v1/label/U__host/values")["data"] == [
        "server1",
        "server2",
    ]


@pytest.mark.parametrize(
    "name",
    [
        "U__http_2e",  # unterminated hex escape
        "U__http_xy_status",  # invalid hex digits
        "U__http_2345678_status",  # more than six hex digits
        "U__http_d800_status",  # surrogate code point
        "U__http_110000_status",  # beyond the last Unicode code point
    ],
)
def test_label_values_malformed_escape_is_treated_literally(name):
    # Like in Prometheus, a malformed escape sequence leaves the name unchanged,
    # and no stored label has such a name.
    assert get_json_from_api(f"/api/v1/label/{name}/values")["data"] == []


def test_label_values_empty_decoded_name_is_rejected():
    get_bad_data_from_api("/api/v1/label/U__/values")


def test_label_values_time_range():
    # All test samples are within [1000, 1030]; only the cpu_usage series extend to 1030.
    assert get_json_from_api("/api/v1/label/host/values?start=990&end=1040")["data"] == [
        "server1",
        "server2",
    ]
    assert get_json_from_api("/api/v1/label/host/values?start=2000&end=3000")["data"] == []
    assert get_json_from_api("/api/v1/label/region/values?start=1030")["data"] == []
    assert get_json_from_api("/api/v1/label/region/values?end=1000")["data"] == ["eu"]


def test_label_values_rejects_inverted_time_range():
    get_bad_data_from_api("/api/v1/label/host/values?start=1030&end=1000")


def test_label_values_empty_optional_parameters_are_ignored():
    data = get_json_from_api("/api/v1/label/host/values?start=&end=&limit=")["data"]
    assert data == ["server1", "server2"]


def test_label_values_time_range_is_ignored_without_stored_time_bounds():
    # Without stored min_time/max_time the time range is ignored (a superset is allowed by Prometheus).
    data = get_json_from_api("/no_bounds/api/v1/label/host/values?start=2000&end=3000")["data"]
    assert data == ["server1"]


def test_label_values_limit_reports_truncation():
    data = get_json_from_api("/api/v1/label/__name__/values?limit=2")
    assert data["data"] == ALL_METRIC_NAMES[:2]
    assert data["warnings"] == ["results truncated due to limit"]


def test_label_values_exact_limit_is_not_reported_as_truncated():
    data = get_json_from_api(f"/api/v1/label/__name__/values?limit={len(ALL_METRIC_NAMES)}")
    assert data["data"] == ALL_METRIC_NAMES
    assert "warnings" not in data


def test_label_values_zero_limit_is_unlimited():
    data = get_json_from_api("/api/v1/label/__name__/values?limit=0")
    assert data["data"] == ALL_METRIC_NAMES
    assert "warnings" not in data


@pytest.mark.parametrize("limit", ["-1", "abc", "1.5"])
def test_label_values_rejects_invalid_limit(limit):
    get_bad_data_from_api(f"/api/v1/label/__name__/values?limit={limit}")


def test_label_values_records_query_finish():
    get_json_from_api("/api/v1/label/service/values")
    node.query("SYSTEM FLUSH LOGS query_log")
    assert (
        int(
            node.query(
                "SELECT count() FROM system.query_log WHERE type = 'QueryFinish' "
                "AND query LIKE '%arrayFilter%' AND query NOT LIKE '%query_log%'"
            )
        )
        > 0
    )
