"""Tests for the Prometheus /api/v1/series endpoint."""

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
            {"__name__": "http_requests_total", "host": "server1", "method": "GET", "status": "200"},
            {1000: 100, 1015: 150, 1030: 200},
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
        node.query("CREATE TABLE prometheus ENGINE=TimeSeries")
        node.query(
            "CREATE TABLE prometheus_no_filter ENGINE=TimeSeries "
            "SETTINGS filter_by_min_time_and_max_time = 0"
        )
        node.query(
            "CREATE TABLE prometheus_no_bounds ENGINE=TimeSeries "
            "SETTINGS store_min_time_and_max_time = 0"
        )
        for table in ("prometheus_no_filter", "prometheus_no_bounds"):
            node.query(
                f"INSERT INTO {table} (metric_name, tags, time_series) VALUES "
                "('cpu_usage', {'host': 'server1'}, [(toDateTime64(1000, 3), 0.5)])"
            )
        node.query(
            "CREATE TABLE prometheus_uint32 (time_series Array(Tuple(UInt32, Float64))) ENGINE=TimeSeries"
        )
        node.query(
            "INSERT INTO prometheus_uint32 (metric_name, tags, time_series) VALUES "
            "('uint32_metric', {'host': 'server1'}, [(toUInt32(100), 1)])"
        )
        node.query(
            "CREATE TABLE prometheus_datetime64_9 (time_series Array(Tuple(DateTime64(9), Float64))) ENGINE=TimeSeries"
        )
        node.query(
            "INSERT INTO prometheus_datetime64_9 (metric_name, tags, time_series) VALUES "
            "('datetime64_9_metric', {'host': 'server1'}, [(toDateTime64(1000, 9), 1)])"
        )
        node.query(
            "CREATE TABLE prometheus_datetime64_3_future "
            "(time_series Array(Tuple(DateTime64(3), Float64))) ENGINE=TimeSeries"
        )
        node.query(
            "INSERT INTO prometheus_datetime64_3_future (metric_name, tags, time_series) VALUES "
            "('datetime64_3_future_metric', {'host': 'server1'}, "
            "[(toDateTime64('2286-11-20 17:46:40.250', 3), 1)]),"
            "('datetime64_3_negative_metric', {'host': 'server1'}, "
            "[(toDateTime64(-100, 3), 1)])"
        )
        send_test_data()
        assert_eq_with_retry(node, "SELECT count() > 0 FROM timeSeriesData(prometheus)", "1")
        yield cluster
    finally:
        cluster.shutdown()


def test_series_returns_metric_labels():
    data = get_json_from_api("/api/v1/series?match[]=cpu_usage")["data"]
    assert {entry["__name__"] for entry in data} == {"cpu_usage"}
    assert {entry["host"] for entry in data} == {"server1", "server2"}
    assert all("datacenter" in entry for entry in data)


def test_series_match_filter():
    data = get_json_from_api("/api/v1/series?match[]=memory_usage")["data"]
    assert len(data) == 1
    assert data[0]["__name__"] == "memory_usage"


def test_series_does_not_duplicate_metric_name():
    response = requests.get(f"http://{node.ip_address}:9093/api/v1/series?match[]=memory_usage")
    assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
    assert response.text.count('"__name__"') == 1, f"Unexpected body: {response.text}"


def test_series_repeated_match_is_union():
    data = get_json_from_api("/api/v1/series?match[]=cpu_usage&match[]=memory_usage")["data"]
    assert {entry["__name__"] for entry in data} == {"cpu_usage", "memory_usage"}


def test_series_repeated_match_deduplicates_overlapping_results():
    data = get_json_from_api(
        "/api/v1/series",
        params=[("match[]", "cpu_usage"), ("match[]", 'cpu_usage{host="server1"}')],
    )["data"]
    assert len(data) == 2
    assert {entry["host"] for entry in data} == {"server1", "server2"}


def test_series_post_urlencoded_repeated_match_is_union():
    response = requests.post(
        f"http://{node.ip_address}:9093/api/v1/series",
        data=[("match[]", "cpu_usage"), ("match[]", "memory_usage")],
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "success"
    assert {entry["__name__"] for entry in data["data"]} == {"cpu_usage", "memory_usage"}


def test_series_post_urlencoded_empty_optional_parameters_are_ignored():
    response = requests.post(
        f"http://{node.ip_address}:9093/api/v1/series",
        data=[("match[]", "cpu_usage"), ("start", ""), ("end", ""), ("limit", "")],
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "success"
    assert len(data["data"]) == 2
    assert "warnings" not in data


def test_series_selector_filters_labels():
    data = get_json_from_api('/api/v1/series?match[]=cpu_usage{host="server1"}')["data"]
    assert data == [{"__name__": "cpu_usage", "datacenter": "us-east", "host": "server1"}]


@pytest.mark.parametrize(
    "selector, expected_hosts",
    [
        ('cpu_usage{host="server1"}', {"server1"}),
        ('cpu_usage{host!="server1"}', {"server2"}),
        ('cpu_usage{host=~"server1|server2"}', {"server1", "server2"}),
        ('cpu_usage{host!~"server1"}', {"server2"}),
    ],
)
def test_series_selector_matcher_operators(selector, expected_hosts):
    data = get_json_from_api("/api/v1/series", params={"match[]": selector})["data"]
    assert {entry["host"] for entry in data} == expected_hosts


def test_series_selector_matches_metric_name_and_multiple_labels():
    data = get_json_from_api(
        "/api/v1/series",
        params={"match[]": '{__name__="cpu_usage",host="server1",datacenter="us-east"}'},
    )["data"]
    assert data == [{"__name__": "cpu_usage", "datacenter": "us-east", "host": "server1"}]


def test_series_regex_matchers_are_anchored():
    data = get_json_from_api("/api/v1/series", params={"match[]": 'cpu_usage{host=~"server"}'})["data"]
    assert data == []


def test_series_matches_without_metric_name():
    data = get_json_from_api('/api/v1/series?match[]={datacenter="us-east"}')['data']
    assert {tuple(sorted(entry.items())) for entry in data} == {
        (("__name__", "cpu_usage"), ("datacenter", "us-east"), ("host", "server1")),
        (("__name__", "memory_usage"), ("datacenter", "us-east"), ("host", "server1")),
    }


def test_series_regex_matcher_alternation_is_anchored_as_a_whole():
    data = get_json_from_api(
        "/api/v1/series",
        params={"match[]": 'regex_metric{host=~"server1|server2"}'},
    )["data"]
    assert [entry["host"] for entry in data] == ["server2"]


def test_series_rejects_prometheus_unsupported_regexp_escape():
    get_bad_data_from_api("/api/v1/series", params={"match[]": r"{host=~`\C`}"})


def test_series_missing_label_matches_empty_value():
    equal_data = get_json_from_api("/api/v1/series", params={"match[]": 'cpu_usage{zone=""}'})["data"]
    not_equal_data = get_json_from_api("/api/v1/series", params={"match[]": 'cpu_usage{zone!="prod"}'})["data"]
    assert {entry["host"] for entry in equal_data} == {"server1", "server2"}
    assert {entry["host"] for entry in not_equal_data} == {"server1", "server2"}


def test_series_supports_quoted_label_names():
    data = get_json_from_api(
        "/api/v1/series", params={"match[]": '{"http.status_code"="200"}'}
    )["data"]
    assert data == [{"__name__": "quoted_label_metric", "http.status_code": "200", "service": "web"}]


def test_series_requires_match_selector():
    response = requests.get(f"http://{node.ip_address}:9093/api/v1/series")
    assert response.status_code == 400
    assert response.json()["errorType"] == "bad_data"


def test_series_requires_select_on_configured_time_series_table():
    response = requests.get(
        f"http://{node.ip_address}:9093/api/v1/series",
        params={"match[]": "cpu_usage"},
        auth=("metadata_temp_table_only", ""),
    )
    assert response.status_code == 400, response.text
    data = response.json()
    assert data["status"] == "error"
    assert data["errorType"] == "bad_data"
    assert "SELECT" in data["error"]
    assert "default.prometheus" in data["error"]


def test_series_allows_select_on_configured_time_series_table():
    data = get_json_from_api(
        "/api/v1/series",
        params={"match[]": "cpu_usage"},
        auth=("metadata_select_time_series", ""),
    )["data"]
    assert {entry["host"] for entry in data} == {"server1", "server2"}


@pytest.mark.parametrize(
    "params, expected_labels",
    [
        ({}, ["__name__", "datacenter", "host", "http.status_code", "method", "service", "status"]),
        ({"match[]": "cpu_usage"}, ["__name__", "datacenter", "host"]),
        (
            [("match[]", "cpu_usage"), ("match[]", "http_requests_total")],
            ["__name__", "datacenter", "host", "method", "status"],
        ),
    ],
)
def test_labels_allows_select_on_configured_time_series_table(params, expected_labels):
    data = get_json_from_api(
        "/combined/api/v1/labels",
        params=params,
        auth=("metadata_select_time_series", ""),
    )
    assert data["data"] == expected_labels


@pytest.mark.parametrize("path", ["/combined/api/v1/format_query", "/combined/api/v1/parse_query"])
def test_parser_endpoints_do_not_require_time_series_select(path):
    response = requests.get(
        f"http://{node.ip_address}:9093{path}",
        auth=("metadata_temp_table_only", ""),
    )
    assert response.status_code == 400, response.text
    data = response.json()
    assert data["status"] == "error"
    assert data["errorType"] == "bad_data"
    assert "not implemented" in data["error"]
    assert "SELECT" not in data["error"]
    assert "default.prometheus" not in data["error"]


@pytest.mark.parametrize(
    "path, params",
    [
        ("/combined/api/v1/query", {"query": "cpu_usage", "time": 1015}),
        (
            "/combined/api/v1/query_range",
            {"query": "cpu_usage", "start": 1000, "end": 1030, "step": 15},
        ),
        ("/combined/api/v1/series", {"match[]": "cpu_usage"}),
        ("/combined/api/v1/labels", {}),
        ("/combined/api/v1/label/host/values", {}),
    ],
)
def test_all_query_endpoints_require_select_on_configured_time_series_table(path, params):
    response = requests.get(
        f"http://{node.ip_address}:9093{path}",
        params=params,
        auth=("metadata_temp_table_only", ""),
    )
    assert response.status_code == 400, response.text
    data = response.json()
    assert data["status"] == "error"
    assert data["errorType"] == "bad_data"
    assert "SELECT" in data["error"]
    assert "default.prometheus" in data["error"]


@pytest.mark.parametrize(
    "path, params",
    [
        ("/api/v1/query", {"query": "cpu_usage", "time": 1015}),
        (
            "/api/v1/query_range",
            {"query": "cpu_usage", "start": 1000, "end": 1030, "step": 15},
        ),
    ],
)
def test_query_endpoints_allow_select_on_configured_time_series_table(path, params):
    data = get_json_from_api(
        path,
        params=params,
        auth=("metadata_select_time_series", ""),
    )
    assert data["status"] == "success"


def test_scalar_query_requires_select_on_configured_time_series_table():
    response = requests.get(
        f"http://{node.ip_address}:9093/api/v1/query",
        params={"query": "1", "time": 1015},
        auth=("metadata_temp_table_only", ""),
    )
    assert response.status_code == 400, response.text
    data = response.json()
    assert data["status"] == "error"
    assert data["errorType"] == "bad_data"
    assert "SELECT" in data["error"]
    assert "default.prometheus" in data["error"]


def test_unknown_endpoint_is_not_authorized_as_a_table_read():
    response = requests.get(
        f"http://{node.ip_address}:9093/combined/api/v1/does-not-exist",
        auth=("metadata_temp_table_only", ""),
    )
    assert response.status_code == 404, response.text
    assert "default.prometheus" not in response.text
    assert response.json() == {
        "status": "error",
        "errorType": "not_found",
        "error": "API endpoint not found",
    }


@pytest.mark.parametrize(
    "selector",
    [
        "",
        "{}",
        "cpu_usage[5m]",
        "rate(cpu_usage[5m])",
        "sum(cpu_usage)",
        "(cpu_usage)",
        '((cpu_usage{host="server1"}))',
        "cpu_usage offset 5m",
        "cpu_usage @ 1700000000",
        '{host=""}',
        '{host!="prod"}',
        '{host!~"prod"}',
        '{host=~".*"}',
        'cpu_usage{host=~"["}',
    ],
)
def test_series_rejects_invalid_or_unbounded_selectors(selector):
    get_bad_data_from_api("/api/v1/series", params={"match[]": selector})


def test_series_rejects_an_invalid_selector_in_a_repeated_request():
    get_bad_data_from_api(
        "/api/v1/series",
        params=[("match[]", "cpu_usage"), ("match[]", "")],
    )


def test_series_accepts_negative_regex_that_cannot_match_empty():
    data = get_json_from_api(
        "/api/v1/series", params={"match[]": '{host!~".*"}'}
    )["data"]
    assert data == []


@pytest.mark.parametrize("selector", ['{host!=""}', '{host=~".+"}'])
def test_series_accepts_matchers_that_cannot_match_empty(selector):
    data = get_json_from_api("/api/v1/series", params={"match[]": selector})["data"]
    assert data


def test_series_deduplicates_rows_before_limit():
    send_test_data()
    data = get_json_from_api(
        "/api/v1/series", params={"match[]": "cpu_usage", "limit": 2}
    )
    assert len(data["data"]) == 2
    assert "warnings" not in data


def test_series_time_range():
    in_range = get_json_from_api("/api/v1/series?match[]=cpu_usage&start=1010&end=1020")["data"]
    out_of_range = get_json_from_api("/api/v1/series?match[]=cpu_usage&start=2000&end=3000")["data"]
    assert len(in_range) == 2
    assert out_of_range == []


def test_series_time_range_is_inclusive_and_supports_one_sided_bounds():
    at_start = get_json_from_api(
        "/api/v1/series", params={"match[]": "cpu_usage", "start": 1000, "end": 1000}
    )["data"]
    at_end = get_json_from_api(
        "/api/v1/series", params={"match[]": "cpu_usage", "start": 1030, "end": 1030}
    )["data"]
    from_start = get_json_from_api("/api/v1/series", params={"match[]": "cpu_usage", "start": 1030})["data"]
    through_end = get_json_from_api("/api/v1/series", params={"match[]": "cpu_usage", "end": 1000})["data"]
    assert len(at_start) == 2
    assert len(at_end) == 2
    assert len(from_start) == 2
    assert len(through_end) == 2


def test_series_time_range_accepts_rfc3339_bounds():
    data = get_json_from_api(
        "/api/v1/series",
        params={
            "match[]": "cpu_usage",
            "start": "1970-01-01T00:16:40Z",
            "end": "1970-01-01T00:16:40Z",
        },
    )["data"]
    assert len(data) == 2


def test_series_time_range_accepts_rfc3339_comma_fraction():
    data = get_json_from_api(
        "/api/v1/series",
        params={
            "match[]": "cpu_usage",
            "start": "1970-01-01T00:16:40,000Z",
            "end": "1970-01-01T00:16:40,000Z",
        },
    )["data"]
    assert len(data) == 2


def test_series_time_range_applies_rfc3339_timezone_offsets():
    data = get_json_from_api(
        "/api/v1/series",
        params={
            "match[]": "cpu_usage",
            "start": "1970-01-01T02:16:40+02:00",
            "end": "1970-01-01T02:16:40+02:00",
        },
    )["data"]
    assert len(data) == 2


@pytest.mark.parametrize(
    "bound_value",
    [
        "1970-01-01T00:16:40",
        "1970-01-01T00:16:40Ztrailing",
        "5m",
        "0x10",
        "1.2.3",
        "1..0",
        "..1",
        "2023-02-29T00:00:00Z",
        "2024-02-30T00:00:00Z",
        "2024-04-31T00:00:00Z",
    ],
)
def test_series_rejects_non_prometheus_http_timestamps(bound_value):
    get_bad_data_from_api(
        "/api/v1/series",
        params={"match[]": "cpu_usage", "start": bound_value},
    )


@pytest.mark.parametrize("bound_value", ["1e3", "1.5e3", "1e+3", "1e-3"])
def test_series_accepts_decimal_exponents_in_http_timestamps(bound_value):
    get_json_from_api(
        "/api/v1/series",
        params={"match[]": "cpu_usage", "start": bound_value},
    )


@pytest.mark.parametrize("bound_value", ["1_000", "0x1p4"])
def test_series_accepts_go_float_syntax_in_http_timestamps(bound_value):
    get_json_from_api(
        "/api/v1/series",
        params={"match[]": "cpu_usage", "start": bound_value},
    )


def test_series_datetime64_3_preserves_far_future_rfc3339_fractions():
    data = get_json_from_api(
        "/datetime64_3_future/api/v1/series",
        params={
            "match[]": "datetime64_3_future_metric",
            "start": "2286-11-20T17:46:40.250Z",
            "end": "2286-11-20T17:46:40.500Z",
        },
    )["data"]
    assert len(data) == 1


@pytest.mark.parametrize(
    "path, selector, start, end",
    [
        ("/api/v1/series", "cpu_usage", "1000.0009", "1000.0001"),
        ("/uint32/api/v1/series", "uint32_metric", "100.9", "100.1"),
    ],
)
def test_series_rejects_inverted_range_before_storage_quantization(path, selector, start, end):
    get_bad_data_from_api(path, params={"match[]": selector, "start": start, "end": end})


@pytest.mark.parametrize(
    "params, expected_count",
    [
        ({"match[]": "uint32_metric", "start": "-1", "end": "200"}, 1),
        ({"match[]": "uint32_metric", "start": "0", "end": "4294967296"}, 1),
        ({"match[]": "uint32_metric", "start": "4294967296"}, 0),
        ({"match[]": "uint32_metric", "end": "-1"}, 0),
    ],
)
def test_series_uint32_time_bounds_do_not_wrap(params, expected_count):
    data = get_json_from_api("/uint32/api/v1/series", params=params)["data"]
    assert len(data) == expected_count


@pytest.mark.parametrize(
    "bound_name, bound_value, expected_count",
    [
        ("end", "10000000000", 1),
        ("end", "2286-11-20T17:46:40Z", 1),
        ("start", "10000000000", 0),
        ("start", "2286-11-20T17:46:40Z", 0),
    ],
)
def test_series_datetime64_9_time_bounds_are_clipped_before_native_conversion(
    bound_name, bound_value, expected_count
):
    params = {"match[]": "datetime64_9_metric", bound_name: bound_value}
    data = get_json_from_api("/datetime64_9/api/v1/series", params=params)["data"]
    assert len(data) == expected_count


@pytest.mark.parametrize(
    "path, selector, bound_name, bound_value, expected_count",
    [
        ("/uint32/api/v1/series", "uint32_metric", "start", "100.9", 0),
        ("/api/v1/series", "cpu_usage", "start", "1000.001", 2),
        (
            "/datetime64_3_future/api/v1/series",
            "datetime64_3_negative_metric",
            "end",
            "-100.0009",
            0,
        ),
        (
            "/datetime64_3_future/api/v1/series",
            "datetime64_3_negative_metric",
            "end",
            "-100",
            1,
        ),
        ("/api/v1/series", "cpu_usage", "start", "1000.0000", 2),
    ],
)
def test_series_fractional_bounds_are_quantized_towards_overlap(
    path, selector, bound_name, bound_value, expected_count
):
    data = get_json_from_api(
        path,
        params={"match[]": selector, bound_name: bound_value},
    )["data"]
    assert len(data) == expected_count


@pytest.mark.parametrize("path", ["/no_filter/api/v1/series", "/no_bounds/api/v1/series"])
def test_series_time_range_returns_approximate_superset_without_trusted_bounds(path):
    data = get_json_from_api(
        path,
        params={"match[]": "cpu_usage", "start": 2000, "end": 3000},
    )["data"]
    assert data == [{"__name__": "cpu_usage", "host": "server1"}]


@pytest.mark.parametrize("path", ["/no_filter/api/v1/series", "/no_bounds/api/v1/series"])
def test_series_time_range_still_validates_timestamps_without_trusted_bounds(path):
    get_bad_data_from_api(
        path,
        params={"match[]": "cpu_usage", "start": "not-a-timestamp"},
    )


@pytest.mark.parametrize(
    "params",
    [
        {"match[]": "cpu_usage", "start": 2000, "end": 1000},
        {"match[]": "cpu_usage", "start": "not-a-timestamp"},
        {"match[]": "cpu_usage", "end": "not-a-timestamp"},
    ],
)
def test_series_rejects_invalid_time_ranges(params):
    get_bad_data_from_api("/api/v1/series", params=params)


def test_series_empty_time_range_is_ignored():
    data = get_json_from_api(
        "/api/v1/series",
        params={"match[]": "cpu_usage", "start": "", "end": "", "limit": ""},
    )["data"]
    assert len(data) == 2


def test_series_limit_reports_truncation():
    data = get_json_from_api("/api/v1/series?match[]=cpu_usage&limit=1")
    assert len(data["data"]) == 1
    assert data["warnings"] == ["results truncated due to limit"]


def test_series_limit_across_multiple_blocks():
    params = {
        "match[]": ["cpu_usage", "memory_usage"],
        "limit": 2,
        "max_block_size": 1,
        "max_threads": 1,
    }
    data = get_json_from_api("/api/v1/series", params=params)
    assert len(data["data"]) == 2
    assert data["warnings"] == ["results truncated due to limit"]

    exact_data = get_json_from_api(
        "/api/v1/series",
        params={**params, "limit": 3},
    )
    assert len(exact_data["data"]) == 3
    assert "warnings" not in exact_data


def test_series_limit_does_not_cache_a_partial_result():
    node.query("SYSTEM DROP QUERY CACHE")
    cache_params = {
        "match[]": ["cpu_usage", "memory_usage"],
        "use_query_cache": 1,
        "query_cache_nondeterministic_function_handling": "save",
        "query_cache_min_query_duration": 0,
        "max_block_size": 1,
        "max_threads": 1,
    }

    limited_data = get_json_from_api(
        "/api/v1/series",
        params={**cache_params, "limit": 1},
    )
    assert len(limited_data["data"]) == 1
    assert limited_data["warnings"] == ["results truncated due to limit"]
    assert int(node.query("SELECT count() FROM system.query_cache")) == 0

    unlimited_data = get_json_from_api("/api/v1/series", params=cache_params)
    assert len(unlimited_data["data"]) == 3
    assert "warnings" not in unlimited_data
    assert int(node.query("SELECT count() FROM system.query_cache")) == 1


def test_series_zero_limit_is_unlimited():
    data = get_json_from_api("/api/v1/series", params={"match[]": "cpu_usage", "limit": 0})
    assert len(data["data"]) == 2
    assert "warnings" not in data


def test_series_exact_limit_is_not_reported_as_truncated():
    data = get_json_from_api("/api/v1/series", params={"match[]": "cpu_usage", "limit": 2})
    assert len(data["data"]) == 2
    assert "warnings" not in data


def test_series_accepts_signed_int64_max_limit():
    data = get_json_from_api(
        "/api/v1/series",
        params={"match[]": "cpu_usage", "limit": "9223372036854775807"},
    )
    assert len(data["data"]) == 2
    assert "warnings" not in data


@pytest.mark.parametrize(
    "limit",
    [
        "-1",
        "not-a-number",
        "9223372036854775808",
        "18446744073709551615",
        "18446744073709551616",
    ],
)
def test_series_rejects_invalid_limit(limit):
    get_bad_data_from_api("/api/v1/series", params={"match[]": "cpu_usage", "limit": limit})


def test_series_records_query_finish():
    query_id = "prometheus_series_query_log_test"
    get_json_from_api("/api/v1/series?match[]=cpu_usage", headers={"X-ClickHouse-Query-Id": query_id})
    node.query("SYSTEM FLUSH LOGS query_log")
    assert_eq_with_retry(
        node,
        f"SELECT count() FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'",
        "1",
    )


def test_series_limited_response_records_query_finish():
    query_id = "prometheus_series_limited_query_log_test"
    get_json_from_api(
        "/api/v1/series",
        params={"match[]": "cpu_usage", "limit": 1},
        headers={"X-ClickHouse-Query-Id": query_id},
    )
    node.query("SYSTEM FLUSH LOGS query_log")
    assert_eq_with_retry(
        node,
        f"SELECT count() FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish'",
        "1",
    )
