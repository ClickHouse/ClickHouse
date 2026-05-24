"""Tests for Prometheus HTTP API endpoints: /api/v1/series, /api/v1/labels, /api/v1/label/<name>/values

Run locally with the in-tree server binary, e.g.:
  CLICKHOUSE_TESTS_SERVER_BIN_PATH=$PWD/build/programs/clickhouse pytest ...
"""

import urllib.parse

import pytest
import requests

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
    # Label names must be in strict lexicographic order (TimeSeries / remote write validation).
    time_series = [
        (
            {"__name__": "cpu_usage", "datacenter": "us-east", "host": "server1"},
            {1000: 0.5, 1015: 0.6, 1030: 0.7},
        ),
        (
            {"__name__": "cpu_usage", "datacenter": "us-west", "host": "server2"},
            {1000: 0.3, 1015: 0.4, 1030: 0.5},
        ),
        (
            {"__name__": "memory_usage", "datacenter": "us-east", "host": "server1"},
            {1000: 0.8, 1015: 0.85, 1030: 0.9},
        ),
        (
            {"__name__": "http_requests_total", "host": "server1", "method": "GET", "status": "200"},
            {1000: 100, 1015: 150, 1030: 200},
        ),
        (
            {"__name__": "with_empty_zone", "datacenter": "us-east", "host": "server1", "zone": ""},
            {1000: 1.0},
        ),
        # Series with an explicit non-empty `zone` so `{zone!=""}` can be exercised meaningfully.
        (
            {"__name__": "rack_metric", "datacenter": "us-east", "zone": "us-east-1a"},
            {1000: 0.5},
        ),
    ]
    protobuf = convert_time_series_to_protobuf(time_series)
    send_protobuf_to_remote_write(node.ip_address, 9093, "/write", protobuf)


def get_json_from_api(path, params=None):
    """Make a GET request to the ClickHouse Prometheus API and return parsed JSON."""
    url = f"http://{node.ip_address}:9093{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params, doseq=True)
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


def test_label_values_includes_empty_string():
    """Prometheus: explicit empty label values must appear in /label/<name>/values."""
    data = get_json_from_api("/api/v1/label/zone/values")
    assert isinstance(data, list)
    assert "" in data


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


def test_series_multiple_match_union():
    """Repeated match[] is OR across selectors (Prometheus union semantics)."""
    data = get_json_from_api(
        "/api/v1/series",
        params=[
            ("match[]", '{__name__="cpu_usage"}'),
            ("match[]", '{__name__="memory_usage"}'),
        ],
    )
    assert isinstance(data, list)
    names = {e["__name__"] for e in data if "__name__" in e}
    assert names == {"cpu_usage", "memory_usage"}
    assert "http_requests_total" not in names


def test_series_multiple_match_with_time_window():
    """Union of match[] must AND with start/end. Without wrapping the OR chain, `a OR b AND time` wrongly keeps rows matching only `a` when `time` is false for them."""
    union = get_json_from_api(
        "/api/v1/series",
        params=[
            ("match[]", '{__name__="cpu_usage"}'),
            ("match[]", '{__name__="memory_usage"}'),
        ],
    )
    assert {e["__name__"] for e in union if "__name__" in e} == {"cpu_usage", "memory_usage"}

    # Far-future window: no series overlaps; result must be empty. Buggy precedence would still return `cpu_usage` (first match[] without time).
    data_empty = get_json_from_api(
        "/api/v1/series",
        params=[
            ("match[]", '{__name__="cpu_usage"}'),
            ("match[]", '{__name__="memory_usage"}'),
            ("start", "1000000000"),
            ("end", "1000000001"),
        ],
    )
    assert data_empty == []


def test_labels_multiple_match_union():
    """Repeated match[] on /labels unions label names from any matching series."""
    data = get_json_from_api(
        "/api/v1/labels",
        params=[
            ("match[]", '{__name__="cpu_usage"}'),
            ("match[]", '{__name__="http_requests_total"}'),
        ],
    )
    assert isinstance(data, list)
    assert "__name__" in data
    assert "method" in data
    assert "datacenter" in data


def test_label_values_name_multiple_match_union():
    """Repeated match[] on /label/__name__/values restricts metric names to the union of selectors."""
    data = get_json_from_api(
        "/api/v1/label/__name__/values",
        params=[
            ("match[]", '{__name__="cpu_usage"}'),
            ("match[]", '{__name__="memory_usage"}'),
        ],
    )
    assert isinstance(data, list)
    assert set(data) == {"cpu_usage", "memory_usage"}
    assert "http_requests_total" not in data


def test_series_with_promql_selector_host():
    """GET /api/v1/series with match[] selector filters by host."""
    data = get_json_from_api(
        "/api/v1/series",
        params=[("match[]", '{host="server1"}')],
    )
    assert isinstance(data, list)
    assert len(data) >= 1
    for entry in data:
        assert entry.get("host") == "server1"


def test_series_with_promql_selector_regex_and_ne():
    """GET /api/v1/series with match[] uses method and status matchers."""
    data = get_json_from_api(
        "/api/v1/series",
        params=[("match[]", '{method!="",status=~"2.."}')],
    )
    assert isinstance(data, list)
    assert len(data) >= 1
    names = {e["__name__"] for e in data if "__name__" in e}
    assert "http_requests_total" in names


def test_series_missing_label_matches_empty_eq():
    """Prometheus: absent label is implicit ''; {zone=\"\"} matches series without zone."""
    data = get_json_from_api(
        "/api/v1/series",
        params=[("match[]", '{zone=""}')],
    )
    assert isinstance(data, list)
    names = {e["__name__"] for e in data if "__name__" in e}
    assert "http_requests_total" in names


def test_series_missing_label_matches_empty_regex():
    """Absent label is ''; regex matches against empty (e.g. .*)."""
    data = get_json_from_api(
        "/api/v1/series",
        params=[("match[]", '{zone=~".*"}')],
    )
    assert isinstance(data, list)
    names = {e["__name__"] for e in data if "__name__" in e}
    assert "http_requests_total" in names


def test_series_non_empty_eq_excludes_missing_label():
    """EQ to a non-empty value requires the key in the tag map."""
    data = get_json_from_api(
        "/api/v1/series",
        params=[("match[]", '{zone="us-east"}')],
    )
    assert isinstance(data, list)
    names = {e["__name__"] for e in data if "__name__" in e}
    assert "http_requests_total" not in names


def test_series_ne_empty_excludes_missing_and_explicit_empty():
    """`{zone!=""}` must exclude series where `zone` is absent (Prometheus treats absent == empty)
    and series where `zone` is explicitly empty. Only series with a non-empty `zone` survive."""
    data = get_json_from_api(
        "/api/v1/series",
        params=[("match[]", '{zone!=""}')],
    )
    assert isinstance(data, list)
    names = {e["__name__"] for e in data if "__name__" in e}
    # Must include the only series with a non-empty zone.
    assert names == {"rack_metric"}, names
    # Sanity: the entry actually carries the non-empty zone value.
    rack = next(e for e in data if e.get("__name__") == "rack_metric")
    assert rack.get("zone") == "us-east-1a"


def test_labels_ne_empty_includes_zone_only_for_non_empty_rows():
    """`/api/v1/labels?match[]={zone!=""}` must derive labels from the matched (non-empty zone) rows
    and not from series where `zone` is absent or explicitly empty."""
    data = get_json_from_api(
        "/api/v1/labels",
        params=[("match[]", '{zone!=""}')],
    )
    assert "__name__" in data
    assert "zone" in data
    assert "datacenter" in data
    # Labels not present on `rack_metric` (e.g. `host`, `method`, `status`) must not appear.
    assert "host" not in data, data
    assert "method" not in data, data


def test_label_values_zone_ne_empty_excludes_empty_string():
    """`/api/v1/label/zone/values?match[]={zone!=""}` must not surface `""` and must list
    only the values from rows that actually carry a non-empty `zone`."""
    data = get_json_from_api(
        "/api/v1/label/zone/values",
        params=[("match[]", '{zone!=""}')],
    )
    assert "" not in data, data
    assert data == ["us-east-1a"], data


def test_labels_with_match_selector():
    """labels endpoint narrows when match[] matches a subset of metrics."""
    data = get_json_from_api(
        "/api/v1/labels",
        params=[("match[]", "cpu_usage")],
    )
    assert isinstance(data, list)
    assert "__name__" in data
    assert "host" in data
    assert "method" not in data


def test_label_values_with_regex_match():
    """label values with match[] regex for host."""
    data = get_json_from_api(
        "/api/v1/label/host/values",
        params=[("match[]", '{host=~"server.*"}')],
    )
    assert isinstance(data, list)
    assert "server1" in data
    assert "server2" in data


def test_time_window_params_smoke():
    """start/end query params are accepted (time overlap uses min_time/max_time on tags)."""
    # Sample timestamps are ~1000s epoch; keep window around that range.
    data = get_json_from_api(
        "/api/v1/series",
        params=[("start", "500"), ("end", "2000")],
    )
    assert isinstance(data, list)


def test_label_values_percent_encoded_label_name():
    """Label name in the URI may be percent-encoded (Prometheus allows label names that
    include characters requiring escaping after custom remote-write or instrumentation).
    The handler must URL-decode the segment before querying storage."""
    # Ingest a series whose label name is `a/b` (a slash is unusual but allowed for this test).
    time_series = [
        (
            {"__name__": "encoded_label_metric", "a/b": "v1"},
            {2000: 1.0},
        ),
    ]
    protobuf = convert_time_series_to_protobuf(time_series)
    send_protobuf_to_remote_write(node.ip_address, 9093, "/write", protobuf)
    assert_eq_with_retry(
        node,
        "SELECT count() > 0 FROM timeSeriesTags(prometheus) WHERE metric_name = 'encoded_label_metric'",
        "1",
    )

    # Raw `/api/v1/label/a/b/values` is ambiguous; clients must percent-encode the `/`.
    data = get_json_from_api("/api/v1/label/a%2Fb/values")
    assert data == ["v1"], data


def test_label_values_trailing_values_segment_not_confused_with_label_name():
    """`find("/values")` returns the first occurrence; if the label name itself contains
    `/values` the extraction truncates inside the label. Use the trailing `/values` as the
    anchor (via `rfind` / size-based suffix strip) so the full label name is preserved."""
    time_series = [
        (
            {"__name__": "tricky_label_metric", "foo/values_bar": "vv"},
            {2000: 2.0},
        ),
    ]
    protobuf = convert_time_series_to_protobuf(time_series)
    send_protobuf_to_remote_write(node.ip_address, 9093, "/write", protobuf)
    assert_eq_with_retry(
        node,
        "SELECT count() > 0 FROM timeSeriesTags(prometheus) WHERE metric_name = 'tricky_label_metric'",
        "1",
    )

    # Percent-encode the embedded `/` so the server sees one label-name segment.
    data = get_json_from_api("/api/v1/label/foo%2Fvalues_bar/values")
    assert data == ["vv"], data


def test_sql_injection_safe():
    """Malicious-looking label names and match[] must not break the server."""
    bad_label = "foo'; DROP TABLE x;--"
    path = f"/api/v1/label/{urllib.parse.quote(bad_label, safe='')}/values"
    data = get_json_from_api(path)
    assert isinstance(data, list)
    assert data == []

    # Invalid PromQL in match[] returns 400 bad_data (no silent fallback to __name__=...).
    url = f"http://{node.ip_address}:9093/api/v1/series"
    response = requests.get(url, params=[("match[]", "foo' OR '1'='1")])
    assert response.status_code == 400, response.text
    err = response.json()
    assert err["status"] == "error"
    assert err["errorType"] == "bad_data"

