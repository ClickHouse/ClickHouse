"""Tests for Prometheus-compatibility behaviors of the metadata endpoints:

1. `/api/v1/label/<name>/values` decodes Prometheus' label-name escaping, so tag names that are not
   legacy Prometheus names (dotted, slashed, ...) are queryable through their escaped `U__...` form.
2. The `match[]` parameter is a full Prometheus series selector: a bare metric name or an instant
   selector with `=`, `!=`, `=~`, and `!~` label matchers, filtering the series set exactly like the
   PromQL query endpoints do (Grafana emits the selector forms to narrow label names / label values).
   Non-legacy (UTF-8) label names are written in a selector as quoted string literals, e.g.
   {"http.status_code"="200"}, covering the same label surface as the escaped `U__...` form.
3. Prometheus allows `match[]` to be repeated; the result is the union of the series matched by each
   selector, and a repeated value must not be silently dropped.
4. An explicitly empty `match[]` value, an unparsable selector, a PromQL expression that is not an
   instant selector, and the empty selector `{}` are rejected (Prometheus fails to parse them) instead
   of being silently dropped, which would fall back to unfiltered or partially filtered metadata.
"""

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
    "node_escaping_selectors",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
    handle_prometheus_remote_write=(9093, "/write"),
)


def send_test_data():
    """Write series carrying non-legacy tag names (a dot and a slash) alongside a legacy one."""
    time_series = [
        (
            {
                "__name__": "cpu_usage",
                "host": "server1",
                "http.status_code": "200",
                "path/segment": "a",
            },
            {1000: 0.5},
        ),
        (
            {
                "__name__": "cpu_usage",
                "host": "server2",
                "http.status_code": "500",
                "path/segment": "b",
            },
            {1000: 0.3},
        ),
        (
            {
                "__name__": "memory_usage",
                "host": "server1",
            },
            {1000: 0.8},
        ),
    ]
    protobuf = convert_time_series_to_protobuf(time_series)
    send_protobuf_to_remote_write(node.ip_address, 9093, "/write", protobuf)


def get_json_from_api(path):
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
        assert_eq_with_retry(
            node, "SELECT count() > 0 FROM timeSeriesData(prometheus)", "1"
        )
        yield cluster
    finally:
        cluster.shutdown()


def test_label_values_decodes_dotted_name():
    """`http.status_code` is requested through its escaped form `U__http_2e_status__code`."""
    data = get_json_from_api("/api/v1/label/U__http_2e_status__code/values")
    assert set(data) == {"200", "500"}, f"Unexpected values: {data}"


def test_label_values_decodes_slashed_name():
    """`path/segment` is requested through its escaped form `U__path_2f_segment`."""
    data = get_json_from_api("/api/v1/label/U__path_2f_segment/values")
    assert set(data) == {"a", "b"}, f"Unexpected values: {data}"


def test_label_values_legacy_name_unchanged():
    """A legacy label name is not escaped by Prometheus and must keep working verbatim."""
    data = get_json_from_api("/api/v1/label/host/values")
    assert set(data) == {"server1", "server2"}, f"Unexpected values: {data}"


def test_series_selector_with_label_matcher():
    """A full series selector in `match[]` filters by both the metric name and the label matchers."""
    data = get_json_from_api('/api/v1/series?match[]=cpu_usage{host="server1"}')
    assert len(data) == 1, f"Expected 1 series, got: {data}"
    assert data[0]["__name__"] == "cpu_usage" and data[0]["host"] == "server1", f"Unexpected series: {data}"


def test_series_selector_without_metric_name():
    """A selector with label matchers only (no metric name) matches series of any metric."""
    data = get_json_from_api('/api/v1/series?match[]={host="server1"}')
    names = sorted(series["__name__"] for series in data)
    assert names == ["cpu_usage", "memory_usage"], f"Unexpected series: {data}"


def test_series_selector_negative_and_regexp_matchers():
    """The `!=`, `=~`, and `!~` matcher forms translate to the same filters as the query endpoints."""
    data = get_json_from_api('/api/v1/series?match[]=cpu_usage{host!="server1"}')
    assert len(data) == 1 and data[0]["host"] == "server2", f"Unexpected series: {data}"

    data = get_json_from_api('/api/v1/series?match[]={host=~"server[12]"}')
    assert len(data) == 3, f"Expected all 3 series, got: {data}"

    data = get_json_from_api('/api/v1/series?match[]=cpu_usage{host!~"server1"}')
    assert len(data) == 1 and data[0]["host"] == "server2", f"Unexpected series: {data}"

    # Prometheus regexps are anchored on both sides: "server" must not match "server1".
    data = get_json_from_api('/api/v1/series?match[]={host=~"server"}')
    assert data == [], f"Expected no series for the anchored regexp, got: {data}"


def test_series_selector_on_metric_name_matcher():
    """The metric name is matchable as the `__name__` label, including with a regexp."""
    data = get_json_from_api('/api/v1/series?match[]={__name__=~"cpu_.*"}')
    assert len(data) == 2, f"Expected 2 cpu_usage series, got: {data}"


def test_labels_narrowed_by_selector():
    """`/api/v1/labels` reports only the label names of the series matched by the selector."""
    data = get_json_from_api('/api/v1/labels?match[]={host="server1"}')
    assert set(data) == {"__name__", "host", "http.status_code", "path/segment"}, f"Unexpected labels: {data}"

    data = get_json_from_api("/api/v1/labels?match[]=memory_usage")
    assert set(data) == {"__name__", "host"}, f"Unexpected labels: {data}"


def test_label_values_narrowed_by_selector():
    """`/api/v1/label/<name>/values` honors a selector with label matchers, including for an
    escaped non-legacy label name in the path."""
    data = get_json_from_api('/api/v1/label/host/values?match[]=cpu_usage{host!="server2"}')
    assert data == ["server1"], f"Unexpected values: {data}"

    data = get_json_from_api('/api/v1/label/U__http_2e_status__code/values?match[]={host="server2"}')
    assert data == ["500"], f"Unexpected values: {data}"


def test_selector_with_quoted_label_name():
    """A non-legacy (UTF-8) label name is written in a selector as a quoted string literal,
    e.g. {"http.status_code"="200"}, and must filter the same label surface that the escaped
    `U__...` form of `/api/v1/label/<name>/values` exposes."""
    data = get_json_from_api('/api/v1/series?match[]={"http.status_code"="200"}')
    assert len(data) == 1, f"Expected 1 series, got: {data}"
    assert data[0]["host"] == "server1", f"Unexpected series: {data}"

    data = get_json_from_api('/api/v1/series?match[]=cpu_usage{"path/segment"!="a"}')
    assert len(data) == 1 and data[0]["host"] == "server2", f"Unexpected series: {data}"

    data = get_json_from_api('/api/v1/series?match[]={"http.status_code"=~"[45]00"}')
    assert len(data) == 1 and data[0]["host"] == "server2", f"Unexpected series: {data}"


def test_labels_narrowed_by_quoted_label_name_selector():
    data = get_json_from_api('/api/v1/labels?match[]={"http.status_code"="500"}')
    assert set(data) == {"__name__", "host", "http.status_code", "path/segment"}, f"Unexpected labels: {data}"


def test_label_values_narrowed_by_quoted_label_name_selector():
    data = get_json_from_api('/api/v1/label/host/values?match[]={"http.status_code"="200"}')
    assert data == ["server1"], f"Unexpected values: {data}"

    data = get_json_from_api(
        '/api/v1/label/U__http_2e_status__code/values?match[]={"path/segment"="b"}'
    )
    assert data == ["500"], f"Unexpected values: {data}"


def test_empty_quoted_label_name_is_rejected():
    """Prometheus rejects an empty label name in a selector with a parse error."""
    url = f'http://{node.ip_address}:9093/api/v1/series?match[]={{""="x"}}'
    response = requests.get(url)
    assert response.status_code == 400, f"Expected 400, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "error", f"Expected error status, got: {data}"
    assert "match[]" in data["error"], f"Unexpected error message: {data}"


def test_bare_metric_name_match_still_works():
    """A bare metric name in `match[]` keeps filtering the series set."""
    data = get_json_from_api("/api/v1/series?match[]=cpu_usage")
    assert len(data) == 2, f"Expected 2 cpu_usage series, got: {data}"


def test_repeated_match_returns_union_of_series():
    """Prometheus allows `match[]` to be repeated: the result is the union of the matched series,
    so the second value must not be silently dropped."""
    data = get_json_from_api("/api/v1/series?match[]=cpu_usage&match[]=memory_usage")
    names = sorted(series["__name__"] for series in data)
    assert names == ["cpu_usage", "cpu_usage", "memory_usage"], f"Unexpected series: {data}"


def test_repeated_match_returns_union_of_labels():
    data = get_json_from_api("/api/v1/labels?match[]=cpu_usage&match[]=memory_usage")
    assert set(data) == {"__name__", "host", "http.status_code", "path/segment"}, f"Unexpected labels: {data}"


def test_repeated_match_returns_union_of_label_values():
    data = get_json_from_api("/api/v1/label/host/values?match[]=memory_usage")
    assert data == ["server1"], f"Unexpected values: {data}"

    data = get_json_from_api("/api/v1/label/host/values?match[]=cpu_usage&match[]=memory_usage")
    assert set(data) == {"server1", "server2"}, f"Unexpected values: {data}"


def test_repeated_match_with_selector_returns_union():
    """The union semantics of a repeated `match[]` also applies to selector-shaped values."""
    data = get_json_from_api(
        '/api/v1/series?match[]=cpu_usage{host="server2"}&match[]=memory_usage'
    )
    names = sorted((series["__name__"], series["host"]) for series in data)
    assert names == [("cpu_usage", "server2"), ("memory_usage", "server1")], f"Unexpected series: {data}"


def test_invalid_selector_is_rejected():
    """An unparsable selector, a PromQL expression that is not an instant selector, and the empty
    selector `{}` are rejected with a clear error (fail closed), as in Prometheus, even when another
    `match[]` value in the list is valid."""
    for path in (
        "/api/v1/series?match[]=cpu_usage{",
        '/api/v1/labels?match[]=cpu_usage{host="server1"',
        "/api/v1/label/host/values?match[]=cpu_usage{",
        "/api/v1/series?match[]=cpu_usage[5m]",
        "/api/v1/series?match[]=rate(cpu_usage[5m])",
        "/api/v1/series?match[]={}",
        "/api/v1/series?match[]=cpu_usage&match[]={}",
    ):
        url = f"http://{node.ip_address}:9093{path}"
        response = requests.get(url)
        assert response.status_code == 400, f"{path}: expected 400, got {response.status_code}: {response.text}"
        data = response.json()
        assert data["status"] == "error", f"{path}: expected error status, got: {data}"
        assert "match[]" in data["error"], f"{path}: unexpected error message: {data}"


def test_empty_match_value_is_rejected():
    """An explicitly empty `match[]` value (`?match[]=`) is not a valid series selector: Prometheus
    rejects it with a parse error instead of silently dropping it, so all three metadata endpoints
    must return a 400 instead of falling back to unfiltered metadata."""
    for path in (
        "/api/v1/series?match[]=",
        "/api/v1/labels?match[]=",
        "/api/v1/label/host/values?match[]=",
    ):
        url = f"http://{node.ip_address}:9093{path}"
        response = requests.get(url)
        assert response.status_code == 400, f"{path}: expected 400, got {response.status_code}: {response.text}"
        data = response.json()
        assert data["status"] == "error", f"{path}: expected error status, got: {data}"
        assert "match[]" in data["error"], f"{path}: unexpected error message: {data}"


def test_empty_match_value_mixed_with_valid_is_rejected():
    """An empty `match[]` value must be rejected even when another value in the list is a valid bare
    metric name; otherwise the request would fail open and return partially filtered metadata."""
    for path in (
        "/api/v1/series?match[]=&match[]=cpu_usage",
        "/api/v1/labels?match[]=&match[]=cpu_usage",
        "/api/v1/label/host/values?match[]=&match[]=cpu_usage",
        "/api/v1/series?match[]=cpu_usage&match[]=",
        "/api/v1/labels?match[]=cpu_usage&match[]=",
        "/api/v1/label/host/values?match[]=cpu_usage&match[]=",
    ):
        url = f"http://{node.ip_address}:9093{path}"
        response = requests.get(url)
        assert response.status_code == 400, f"{path}: expected 400, got {response.status_code}: {response.text}"
        data = response.json()
        assert data["status"] == "error", f"{path}: expected error status, got: {data}"
        assert "match[]" in data["error"], f"{path}: unexpected error message: {data}"
