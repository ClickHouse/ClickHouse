"""Tests for two Prometheus-compatibility behaviors of the metadata endpoints:

1. `/api/v1/label/<name>/values` decodes Prometheus' label-name escaping, so tag names that are not
   legacy Prometheus names (dotted, slashed, ...) are queryable through their escaped `U__...` form.
2. The `match[]` parameter is only supported as a bare metric name so far; a full series selector with
   label matchers is rejected with a clear error instead of being treated as a literal metric name and
   silently returning the wrong metadata.
3. Prometheus allows `match[]` to be repeated; the result is the union over all the given metric names,
   and a repeated value must not be silently dropped.
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


def test_series_selector_with_matchers_rejected():
    """A full series selector in `match[]` must be rejected with a clear error rather than being
    treated as a metric literally named `cpu_usage{host="server1"}`."""
    for path in (
        '/api/v1/series?match[]=cpu_usage{host="server1"}',
        '/api/v1/labels?match[]=cpu_usage{host="server1"}',
        '/api/v1/label/host/values?match[]=cpu_usage{host="server1"}',
    ):
        url = f"http://{node.ip_address}:9093{path}"
        response = requests.get(url)
        assert response.status_code == 400, f"{path}: expected 400, got {response.status_code}: {response.text}"
        data = response.json()
        assert data["status"] == "error", f"{path}: expected error status, got: {data}"
        assert "match[]" in data["error"], f"{path}: unexpected error message: {data}"


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


def test_selector_anywhere_in_repeated_match_rejected():
    """A full series selector must be rejected even when it is not the first `match[]` value."""
    url = f'http://{node.ip_address}:9093/api/v1/series?match[]=cpu_usage&match[]=memory_usage{{host="server1"}}'
    response = requests.get(url)
    assert response.status_code == 400, f"Expected 400, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "error", f"Expected error status, got: {data}"
    assert "match[]" in data["error"], f"Unexpected error message: {data}"
