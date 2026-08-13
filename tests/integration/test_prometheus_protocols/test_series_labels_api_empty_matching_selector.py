"""Tests for the rejection of `match[]` selectors whose matchers all match the empty label value.

Prometheus rejects a series selector in which every matcher matches the empty string (a missing
label is equal to the empty label value), e.g. `{job=~".*"}`, because such a selector does not
narrow the set of series at all. This keeps the "`match[]` is required" guard on `/api/v1/series`
meaningful: without this rule `match[]={job=~".*"}` would degenerate into a full scan of the tags
table. At least one matcher of each selector must not match the empty string.
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
    "node_empty_matching_selector",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
    handle_prometheus_remote_write=(9093, "/write"),
)


def send_test_data():
    time_series = [
        ({"__name__": "cpu_usage", "host": "server1"}, {1000: 0.5}),
        ({"__name__": "cpu_usage", "host": "server2"}, {1000: 0.3}),
        ({"__name__": "memory_usage", "host": "server1"}, {1000: 0.8}),
    ]
    protobuf = convert_time_series_to_protobuf(time_series)
    send_protobuf_to_remote_write(node.ip_address, 9093, "/write", protobuf)


def get_response(path, params=None):
    url = f"http://{node.ip_address}:9093{path}"
    print(f"Requesting {url} with params {params}")
    response = requests.get(url, params=params)
    print(f"Status code: {response.status_code}, Body: {response.text[:500]}")
    return response


def assert_rejected(path, match):
    response = get_response(path, params={"match[]": match})
    assert response.status_code == 400, f"{path} {match}: expected 400, got {response.status_code}: {response.text}"
    result = response.json()
    assert result["status"] == "error", f"{path} {match}: expected an error, got: {result}"
    assert "at least one matcher" in result["error"], f"{path} {match}: unexpected error message: {result}"


def assert_accepted(path, match):
    response = get_response(path, params={"match[]": match})
    assert response.status_code == 200, f"{path} {match}: expected 200, got {response.status_code}: {response.text}"
    result = response.json()
    assert result["status"] == "success", f"{path} {match}: expected success, got: {result}"
    return result["data"]


@pytest.fixture(scope="module", autouse=True)
def setup(request):
    try:
        cluster.start()
        node.query("CREATE TABLE prometheus ENGINE=TimeSeries")
        send_test_data()
        assert_eq_with_retry(node, "SELECT count() > 0 FROM timeSeriesData(prometheus)", "1")
        yield cluster
    finally:
        cluster.shutdown()


def test_selectors_matching_the_empty_label_value_are_rejected():
    """Every matcher of each of these selectors matches the empty label value, so the selector
    selects every series (a full scan) and must be rejected with `bad_data`, as in Prometheus."""
    for match in [
        '{job=~".*"}',
        '{host=~".*"}',
        '{host=""}',
        '{host!="server1"}',
        '{host!~".+"}',
        '{host=~"server1|"}',
        '{host="", instance!="i1"}',
    ]:
        for path in ["/api/v1/series", "/api/v1/labels", "/api/v1/label/host/values"]:
            assert_rejected(path, match)


def test_selectors_with_a_non_empty_matcher_are_accepted():
    """One matcher that cannot match the empty label value makes the selector valid, even when the
    other matchers can: this is the exact Prometheus rule, not a blanket ban on `=~".*"`."""
    data = assert_accepted("/api/v1/series", 'cpu_usage{host=~".*"}')
    assert len(data) == 2, f"Unexpected series: {data}"
    data = assert_accepted("/api/v1/series", '{__name__=~".+", host=~".*"}')
    assert len(data) == 3, f"Unexpected series: {data}"
    data = assert_accepted("/api/v1/series", '{host!~""}')
    assert len(data) == 3, f"Unexpected series: {data}"
    data = assert_accepted("/api/v1/label/host/values", '{host=~".+"}')
    assert data == ["server1", "server2"], f"Unexpected values: {data}"


def test_one_bad_selector_rejects_the_whole_request():
    """`match[]` is a union of selectors, but each value must be valid on its own: a non-narrowing
    selector is rejected even when another value already narrows the series set."""
    response = get_response("/api/v1/series", params={"match[]": ["cpu_usage", '{host=~".*"}']})
    assert response.status_code == 400, f"Expected 400, got {response.status_code}: {response.text}"
    result = response.json()
    assert result["status"] == "error", f"Expected an error, got: {result}"


def test_invalid_regexp_is_rejected():
    """An unparsable regexp anywhere in the selector fails with `bad_data` at parse time, even when
    another matcher already narrows the series set."""
    for match in ['{host=~"["}', 'cpu_usage{host=~"["}', 'cpu_usage{host!~"("}']:
        response = get_response("/api/v1/series", params={"match[]": match})
        assert response.status_code == 400, f"{match}: expected 400, got {response.status_code}: {response.text}"
        result = response.json()
        assert result["status"] == "error", f"{match}: expected an error, got: {result}"
        assert "regexp" in result["error"], f"{match}: unexpected error message: {result}"
