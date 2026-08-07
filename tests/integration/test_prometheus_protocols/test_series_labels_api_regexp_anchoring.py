"""Regression tests for the anchoring of `=~` / `!~` matchers in the `match[]` parameter of the
Prometheus metadata endpoints.

Prometheus regexp matchers are fully anchored: the pattern must match the whole label value, as if
it were wrapped in `^(?:...)$`. Since ClickHouse's `match` function performs an unanchored search,
the translation must wrap the pattern in a non-capturing group before adding the anchors; otherwise
a top-level alternation binds the anchors to its first and last branches only, so `host=~"server1|server2"`
would become `match(host, '^server1|server2$')`, which also matches `server10` and `xserver2`.
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
    "node_regexp_anchoring",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
    handle_prometheus_remote_write=(9093, "/write"),
)


def send_test_data():
    """`server10` extends the last alternation branch and `xserver2` extends the first one:
    both match the misanchored pattern `^server1|server2$` but not the correct `^(?:server1|server2)$`."""
    time_series = [
        ({"__name__": "cpu_usage", "host": "server1"}, {1000: 0.1}),
        ({"__name__": "cpu_usage", "host": "server2"}, {1000: 0.2}),
        ({"__name__": "cpu_usage", "host": "server10"}, {1000: 0.3}),
        ({"__name__": "cpu_usage", "host": "xserver2"}, {1000: 0.4}),
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


def test_alternation_regexp_is_fully_anchored():
    """`host=~"server1|server2"` must match exactly `server1` and `server2`."""
    data = get_json_from_api('/api/v1/series?match[]={host=~"server1|server2"}')
    hosts = sorted(series["host"] for series in data)
    assert hosts == ["server1", "server2"], f"Unexpected series: {data}"


def test_negative_alternation_regexp_is_fully_anchored():
    """`host!~"server1|server2"` must exclude exactly `server1` and `server2`."""
    data = get_json_from_api('/api/v1/series?match[]={host!~"server1|server2"}')
    hosts = sorted(series["host"] for series in data)
    assert hosts == ["server10", "xserver2"], f"Unexpected series: {data}"


def test_alternation_regexp_anchors_label_values():
    """The same anchoring applies to `/api/v1/label/<name>/values` and `/api/v1/labels`."""
    data = get_json_from_api('/api/v1/label/host/values?match[]={host=~"server1|server2"}')
    assert set(data) == {"server1", "server2"}, f"Unexpected values: {data}"


def test_explicitly_anchored_regexp_keeps_working():
    """A pattern that already carries its own anchors stays correct after the wrapping."""
    data = get_json_from_api('/api/v1/series?match[]={host=~"^server1$"}')
    hosts = sorted(series["host"] for series in data)
    assert hosts == ["server1"], f"Unexpected series: {data}"
