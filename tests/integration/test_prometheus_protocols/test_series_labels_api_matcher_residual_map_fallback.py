"""Tests that `match[]`/`query` label matchers on a tag configured via `tags_to_columns` see the
value stored in the residual `tags` Map when the dedicated column is NULL or empty (e.g. legacy rows
of a supported external tags table written before the `tags_to_columns` layout was adopted).
`/api/v1/series` emits such labels from the Map and `/api/v1/label/<name>/values` falls back to the
Map, so selectors filtering on the label itself must resolve it the same way instead of silently
dropping those rows."""

import pytest
import requests

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node_matcher_residual_map_fallback",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
)


def get_json_from_api(path, params=None):
    """Make a GET request to the ClickHouse Prometheus API and return parsed JSON."""
    url = f"http://{node.ip_address}:9093{path}"
    print(f"Requesting {url} with params {params}")
    response = requests.get(url, params=params)
    print(f"Status code: {response.status_code}, Body: {response.text[:500]}")
    assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "success", f"Expected success, got: {data}"
    return data["data"]


def get_series_hosts(match):
    """Return the set of `host` label values of the series matching the given selector."""
    data = get_json_from_api("/api/v1/series", params={"match[]": match})
    return {entry.get("host") for entry in data}


@pytest.fixture(scope="module", autouse=True)
def setup(request):
    try:
        cluster.start()
        # The external tags table declares a dedicated `host` tag column, but two "legacy" rows carry
        # the `host` tag only in the residual `tags` Map: one with the column NULL and one with it ''.
        node.query("CREATE TABLE prometheus_data (id UUID, timestamp DateTime64(3, 'UTC'), value Float64) ENGINE = MergeTree ORDER BY (id, timestamp)")
        node.query(
            "CREATE TABLE prometheus_tags (id UUID, metric_name LowCardinality(String),"
            " host LowCardinality(Nullable(String)),"
            " tags Map(LowCardinality(String), String),"
            " min_time SimpleAggregateFunction(min, Nullable(DateTime64(3, 'UTC'))),"
            " max_time SimpleAggregateFunction(max, Nullable(DateTime64(3, 'UTC'))))"
            " ENGINE = AggregatingMergeTree ORDER BY (metric_name, id)"
            " SETTINGS allow_dimensions_outside_sorting_key = 1"
        )
        node.query("CREATE TABLE prometheus_metrics (metric_family_name String, type String, unit String, help String) ENGINE = ReplacingMergeTree ORDER BY metric_family_name")
        node.query("CREATE TABLE prometheus ENGINE = TimeSeries SETTINGS tags_to_columns = {'host': 'host'} DATA prometheus_data TAGS prometheus_tags METRICS prometheus_metrics")
        node.query(
            "INSERT INTO prometheus_tags VALUES"
            " ('00000000-0000-0000-0000-000000000001', 'cpu_usage', 'server1', {'instance':'i1'},"
            "  toDateTime64(1700000000, 3, 'UTC'), toDateTime64(1700000000, 3, 'UTC')),"
            " ('00000000-0000-0000-0000-000000000002', 'cpu_usage', NULL, {'instance':'i2', 'host':'legacy_null'},"
            "  toDateTime64(1700000000, 3, 'UTC'), toDateTime64(1700000000, 3, 'UTC')),"
            " ('00000000-0000-0000-0000-000000000003', 'cpu_usage', '', {'instance':'i3', 'host':'legacy_empty'},"
            "  toDateTime64(1700000000, 3, 'UTC'), toDateTime64(1700000000, 3, 'UTC'))"
        )
        node.query(
            "INSERT INTO prometheus_data VALUES"
            " ('00000000-0000-0000-0000-000000000001', toDateTime64(1700000000, 3, 'UTC'), 1),"
            " ('00000000-0000-0000-0000-000000000002', toDateTime64(1700000000, 3, 'UTC'), 2),"
            " ('00000000-0000-0000-0000-000000000003', toDateTime64(1700000000, 3, 'UTC'), 3)"
        )
        yield cluster
    finally:
        cluster.shutdown()


def test_series_eq_matcher_on_fallback_label():
    """An equality matcher on the configured label itself selects a row whose value lives only in
    the residual Map, and the dedicated-column row keeps working."""
    assert get_series_hosts('{host="legacy_null"}') == {"legacy_null"}
    assert get_series_hosts('cpu_usage{host="legacy_empty"}') == {"legacy_empty"}
    assert get_series_hosts('{host="server1"}') == {"server1"}


def test_series_ne_and_regexp_matchers_on_fallback_label():
    """Negative and regexp matchers see the Map value too."""
    assert get_series_hosts('cpu_usage{host!="server1"}') == {"legacy_null", "legacy_empty"}
    assert get_series_hosts('cpu_usage{host=~"legacy.*"}') == {"legacy_null", "legacy_empty"}
    assert get_series_hosts('cpu_usage{host!~"legacy.*"}') == {"server1"}


def test_query_endpoint_matcher_on_fallback_label():
    """The same selector translation drives /api/v1/query, so an instant query filtering on the
    configured label returns the legacy row's sample."""
    data = get_json_from_api("/api/v1/query", params={"query": 'cpu_usage{host="legacy_null"}', "time": "1700000000"})
    result = data["result"]
    assert len(result) == 1, f"Unexpected result: {result}"
    assert result[0]["metric"].get("instance") == "i2"
    assert result[0]["value"][1] == "2"


def test_labels_and_label_values_with_fallback_label_matcher():
    """/api/v1/labels and /api/v1/label/<name>/values accept a match[] selector on the fallback
    label itself."""
    data = get_json_from_api("/api/v1/labels", params={"match[]": '{host="legacy_null"}'})
    assert data == ["__name__", "host", "instance"], f"Unexpected labels: {data}"
    data = get_json_from_api("/api/v1/label/instance/values", params={"match[]": '{host="legacy_null"}'})
    assert data == ["i2"], f"Unexpected values: {data}"
