"""Tests for `/api/v1/label/<name>/values` on a tag configured via `tags_to_columns` when a supported
external tags table still carries the tag only in the residual `tags` Map (e.g. rows preloaded before
the `tags_to_columns` layout was adopted), with the dedicated column empty or NULL. `/api/v1/series`
emits such labels straight from the Map and `/api/v1/labels` reports them via the Map keys, so the
label-values endpoint must fall back to the Map as well instead of silently dropping those values."""

import pytest
import requests

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node_residual_map_fallback",
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


def test_label_values_fall_back_to_residual_map():
    """The dedicated column wins when non-empty; when it is NULL or '' the value stored in the
    residual `tags` Map must still be reported."""
    data = get_json_from_api("/api/v1/label/host/values")
    assert data == ["legacy_empty", "legacy_null", "server1"], f"Unexpected values: {data}"


def test_label_values_fallback_respects_match():
    """The fallback composes with a `match[]` selector on another label."""
    data = get_json_from_api("/api/v1/label/host/values", params={"match[]": 'cpu_usage{instance="i2"}'})
    assert data == ["legacy_null"], f"Unexpected values: {data}"


def test_series_and_labels_stay_consistent_with_label_values():
    """/api/v1/series reports the legacy rows' `host` from the Map and /api/v1/labels lists `host`
    for them, so the label-values fallback keeps the three endpoints consistent."""
    data = get_json_from_api("/api/v1/series", params={"match[]": "cpu_usage"})
    hosts = {entry["instance"]: entry.get("host") for entry in data}
    assert hosts == {"i1": "server1", "i2": "legacy_null", "i3": "legacy_empty"}, f"Unexpected series: {data}"
    data = get_json_from_api("/api/v1/labels")
    assert "host" in data, f"Unexpected labels: {data}"
