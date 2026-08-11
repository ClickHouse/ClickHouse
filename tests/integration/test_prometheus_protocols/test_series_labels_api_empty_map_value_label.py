"""Tests for `/api/v1/labels` on a supported external tags table whose residual `tags` Map contains
entries with an empty value, e.g. `tags = {'env': ''}`. The write path strips such entries and the
rest of the implementation treats an empty label value as "label absent" (`/api/v1/series` drops it,
matcher translation treats it as the missing/empty case, `/api/v1/label/<name>/values` filters it),
so `/api/v1/labels` must not surface the key of an empty-valued map entry either - neither as a
returned label name nor as an item counted toward `limit`."""

import pytest
import requests

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node_empty_map_value_label",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
)

TRUNCATION_WARNING = "results truncated due to limit"


def get_result_from_api(path, params=None):
    """Make a GET request to the ClickHouse Prometheus API and return the whole JSON response."""
    url = f"http://{node.ip_address}:9093{path}"
    print(f"Requesting {url} with params {params}")
    response = requests.get(url, params=params)
    print(f"Status code: {response.status_code}, Body: {response.text[:500]}")
    assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
    result = response.json()
    assert result["status"] == "success", f"Expected success, got: {result}"
    return result


@pytest.fixture(scope="module", autouse=True)
def setup(request):
    try:
        cluster.start()
        # A supported external tags table without `tags_to_columns`: all labels live in the residual
        # `tags` Map, and two rows carry map entries with an empty value ('env' and 'zone').
        node.query("CREATE TABLE prometheus_data (id UUID, timestamp DateTime64(3, 'UTC'), value Float64) ENGINE = MergeTree ORDER BY (id, timestamp)")
        node.query(
            "CREATE TABLE prometheus_tags (id UUID, metric_name LowCardinality(String),"
            " tags Map(LowCardinality(String), String),"
            " min_time SimpleAggregateFunction(min, Nullable(DateTime64(3, 'UTC'))),"
            " max_time SimpleAggregateFunction(max, Nullable(DateTime64(3, 'UTC'))))"
            " ENGINE = AggregatingMergeTree ORDER BY (metric_name, id)"
            " SETTINGS allow_dimensions_outside_sorting_key = 1"
        )
        node.query("CREATE TABLE prometheus_metrics (metric_family_name String, type String, unit String, help String) ENGINE = ReplacingMergeTree ORDER BY metric_family_name")
        node.query("CREATE TABLE prometheus ENGINE = TimeSeries DATA prometheus_data TAGS prometheus_tags METRICS prometheus_metrics")
        node.query(
            "INSERT INTO prometheus_tags VALUES"
            " ('00000000-0000-0000-0000-000000000001', 'cpu_usage', {'instance':'i1', 'env':''},"
            "  toDateTime64(1700000000, 3, 'UTC'), toDateTime64(1700000000, 3, 'UTC')),"
            " ('00000000-0000-0000-0000-000000000002', 'cpu_usage', {'instance':'i2', 'region':'eu'},"
            "  toDateTime64(1700000000, 3, 'UTC'), toDateTime64(1700000000, 3, 'UTC')),"
            " ('00000000-0000-0000-0000-000000000003', 'mem_usage', {'zone':''},"
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


def test_labels_skip_empty_valued_map_entries():
    """A map entry with an empty value means the label is absent, so its key must not be listed."""
    result = get_result_from_api("/api/v1/labels")
    assert result["data"] == ["__name__", "instance", "region"], f"Unexpected labels: {result}"


def test_labels_series_with_only_empty_valued_tags_still_signals_name():
    """A matching series whose every map entry has an empty value carries only __name__; it must
    still make the virtual __name__ label appear, but nothing else."""
    result = get_result_from_api("/api/v1/labels", params={"match[]": "mem_usage"})
    assert result["data"] == ["__name__"], f"Unexpected labels: {result}"


def test_labels_empty_valued_map_entry_does_not_count_toward_limit():
    """With the spurious keys filtered out, limit=3 fits the whole logical label set, so the result
    must be complete and not flagged as truncated; limit=2 must truncate to the first two."""
    result = get_result_from_api("/api/v1/labels", params={"limit": "3"})
    assert result["data"] == ["__name__", "instance", "region"], f"Unexpected labels: {result}"
    assert "warnings" not in result, f"Unexpected truncation: {result}"

    result = get_result_from_api("/api/v1/labels", params={"limit": "2"})
    assert result["data"] == ["__name__", "instance"], f"Unexpected labels: {result}"
    assert TRUNCATION_WARNING in result.get("warnings", []), f"Expected truncation: {result}"


def test_series_stays_consistent_with_labels():
    """/api/v1/series drops empty-valued map entries from the emitted label sets, so the labels
    endpoint now reports exactly the union of the keys /series emits."""
    result = get_result_from_api("/api/v1/series", params={"match[]": '{__name__=~".+"}'})
    emitted_keys = set()
    for entry in result["data"]:
        emitted_keys.update(entry.keys())
    assert emitted_keys == {"__name__", "instance", "region"}, f"Unexpected series: {result}"
