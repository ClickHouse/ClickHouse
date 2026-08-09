"""Tests that `/api/v1/series` deduplicates by the logical Prometheus label set, not by the raw
`tags`-table layout. A supported external tags table can store a `tags_to_columns` tag in two
carriers: the dedicated column and the residual `tags` Map (e.g. legacy rows written before the
dedicated column was adopted). The same logical series stored once per carrier must be returned
once, a single row carrying the same value in both carriers must emit the label key once, and a
row with conflicting values in the two carriers must be rejected with `bad_data`, following the
same normalization rules as `timeSeriesStoreTags` on the write path."""

import pytest
import requests

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node_mixed_carrier_dedup",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
)


def request_api(path, params=None):
    url = f"http://{node.ip_address}:9093{path}"
    print(f"Requesting {url} with params {params}")
    response = requests.get(url, params=params)
    print(f"Status code: {response.status_code}, Body: {response.text[:500]}")
    return response


def get_series(match):
    response = request_api("/api/v1/series", params={"match[]": match})
    assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "success", f"Expected success, got: {data}"
    return data["data"]


@pytest.fixture(scope="module", autouse=True)
def setup(request):
    try:
        cluster.start()
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
        # `cpu_usage{host="server1",instance="i1"}` is stored twice, once per carrier layout:
        # a legacy row with `host` only in the residual Map and a migrated row with `host` only in
        # the dedicated column. `mem_usage` is a single mixed row carrying the same `host` value in
        # both carriers at once. `conflicting_metric` carries two different `host` values.
        node.query(
            "INSERT INTO prometheus_tags VALUES"
            " ('00000000-0000-0000-0000-000000000001', 'cpu_usage', NULL, {'instance':'i1', 'host':'server1'},"
            "  toDateTime64(1700000000, 3, 'UTC'), toDateTime64(1700000000, 3, 'UTC')),"
            " ('00000000-0000-0000-0000-000000000002', 'cpu_usage', 'server1', {'instance':'i1'},"
            "  toDateTime64(1700000000, 3, 'UTC'), toDateTime64(1700000000, 3, 'UTC')),"
            " ('00000000-0000-0000-0000-000000000003', 'mem_usage', 'server2', {'instance':'i2', 'host':'server2'},"
            "  toDateTime64(1700000000, 3, 'UTC'), toDateTime64(1700000000, 3, 'UTC')),"
            " ('00000000-0000-0000-0000-000000000004', 'conflicting_metric', 'column_host', {'host':'map_host'},"
            "  toDateTime64(1700000000, 3, 'UTC'), toDateTime64(1700000000, 3, 'UTC'))"
        )
        node.query(
            "INSERT INTO prometheus_data VALUES"
            " ('00000000-0000-0000-0000-000000000001', toDateTime64(1700000000, 3, 'UTC'), 1),"
            " ('00000000-0000-0000-0000-000000000002', toDateTime64(1700000000, 3, 'UTC'), 2),"
            " ('00000000-0000-0000-0000-000000000003', toDateTime64(1700000000, 3, 'UTC'), 3),"
            " ('00000000-0000-0000-0000-000000000004', toDateTime64(1700000000, 3, 'UTC'), 4)"
        )
        yield cluster
    finally:
        cluster.shutdown()


def test_mixed_carrier_rows_collapse_to_one_series():
    """The same logical series stored once per carrier layout is returned exactly once."""
    data = get_series("cpu_usage")
    assert data == [{"__name__": "cpu_usage", "host": "server1", "instance": "i1"}], f"Unexpected series: {data}"


def test_single_row_with_both_carriers_emits_label_once():
    """A row carrying the same value in the Map and in the dedicated column emits `host` once."""
    response = request_api("/api/v1/series", params={"match[]": "mem_usage"})
    assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
    assert response.text.count('"host"') == 1, f"Unexpected body: {response.text}"
    data = response.json()
    assert data["data"] == [{"__name__": "mem_usage", "host": "server2", "instance": "i2"}], f"Unexpected series: {data}"


def test_conflicting_carriers_are_rejected():
    """A row with different `host` values in the Map and in the dedicated column fails with
    `bad_data`, like `timeSeriesStoreTags` on the write path, instead of emitting an invalid
    series with a repeated label key."""
    response = request_api("/api/v1/series", params={"match[]": "conflicting_metric"})
    assert response.status_code == 400, f"Expected 400, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "error", f"Unexpected body: {data}"
    assert data["errorType"] == "bad_data", f"Unexpected body: {data}"
    assert "host" in data["error"], f"Unexpected error message: {data}"
