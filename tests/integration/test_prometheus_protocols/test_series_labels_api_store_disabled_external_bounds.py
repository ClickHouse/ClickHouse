"""Tests for the Prometheus metadata endpoints when `store_min_time_and_max_time = 0` while the
external `tags` table still physically has `min_time`/`max_time` columns.

`StorageTimeSeriesSelector::readImpl` gates the tags-table time prefilter on
`filter_by_min_time_and_max_time` AND `store_min_time_and_max_time`. Disabling only
`store_min_time_and_max_time` leaves `filter_by_min_time_and_max_time` reading as `true`
(`checkTimeSeriesSettings` rejects only an explicitly enabled conflict), and a supported external
`tags` table may still carry physical `min_time`/`max_time` columns (`normalizeTimeSeriesDefinition`
requires them only when the setting is enabled, it does not forbid them otherwise). Those columns are
then not maintained by the write path, so the metadata endpoints must reject `start`/`end` based on
the setting alone - before consulting the table schema - instead of silently filtering by stale or
preexisting bounds that `/api/v1/query` and `/api/v1/query_range` ignore."""

import pytest
import requests

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node_store_disabled_external_bounds",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
)


def request_api(path, params=None):
    url = f"http://{node.ip_address}:9093{path}"
    print(f"Requesting {url} with params {params}")
    response = requests.get(url, params=params)
    print(f"Status code: {response.status_code}, Body: {response.text[:500]}")
    return response


@pytest.fixture(scope="module", autouse=True)
def setup(request):
    try:
        cluster.start()
        # A supported external tags table that KEEPS physical `min_time`/`max_time` columns, holding
        # bounds that do NOT cover the sample timestamps, while `store_min_time_and_max_time = 0`
        # tells the storage not to maintain or trust them. The row must still be reachable through
        # the metadata endpoints without a time range, and a ranged request must be rejected rather
        # than silently dropped by the stale bounds.
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
        node.query("CREATE TABLE prometheus ENGINE = TimeSeries SETTINGS store_min_time_and_max_time = 0 DATA prometheus_data TAGS prometheus_tags METRICS prometheus_metrics")
        # Stale bounds far away from the actual sample timestamp (1700000000).
        node.query("INSERT INTO prometheus_tags VALUES ('00000000-0000-0000-0000-000000000001', 'cpu_usage', {'host':'server1'},  toDateTime64(100, 3, 'UTC'), toDateTime64(200, 3, 'UTC'))")
        node.query("INSERT INTO prometheus_data VALUES ('00000000-0000-0000-0000-000000000001', toDateTime64(1700000000, 3, 'UTC'), 1)")
        yield cluster
    finally:
        cluster.shutdown()


def test_metadata_endpoints_work_without_time_range():
    """Without `start`/`end`, the metadata endpoints keep working and see the series."""
    response = request_api("/api/v1/labels")
    assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "success", f"Expected success, got: {data}"
    assert data["data"] == ["__name__", "host"], f"Unexpected labels: {data}"

    response = request_api("/api/v1/series", params={"match[]": "cpu_usage"})
    assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "success", f"Expected success, got: {data}"
    assert data["data"] == [{"__name__": "cpu_usage", "host": "server1"}], f"Unexpected series: {data}"


@pytest.mark.parametrize(
    "path,params",
    [
        ("/api/v1/series", {"match[]": "cpu_usage"}),
        ("/api/v1/labels", None),
        ("/api/v1/label/host/values", None),
    ],
)
def test_start_end_rejected_when_store_disabled(path, params):
    """A ranged metadata request must be rejected based on `store_min_time_and_max_time` alone: the
    physical `min_time`/`max_time` columns of the external table exist but are not maintained, and
    the real query path does not filter by them in this mode. Filtering here would silently drop the
    series (its stale bounds do not cover the sample timestamps)."""
    ranged = dict(params or {})
    ranged.update({"start": "1700000000", "end": "1700000030"})
    response = request_api(path, params=ranged)
    assert response.status_code == 400, f"Expected 400, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "error", f"Expected error status, got: {data}"
    assert "store_min_time_and_max_time" in data["error"], f"Unexpected error message: {data}"
