"""Tests that a small `limit` cannot hide a malformed row from the mixed-carrier `bad_data`
validation of the metadata endpoints.

A row carrying different non-empty values for a `tags_to_columns` tag in the dedicated column and in
the residual `tags` Map is rejected with `bad_data`. That contract requires reading every matched
row: if `limit` were pushed down as a SQL `LIMIT`, the scan could stop after `limit` valid series and
a conflicting row that sorts after them would silently disappear behind a truncated success response,
making the corruption observable or hidden based only on the caller's `limit` and row order. The
conflicting row here sorts after more than `limit` valid series, so these tests fail if the scan is
ever bounded by `limit` again."""

import pytest
import requests

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node_limit_conflicting_row",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
)


def request_api(path, params=None):
    url = f"http://{node.ip_address}:9093{path}"
    print(f"Requesting {url} with params {params}")
    response = requests.get(url, params=params)
    print(f"Status code: {response.status_code}, Body: {response.text[:500]}")
    return response


def assert_bad_data(response):
    assert response.status_code == 400, f"Expected 400, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "error", f"Unexpected body: {data}"
    assert data["errorType"] == "bad_data", f"Unexpected body: {data}"
    assert "host" in data["error"], f"Unexpected error message: {data}"


@pytest.fixture(scope="module", autouse=True)
def setup(request):
    try:
        cluster.start()
        # Three valid series whose metric names sort before the conflicting row's ('zz_conflict'):
        # the tags table is ordered by (metric_name, id), so a scan bounded by a small SQL LIMIT
        # would stop on the valid rows and never reach the malformed one.
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
            " ('00000000-0000-0000-0000-000000000001', 'aa_metric', 'h1', {'instance':'i1'},"
            "  toDateTime64(1700000000, 3, 'UTC'), toDateTime64(1700000000, 3, 'UTC')),"
            " ('00000000-0000-0000-0000-000000000002', 'bb_metric', 'h2', {'instance':'i2'},"
            "  toDateTime64(1700000000, 3, 'UTC'), toDateTime64(1700000000, 3, 'UTC')),"
            " ('00000000-0000-0000-0000-000000000003', 'cc_metric', 'h3', {'instance':'i3'},"
            "  toDateTime64(1700000000, 3, 'UTC'), toDateTime64(1700000000, 3, 'UTC')),"
            " ('00000000-0000-0000-0000-000000000004', 'zz_conflict', 'column_host', {'host':'map_host'},"
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


@pytest.mark.parametrize("limit", ["1", "2"])
def test_series_small_limit_still_surfaces_bad_data(limit):
    """`/api/v1/series` must report `bad_data` even when `limit` valid series would already fill the
    response before the conflicting row is reached."""
    assert_bad_data(request_api("/api/v1/series", params={"match[]": '{__name__=~".+"}', "limit": limit}))


def test_labels_small_limit_still_surfaces_bad_data():
    """`/api/v1/labels` evaluates the conflicting-carrier check on every matched row, regardless of
    `limit`."""
    assert_bad_data(request_api("/api/v1/labels", params={"limit": "1"}))


def test_label_values_small_limit_still_surfaces_bad_data():
    """`/api/v1/label/host/values` materializes the `host` carriers of every matched row, so a small
    `limit` must not hide the conflict."""
    assert_bad_data(request_api("/api/v1/label/host/values", params={"limit": "1"}))


def test_series_without_conflict_match_still_truncates():
    """A selector that excludes the malformed row keeps the normal truncation behavior: `limit=1`
    returns one series and the truncation warning, proving the emission-layer limit still works."""
    response = request_api("/api/v1/series", params={"match[]": '{__name__=~"(aa|bb|cc)_metric"}', "limit": "1"})
    assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "success", f"Expected success, got: {data}"
    assert len(data["data"]) == 1, f"Expected exactly one series: {data}"
    assert "results truncated due to limit" in data.get("warnings", []), f"Expected truncation: {data}"
