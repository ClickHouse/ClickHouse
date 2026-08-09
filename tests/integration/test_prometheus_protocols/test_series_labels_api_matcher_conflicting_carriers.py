"""Tests that a label matcher on a tag configured via `tags_to_columns` rejects a row carrying
different non-empty values for that tag in the dedicated column and in the residual `tags` Map,
instead of silently preferring the column. This mirrors the write path (`timeSeriesStoreTags`) and
`/api/v1/series`, which reject such malformed rows with `bad_data`; without the rejection a selector
like `{host="map_host"}` would silently drop the row and hide the conflict. The conflicting row
lives in its own cluster instance so that the fallback tests stay deterministic."""

import pytest
import requests

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node_matcher_conflicting_carriers",
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
        # A single malformed row: the `host` tag has different non-empty values in the dedicated
        # column and in the residual `tags` Map.
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
            " ('00000000-0000-0000-0000-000000000001', 'conflicting_metric', 'column_host', {'instance':'i1', 'host':'map_host'},"
            "  toDateTime64(1700000000, 3, 'UTC'), toDateTime64(1700000000, 3, 'UTC'))"
        )
        node.query("INSERT INTO prometheus_data VALUES ('00000000-0000-0000-0000-000000000001', toDateTime64(1700000000, 3, 'UTC'), 1)")
        yield cluster
    finally:
        cluster.shutdown()


def test_series_matcher_on_conflicting_row_is_rejected():
    """A matcher on the conflicting label fails with `bad_data` whichever carrier's value it asks
    for - the row must not be silently matched via the column or dropped via the Map."""
    assert_bad_data(request_api("/api/v1/series", params={"match[]": '{host="column_host"}'}))
    assert_bad_data(request_api("/api/v1/series", params={"match[]": '{host="map_host"}'}))
    assert_bad_data(request_api("/api/v1/series", params={"match[]": 'conflicting_metric{host!="something"}'}))


def test_query_matcher_on_conflicting_row_is_rejected():
    """The query endpoint uses the same selector translation."""
    assert_bad_data(request_api("/api/v1/query", params={"query": '{host="column_host"}', "time": "1700000000"}))


def test_matcher_on_another_label_is_unaffected():
    """A selector that does not touch the conflicting label does not evaluate its carriers, so it
    still works (the conflict is reported during emission by /api/v1/series instead)."""
    response = request_api("/api/v1/label/instance/values")
    assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "success", f"Expected success, got: {data}"
    assert data["data"] == ["i1"], f"Unexpected values: {data}"
