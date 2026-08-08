"""Tests for Prometheus label matchers on a tag moved into a dedicated *Nullable* column via the
`tags_to_columns` setting (an external tags table may declare it e.g. `LowCardinality(Nullable(String))`).
A series without this tag stores NULL in the column, and Prometheus treats a missing label as equal to
the empty label value, so matchers like {host=""}, {host!="prod"} or {host=~".*"} must match such
series - both on the metadata endpoints and on the query endpoints, which share the same filter."""

import pytest
import requests

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node_nullable_tag_columns",
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


def get_matched_instances(match):
    """Return the set of `instance` labels of the series matched by a `match[]` selector."""
    data = get_json_from_api("/api/v1/series", params={"match[]": match})
    return {entry["instance"] for entry in data}


@pytest.fixture(scope="module", autouse=True)
def setup(request):
    try:
        cluster.start()
        # The external tags table declares the dedicated `host` tag column as
        # `LowCardinality(Nullable(String))`; a series without the `host` tag stores NULL there.
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
        # Two `cpu_usage` series: one with the `host` tag and one without it (NULL in the column).
        node.query(
            "INSERT INTO prometheus_tags VALUES"
            " ('00000000-0000-0000-0000-000000000001', 'cpu_usage', 'prod', {'instance':'i1'},"
            "  toDateTime64(1700000000, 3, 'UTC'), toDateTime64(1700000000, 3, 'UTC')),"
            " ('00000000-0000-0000-0000-000000000002', 'cpu_usage', NULL, {'instance':'i2'},"
            "  toDateTime64(1700000000, 3, 'UTC'), toDateTime64(1700000000, 3, 'UTC'))"
        )
        node.query(
            "INSERT INTO prometheus_data VALUES"
            " ('00000000-0000-0000-0000-000000000001', toDateTime64(1700000000, 3, 'UTC'), 1),"
            " ('00000000-0000-0000-0000-000000000002', toDateTime64(1700000000, 3, 'UTC'), 2)"
        )
        yield cluster
    finally:
        cluster.shutdown()


def test_eq_matcher_on_missing_tag():
    """{host=""} must match the series where the nullable dedicated column is NULL, and {host="prod"}
    must not."""
    assert get_matched_instances('cpu_usage{host=""}') == {"i2"}
    assert get_matched_instances('cpu_usage{host="prod"}') == {"i1"}


def test_ne_matcher_on_missing_tag():
    """{host!="prod"} must match the series without the tag (NULL is the empty label value)."""
    assert get_matched_instances('cpu_usage{host!="prod"}') == {"i2"}
    assert get_matched_instances('cpu_usage{host!=""}') == {"i1"}


def test_regexp_matchers_on_missing_tag():
    """{host=~".*"} matches everything, {host=~".+"} only series that have the tag, and a negative
    regexp treats the missing tag as the empty value."""
    assert get_matched_instances('cpu_usage{host=~".*"}') == {"i1", "i2"}
    assert get_matched_instances('cpu_usage{host=~".+"}') == {"i1"}
    assert get_matched_instances('cpu_usage{host=~"prod|"}') == {"i1", "i2"}
    assert get_matched_instances('cpu_usage{host!~".+"}') == {"i2"}


def test_labels_and_label_values_with_matcher_on_missing_tag():
    """/api/v1/labels and /api/v1/label/<name>/values use the same matcher translation."""
    data = get_json_from_api("/api/v1/labels", params={"match[]": 'cpu_usage{host=""}'})
    assert "host" not in data, f"Unexpected labels: {data}"
    assert "instance" in data
    data = get_json_from_api("/api/v1/label/instance/values", params={"match[]": 'cpu_usage{host!="prod"}'})
    assert data == ["i2"], f"Unexpected values: {data}"


def test_query_endpoint_matcher_on_missing_tag():
    """The query endpoint shares the matcher translation with the metadata endpoints, so an instant
    query with {host=""} must select the series where the nullable dedicated column is NULL."""
    data = get_json_from_api("/api/v1/query", params={"query": 'cpu_usage{host=""}', "time": "1700000000"})
    result = data["result"]
    assert len(result) == 1, f"Unexpected result: {result}"
    assert result[0]["metric"].get("instance") == "i2"
    assert result[0]["value"][1] == "2"
