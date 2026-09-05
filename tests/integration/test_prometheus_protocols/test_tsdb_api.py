"""Tests for the Prometheus /api/v1/status/tsdb endpoint."""

import pytest
import requests

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
)


def get_json_from_api(path):
    response = requests.get(f"http://{node.ip_address}:9093{path}")
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["status"] == "success", data
    return data["data"]


def get_bad_data_from_api(path):
    response = requests.get(f"http://{node.ip_address}:9093{path}")
    assert response.status_code == 400, response.text
    data = response.json()
    assert data["status"] == "error", data
    assert data["errorType"] == "bad_data", data
    return data


@pytest.fixture(scope="module", autouse=True)
def setup():
    try:
        cluster.start()
        node.query(
            "CREATE TABLE prometheus ENGINE=TimeSeries "
            "SETTINGS tags_to_columns = {'host': 'host_column'}"
        )
        node.query("CREATE TABLE prometheus_empty ENGINE=TimeSeries")
        node.query(
            "CREATE TABLE prometheus_no_bounds ENGINE=TimeSeries "
            "SETTINGS store_min_time_and_max_time = 0"
        )

        rows = [
            "('cpu_usage', {'host': 'server1', 'region': 'eu'}, "
            "[(toDateTime64(1000, 3), 0.5), (toDateTime64(1030, 3), 0.7)])",
            "('cpu_usage', {'host': 'server2', 'region': 'us'}, "
            "[(toDateTime64(1000, 3), 0.3), (toDateTime64(1030, 3), 0.5)])",
            "('memory_usage', {'host': 'server1', 'region': 'eu'}, "
            "[(toDateTime64(1000, 3), 0.8)])",
            "('unicode_metric', {'city': 'é', 'region': 'eu'}, "
            "[(toDateTime64(1000, 3), 1.0)])",
        ]
        for row in rows:
            node.query(
                "INSERT INTO prometheus (metric_name, tags, time_series) VALUES "
                + row
            )

        # Insert one logical series again in a separate part. Physical duplicate metadata rows
        # must not increase the Prometheus series or label cardinalities.
        node.query(
            "INSERT INTO prometheus (metric_name, tags, time_series) VALUES "
            "('cpu_usage', {'host': 'server1', 'region': 'eu'}, "
            "[(toDateTime64(1000, 3), 0.5), (toDateTime64(1030, 3), 0.7)])"
        )
        node.query(
            "INSERT INTO prometheus_no_bounds (metric_name, tags, time_series) VALUES "
            "('only_metric', {'job': 'api', 'empty': ''}, [(toDateTime64(1000, 3), 1.0)])"
        )

        assert_eq_with_retry(node, "SELECT count() > 0 FROM timeSeriesData(prometheus)", "1")
        yield cluster
    finally:
        cluster.shutdown()


def test_tsdb_returns_all_statistics():
    data = get_json_from_api("/api/v1/status/tsdb")

    assert data["headStats"] == {
        "numSeries": 4,
        "numLabelPairs": 8,
        "chunkCount": 0,
        "minTime": 1_000_000,
        "maxTime": 1_030_000,
    }
    assert data["seriesCountByMetricName"] == [
        {"name": "cpu_usage", "value": 2},
        {"name": "memory_usage", "value": 1},
        {"name": "unicode_metric", "value": 1},
    ]
    assert data["labelValueCountByLabelName"] == [
        {"name": "__name__", "value": 3},
        {"name": "host", "value": 2},
        {"name": "region", "value": 2},
        {"name": "city", "value": 1},
    ]
    assert data["memoryInBytesByLabelName"] == [
        {"name": "__name__", "value": 84},
        {"name": "region", "value": 40},
        {"name": "host", "value": 39},
        {"name": "city", "value": 8},
    ]
    assert data["seriesCountByLabelValuePair"] == [
        {"name": "region=eu", "value": 3},
        {"name": "__name__=cpu_usage", "value": 2},
        {"name": "host=server1", "value": 2},
        {"name": "__name__=memory_usage", "value": 1},
        {"name": "__name__=unicode_metric", "value": 1},
        {"name": "city=é", "value": 1},
        {"name": "host=server2", "value": 1},
        {"name": "region=us", "value": 1},
    ]


def test_tsdb_limit_applies_independently_to_each_statistics_list():
    data = get_json_from_api("/api/v1/status/tsdb?limit=1")

    assert data["seriesCountByMetricName"] == [{"name": "cpu_usage", "value": 2}]
    assert data["labelValueCountByLabelName"] == [{"name": "__name__", "value": 3}]
    assert data["memoryInBytesByLabelName"] == [{"name": "__name__", "value": 84}]
    assert data["seriesCountByLabelValuePair"] == [{"name": "region=eu", "value": 3}]


@pytest.mark.parametrize("limit", ["0", "-1", "10001", "abc", "1.5"])
def test_tsdb_rejects_invalid_limits(limit):
    get_bad_data_from_api(f"/api/v1/status/tsdb?limit={limit}")


def test_tsdb_accepts_maximum_limit():
    data = get_json_from_api("/api/v1/status/tsdb?limit=10000")
    assert len(data["seriesCountByLabelValuePair"]) == 8


def test_tsdb_empty_table_returns_zero_statistics():
    data = get_json_from_api("/empty/api/v1/status/tsdb")
    assert data == {
        "headStats": {
            "numSeries": 0,
            "numLabelPairs": 0,
            "chunkCount": 0,
            "minTime": 0,
            "maxTime": 0,
        },
        "seriesCountByMetricName": [],
        "labelValueCountByLabelName": [],
        "memoryInBytesByLabelName": [],
        "seriesCountByLabelValuePair": [],
    }


def test_tsdb_without_stored_time_bounds_still_returns_cardinality():
    data = get_json_from_api("/no_bounds/api/v1/status/tsdb")
    assert data["headStats"] == {
        "numSeries": 1,
        "numLabelPairs": 2,
        "chunkCount": 0,
        "minTime": 0,
        "maxTime": 0,
    }
    assert data["seriesCountByMetricName"] == [{"name": "only_metric", "value": 1}]
    assert data["labelValueCountByLabelName"] == [
        {"name": "__name__", "value": 1},
        {"name": "job", "value": 1},
    ]
    assert data["seriesCountByLabelValuePair"] == [
        {"name": "__name__=only_metric", "value": 1},
        {"name": "job=api", "value": 1},
    ]
