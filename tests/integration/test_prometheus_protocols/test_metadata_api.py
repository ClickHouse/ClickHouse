"""Tests for the Prometheus /api/v1/metadata endpoint."""

import pytest
import requests

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
from .prometheus_test_utils import (
    convert_metrics_metadata_to_protobuf,
    send_protobuf_to_remote_write,
)

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
    handle_prometheus_remote_write=(9093, "/write"),
)

ESCAPED_HELP = 'Contains "quotes", back\\slashes,\nnewlines, \ttabs and unicode: тест'

# The default Metrics target table is a `ReplacingMergeTree` ordered by the metric family name,
# so it keeps one (arbitrary) metadata entry per family: each family below has exactly one entry.
ALL_METADATA = {
    "cpu_usage": [
        {"type": "gauge", "help": "CPU usage of the host", "unit": "percent"}
    ],
    "escaped_metric": [{"type": "gauge", "help": ESCAPED_HELP, "unit": ""}],
    "http_requests_total": [
        {"type": "counter", "help": "Total number of HTTP requests", "unit": ""}
    ],
    "request_duration_seconds": [
        {"type": "histogram", "help": "Duration of requests", "unit": "seconds"}
    ],
}

# Metadata entries of the family stored in the `prometheus_multi` table (see the setup below).
MULTI_METRIC_ENTRIES = [
    {"type": "counter", "help": "The first help text", "unit": ""},
    {"type": "counter", "help": "The second help text", "unit": ""},
]


def send_test_metadata():
    metrics_metadata = [
        (family, entries[0]["type"].upper(), entries[0]["help"], entries[0]["unit"])
        for family, entries in ALL_METADATA.items()
    ]
    send_protobuf_to_remote_write(
        node.ip_address,
        9093,
        "/write",
        convert_metrics_metadata_to_protobuf(metrics_metadata),
    )


def get_json_from_api(path, **kwargs):
    response = requests.get(f"http://{node.ip_address}:9093{path}", **kwargs)
    assert (
        response.status_code == 200
    ), f"Expected 200, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "success", f"Expected success, got: {data}"
    return data


def get_bad_data_from_api(path, **kwargs):
    response = requests.get(f"http://{node.ip_address}:9093{path}", **kwargs)
    assert (
        response.status_code == 400
    ), f"Expected 400, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "error", f"Expected error, got: {data}"
    assert data["errorType"] == "bad_data", f"Expected bad_data, got: {data}"
    return data


@pytest.fixture(scope="module", autouse=True)
def setup():
    try:
        cluster.start()
        node.query("CREATE TABLE prometheus ENGINE=TimeSeries")
        node.query("CREATE TABLE prometheus_empty ENGINE=TimeSeries")
        # A table whose Metrics target never collapses rows, to get a metric family with
        # a deterministic number of multiple metadata entries (the default `ReplacingMergeTree`
        # Metrics target keeps one entry per family in each data part).
        node.query(
            "CREATE TABLE prometheus_multi ENGINE=TimeSeries "
            "METRICS INNER ENGINE=MergeTree ORDER BY metric_family_name"
        )
        # Send/insert the same metadata twice to get duplicate rows (in separate data parts)
        # in the Metrics target tables: the endpoint must deduplicate them.
        for _ in range(2):
            send_test_metadata()
            node.query(
                "INSERT INTO TABLE FUNCTION timeSeriesMetrics(prometheus_multi) "
                "(metric_family_name, type, unit, help) VALUES "
                "('multi_metric', 'counter', '', 'The first help text'), "
                "('multi_metric', 'counter', '', 'The second help text')"
            )
        assert_eq_with_retry(
            node, "SELECT count() > 0 FROM timeSeriesMetrics(prometheus)", "1"
        )
        yield cluster
    finally:
        cluster.shutdown()


def test_metadata_returns_deduplicated_metadata():
    data = get_json_from_api("/api/v1/metadata")["data"]
    assert data == ALL_METADATA


def test_metric_parameter_filters_families():
    data = get_json_from_api("/api/v1/metadata?metric=cpu_usage")["data"]
    assert data == {"cpu_usage": ALL_METADATA["cpu_usage"]}


def test_unknown_metric_returns_empty_result():
    data = get_json_from_api("/api/v1/metadata?metric=no_such_metric")["data"]
    assert data == {}


def test_limit():
    data = get_json_from_api("/api/v1/metadata?limit=2")["data"]
    assert len(data) == 2
    assert data == {
        family: entries for family, entries in ALL_METADATA.items() if family in data
    }

    data = get_json_from_api("/api/v1/metadata?limit=1000")["data"]
    assert data == ALL_METADATA


def test_limit_zero_returns_empty_result():
    data = get_json_from_api("/api/v1/metadata?limit=0")["data"]
    assert data == {}


def test_negative_limit_means_no_limit():
    data = get_json_from_api("/api/v1/metadata?limit=-1")["data"]
    assert data == ALL_METADATA


def test_multiple_metadata_entries_per_family():
    data = get_json_from_api("/multi/api/v1/metadata")["data"]
    entries = sorted(data["multi_metric"], key=lambda entry: entry["help"])
    assert entries == MULTI_METRIC_ENTRIES


def test_limit_per_metric():
    data = get_json_from_api("/multi/api/v1/metadata?limit_per_metric=1")["data"]
    assert len(data["multi_metric"]) == 1
    assert data["multi_metric"][0] in MULTI_METRIC_ENTRIES


def test_non_positive_limit_per_metric_means_no_limit():
    for value in (0, -1):
        data = get_json_from_api(f"/multi/api/v1/metadata?limit_per_metric={value}")[
            "data"
        ]
        entries = sorted(data["multi_metric"], key=lambda entry: entry["help"])
        assert entries == MULTI_METRIC_ENTRIES


def test_metric_and_limit_per_metric_combined():
    data = get_json_from_api(
        "/multi/api/v1/metadata?metric=multi_metric&limit_per_metric=1"
    )["data"]
    assert set(data) == {"multi_metric"}
    assert len(data["multi_metric"]) == 1
    assert data["multi_metric"][0] in MULTI_METRIC_ENTRIES


def test_invalid_limits():
    for parameter in ("limit", "limit_per_metric"):
        error = get_bad_data_from_api(f"/api/v1/metadata?{parameter}=abc")["error"]
        assert parameter in error


def test_json_escaping():
    data = get_json_from_api("/api/v1/metadata?metric=escaped_metric")["data"]
    assert data["escaped_metric"] == [
        {"type": "gauge", "help": ESCAPED_HELP, "unit": ""}
    ]


def test_empty_metrics_table():
    data = get_json_from_api("/empty/api/v1/metadata")["data"]
    assert data == {}
