"""Tests for the Prometheus metadata endpoints `start`/`end` time-range filter on a `TimeSeries` table
whose timestamp type is not the default `DateTime64(3)`.

The metadata endpoints must build the `min_time`/`max_time` comparison literals with the table's actual
timestamp type and scale (here `DateTime64(6)`), using the same conversion path as the PromQL query, rather
than a hard-coded millisecond `DateTime64(3)`."""

import pytest
import requests

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
from .prometheus_test_utils import (
    convert_time_series_to_protobuf,
    send_protobuf_to_remote_write,
)


cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node_timestamp_type",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
    handle_prometheus_remote_write=(9093, "/write"),
)


def send_test_data():
    """All samples are written at t in [1000s, 1030s]."""
    time_series = [
        (
            {"__name__": "cpu_usage", "host": "server1"},
            {1000: 0.5, 1015: 0.6, 1030: 0.7},
        ),
        (
            {"__name__": "memory_usage", "host": "server2"},
            {1000: 0.8, 1015: 0.85, 1030: 0.9},
        ),
    ]
    protobuf = convert_time_series_to_protobuf(time_series)
    send_protobuf_to_remote_write(node.ip_address, 9093, "/write", protobuf)


def get_json_from_api(path):
    """Make a GET request to the ClickHouse Prometheus API and return parsed JSON."""
    url = f"http://{node.ip_address}:9093{path}"
    print(f"Requesting {url}")
    response = requests.get(url)
    print(f"Status code: {response.status_code}, Body: {response.text[:500]}")
    assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "success", f"Expected success, got: {data}"
    return data["data"]


@pytest.fixture(scope="module", autouse=True)
def setup(request):
    try:
        cluster.start()
        # The timestamp column (and therefore `min_time`/`max_time`) is `DateTime64(6)`, not the default
        # `DateTime64(3)`, so the time-range filter must derive the type/scale from the column.
        node.query(
            "CREATE TABLE prometheus ENGINE=TimeSeries SAMPLES INNER COLUMNS (timestamp DateTime64(6))"
        )
        send_test_data()
        assert_eq_with_retry(
            node, "SELECT count() > 0 FROM timeSeriesData(prometheus)", "1"
        )
        yield cluster
    finally:
        cluster.shutdown()


def test_table_uses_non_default_timestamp_type():
    """Sanity check: the table's timestamp type is `DateTime64(6)` (the metadata filter derives the
    comparison literal's type/scale from the `time_series` column), and the inner `min_time`/`max_time`
    columns carry the same `DateTime64(6)` type."""
    time_series_type = node.query(
        "SELECT type FROM system.columns "
        "WHERE database = currentDatabase() AND table = 'prometheus' AND name = 'time_series'"
    ).strip()
    assert time_series_type == "Array(Tuple(DateTime64(6), Float64))", time_series_type

    min_max_types = node.query(
        "SELECT type FROM system.columns "
        "WHERE database = currentDatabase() AND table LIKE '.inner_id.tags.%' "
        "AND name IN ('min_time', 'max_time')"
    )
    assert "DateTime64(6)" in min_max_types, f"Unexpected min_time/max_time types: {min_max_types}"


def test_series_with_start_end_in_range():
    """GET /api/v1/series with a [start, end] overlapping the data must return the series on a
    `DateTime64(6)` table (the comparison literal is built with scale 6)."""
    data = get_json_from_api("/api/v1/series?start=500&end=2000")
    metric_names = {entry["__name__"] for entry in data if "__name__" in entry}
    assert "cpu_usage" in metric_names
    assert "memory_usage" in metric_names


def test_series_with_start_end_out_of_range():
    """GET /api/v1/series with a [start, end] not overlapping any series must return nothing."""
    data = get_json_from_api("/api/v1/series?start=100000&end=200000")
    assert data == []


def test_series_start_bound_respects_microsecond_scale():
    """The `start`/`end` comparison literal must be built with the table's real `DateTime64(6)` scale,
    not a hard-coded millisecond `DateTime64(3)`.

    Every series has its last sample at t = 1030 s exactly, stored with microsecond precision. A `start`
    400 microseconds *after* that sample must exclude the series (`max_time >= start` is false), while a
    `start` 400 microseconds *before* it must keep the series. A `DateTime64(3)` literal would round both
    bounds to 1030.000 s and return the same series in both cases, so this test fails if the implementation
    ever falls back to millisecond literals for a higher-scale table."""
    just_after = get_json_from_api("/api/v1/series?start=1030.0004&end=2000")
    assert (
        just_after == []
    ), f"Expected no series for a start 400us after the last sample, got {just_after}"

    just_before = get_json_from_api("/api/v1/series?start=1029.9996&end=2000")
    metric_names = {entry["__name__"] for entry in just_before if "__name__" in entry}
    assert "cpu_usage" in metric_names and "memory_usage" in metric_names, (
        f"Expected both series for a start 400us before the last sample, got {just_before}"
    )


def test_labels_with_start_end_in_range():
    """GET /api/v1/labels with an in-range [start, end] returns the real labels."""
    data = get_json_from_api("/api/v1/labels?start=500&end=2000")
    assert "__name__" in data
    assert "host" in data


def test_start_after_end_is_rejected():
    """start > end must be rejected on a non-`DateTime64(3)` table as well."""
    url = f"http://{node.ip_address}:9093/api/v1/series?start=2000&end=1000"
    response = requests.get(url)
    assert response.status_code == 400, f"Expected 400, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "error", f"Expected error status, got: {data}"
    assert (
        "start_time must not be greater than end_time" in data["error"]
    ), f"Unexpected error message: {data}"
