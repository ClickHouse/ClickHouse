"""Regression test: the Prometheus metadata endpoints must exclude empty series from ranged requests.

A series that was written with labels but no samples has no time bounds: `TimeSeriesSink::fillMinMaxTimeColumns`
stores `NULL` for its `min_time`/`max_time`. The real query path (`/api/v1/query`, `/api/v1/query_range`) filters
the `tags` table with plain `max_time >= start` / `min_time <= end` comparisons (see
`StorageTimeSeriesSelector::makeWhereFilterForTagsTable`), so a `NULL` bound makes the predicate `NULL` and the row
is dropped - such a series never contributes to a ranged query. The metadata endpoints must fail closed the same
way: a `start`/`end` request must not surface a series (or its labels/label values) whose bounds are `NULL`, even
though a request without a time range still returns it."""

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
    "node_empty_series",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
    handle_prometheus_remote_write=(9093, "/write"),
)


def send_test_data():
    """One normal series with samples and one empty series (labels but no samples).

    The empty series gets `NULL` `min_time`/`max_time` in the `tags` table, which is exactly the row the
    ranged metadata filter must exclude."""
    time_series = [
        (
            {"__name__": "cpu_usage", "host": "server1", "env": "prod"},
            {1000: 0.5, 1015: 0.6, 1030: 0.7},
        ),
        (
            # No samples -> NULL min_time/max_time. `ghost` is a label carried only by this empty series.
            {"__name__": "empty_series", "host": "server_empty", "ghost": "yes"},
            {},
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
        # Defaults: store_min_time_and_max_time = 1 and filter_by_min_time_and_max_time = 1, so the tags
        # table has the min_time/max_time columns and the metadata endpoints honor start/end.
        node.query("CREATE TABLE prometheus ENGINE=TimeSeries")
        send_test_data()
        # `cpu_usage` provides samples, so the data table becomes non-empty once the write is processed.
        assert_eq_with_retry(
            node, "SELECT count() > 0 FROM timeSeriesData(prometheus)", "1"
        )
        # Precondition of the regression: the empty series is present in the tags table with NULL bounds.
        assert_eq_with_retry(
            node,
            "SELECT min_time IS NULL AND max_time IS NULL FROM timeSeriesTags(prometheus) "
            "WHERE metric_name = 'empty_series'",
            "1",
        )
        yield cluster
    finally:
        cluster.shutdown()


def test_empty_series_present_without_time_range():
    """Without start/end, the empty series and its labels are returned - it is only hidden by a time range."""
    series = get_json_from_api("/api/v1/series")
    metric_names = {entry["__name__"] for entry in series if "__name__" in entry}
    assert "cpu_usage" in metric_names
    assert "empty_series" in metric_names

    names = get_json_from_api("/api/v1/label/__name__/values")
    assert "cpu_usage" in names
    assert "empty_series" in names

    labels = get_json_from_api("/api/v1/labels")
    assert "ghost" in labels

    ghost_values = get_json_from_api("/api/v1/label/ghost/values")
    assert ghost_values == ["yes"]


def test_series_excludes_empty_series_in_range():
    """/api/v1/series with a range overlapping the real series must not surface the NULL-bound empty series."""
    series = get_json_from_api("/api/v1/series?start=500&end=2000")
    metric_names = {entry["__name__"] for entry in series if "__name__" in entry}
    assert metric_names == {"cpu_usage"}, f"empty_series must be excluded from a ranged request, got: {series}"


def test_label_name_values_excludes_empty_series_in_range():
    """/api/v1/label/__name__/values with a range must not include the empty series' metric name."""
    names = get_json_from_api("/api/v1/label/__name__/values?start=500&end=2000")
    assert "cpu_usage" in names
    assert "empty_series" not in names, f"empty_series must be excluded from a ranged request, got: {names}"


def test_labels_excludes_empty_series_only_label_in_range():
    """/api/v1/labels with a range must not report `ghost`, which is carried only by the empty series."""
    labels = get_json_from_api("/api/v1/labels?start=500&end=2000")
    assert "__name__" in labels
    assert "host" in labels
    assert "env" in labels
    assert "ghost" not in labels, f"`ghost` is only on the NULL-bound empty series, got: {labels}"


def test_label_values_of_empty_series_only_label_in_range():
    """/api/v1/label/ghost/values with a range must be empty, since only the excluded empty series has `ghost`."""
    ghost_values = get_json_from_api("/api/v1/label/ghost/values?start=500&end=2000")
    assert ghost_values == [], f"`ghost` values must be empty for a ranged request, got: {ghost_values}"
