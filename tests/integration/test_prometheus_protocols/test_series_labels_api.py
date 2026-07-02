"""Tests for Prometheus HTTP API endpoints: /api/v1/series, /api/v1/labels, /api/v1/label/<name>/values"""

import time

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
    "node",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
    handle_prometheus_remote_write=(9093, "/write"),
)


def send_test_data():
    """Send test data with known labels for testing series/labels/label-values endpoints."""
    time_series = [
        (
            {"__name__": "cpu_usage", "host": "server1", "datacenter": "us-east"},
            {1000: 0.5, 1015: 0.6, 1030: 0.7},
        ),
        (
            {"__name__": "cpu_usage", "host": "server2", "datacenter": "us-west"},
            {1000: 0.3, 1015: 0.4, 1030: 0.5},
        ),
        (
            {"__name__": "memory_usage", "host": "server1", "datacenter": "us-east"},
            {1000: 0.8, 1015: 0.85, 1030: 0.9},
        ),
        (
            {"__name__": "http_requests_total", "host": "server1", "method": "GET", "status": "200"},
            {1000: 100, 1015: 150, 1030: 200},
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
        node.query("CREATE TABLE prometheus ENGINE=TimeSeries")
        send_test_data()
        # Wait for data to be available
        assert_eq_with_retry(
            node, "SELECT count() > 0 FROM timeSeriesData(prometheus)", "1"
        )
        yield cluster
    finally:
        cluster.shutdown()


def test_labels_returns_all_label_names():
    """GET /api/v1/labels should return all unique label names including __name__."""
    data = get_json_from_api("/api/v1/labels")
    assert isinstance(data, list)
    assert "__name__" in data
    assert "host" in data
    assert "datacenter" in data


def test_label_values_for_name():
    """GET /api/v1/label/__name__/values should return all metric names."""
    data = get_json_from_api("/api/v1/label/__name__/values")
    assert isinstance(data, list)
    assert "cpu_usage" in data
    assert "memory_usage" in data
    assert "http_requests_total" in data


def test_label_values_for_host():
    """GET /api/v1/label/host/values should return all host values."""
    data = get_json_from_api("/api/v1/label/host/values")
    assert isinstance(data, list)
    assert "server1" in data
    assert "server2" in data


def test_label_values_for_datacenter():
    """GET /api/v1/label/datacenter/values should return datacenter values."""
    data = get_json_from_api("/api/v1/label/datacenter/values")
    assert isinstance(data, list)
    assert "us-east" in data
    assert "us-west" in data


def test_series_returns_metric_labels():
    """GET /api/v1/series should return series with their full label sets."""
    data = get_json_from_api("/api/v1/series")
    assert isinstance(data, list)
    assert len(data) > 0

    # Each entry should be a dict with __name__ and other labels
    metric_names = {entry["__name__"] for entry in data if "__name__" in entry}
    assert "cpu_usage" in metric_names
    assert "memory_usage" in metric_names


def test_label_values_for_nonexistent_label():
    """GET /api/v1/label/nonexistent/values should return empty list."""
    data = get_json_from_api("/api/v1/label/nonexistent/values")
    assert isinstance(data, list)
    assert len(data) == 0


def test_series_includes_real_labels():
    """GET /api/v1/series must return the full label set, not only __name__.

    The tags column is a Map, so each returned series should contain the real labels
    (e.g. host, datacenter) that were written, deduplicated by series identity.
    """
    data = get_json_from_api("/api/v1/series")
    cpu_series = [entry for entry in data if entry.get("__name__") == "cpu_usage"]
    assert len(cpu_series) == 2, f"Expected 2 cpu_usage series, got: {cpu_series}"

    # Each series must carry its full label set, not just __name__.
    for entry in cpu_series:
        assert "host" in entry
        assert "datacenter" in entry

    hosts = {entry["host"] for entry in cpu_series}
    assert hosts == {"server1", "server2"}


def test_series_match_filter():
    """GET /api/v1/series?match[]=<metric> should filter by metric name.

    Exercises that match[] is treated as an endpoint parameter (not a ClickHouse setting).
    """
    data = get_json_from_api("/api/v1/series?match[]=cpu_usage")
    assert len(data) > 0
    metric_names = {entry["__name__"] for entry in data if "__name__" in entry}
    assert metric_names == {"cpu_usage"}


def test_label_values_with_match_filter():
    """GET /api/v1/label/<name>/values?match[]=<metric> should route correctly with a query string.

    The query string must not break path routing (the endpoint used to fall through to 404).
    """
    data = get_json_from_api("/api/v1/label/host/values?match[]=memory_usage")
    assert isinstance(data, list)
    assert "server1" in data
    assert "server2" not in data


def test_series_deduplicated():
    """Re-writing the same series must not produce duplicate entries in /api/v1/series.

    The tags target is AggregatingMergeTree and stores a row per write, so the same
    series appears as multiple rows until parts are merged; /api/v1/series must
    deduplicate by series identity. Merges are stopped so the duplicate rows persist.
    """
    series = [
        (
            {"__name__": "dedup_metric", "host": "serverX"},
            {1000: 1.0},
        ),
    ]
    protobuf = convert_time_series_to_protobuf(series)

    node.query("SYSTEM STOP MERGES")
    try:
        send_protobuf_to_remote_write(node.ip_address, 9093, "/write", protobuf)
        send_protobuf_to_remote_write(node.ip_address, 9093, "/write", protobuf)
        # Wait until both writes have produced separate (un-merged) rows in the tags table.
        assert_eq_with_retry(
            node,
            "SELECT count() FROM timeSeriesTags(prometheus) WHERE metric_name = 'dedup_metric'",
            "2",
        )
        data = get_json_from_api("/api/v1/series?match[]=dedup_metric")
        assert len(data) == 1, f"Expected exactly 1 deduplicated series, got: {data}"
        assert data[0]["host"] == "serverX"
    finally:
        node.query("SYSTEM START MERGES")


# All test samples are written at t in [1000s, 1030s], so a window that brackets them keeps every
# series while a window far away excludes them all via the min_time/max_time overlap filter.


def test_series_with_start_end_in_range():
    """GET /api/v1/series with a [start, end] overlapping the data must return the series."""
    data = get_json_from_api("/api/v1/series?start=500&end=2000")
    metric_names = {entry["__name__"] for entry in data if "__name__" in entry}
    assert "cpu_usage" in metric_names
    assert "memory_usage" in metric_names


def test_series_with_start_end_out_of_range():
    """GET /api/v1/series with a [start, end] not overlapping any series must return nothing."""
    data = get_json_from_api("/api/v1/series?start=100000&end=200000")
    assert data == []


def test_labels_with_start_end_out_of_range():
    """GET /api/v1/labels with an out-of-range [start, end] returns only the virtual __name__."""
    data = get_json_from_api("/api/v1/labels?start=100000&end=200000")
    assert data == ["__name__"]


def test_label_values_with_start_end_out_of_range():
    """GET /api/v1/label/<name>/values with an out-of-range [start, end] returns nothing."""
    data = get_json_from_api("/api/v1/label/host/values?start=100000&end=200000")
    assert data == []


@pytest.mark.parametrize(
    "path",
    [
        "/api/v1/series",
        "/api/v1/labels",
        "/api/v1/label/host/values",
    ],
)
def test_start_after_end_is_rejected(path):
    """A request with start > end must be rejected instead of matching long-lived series for an empty
    interval (mirrors the PromQL query path which rejects start_time > end_time)."""
    url = f"http://{node.ip_address}:9093{path}?start=2000&end=1000"
    response = requests.get(url)
    assert response.status_code == 400, f"Expected 400, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["status"] == "error", f"Expected error status, got: {data}"
    assert (
        "start_time must not be greater than end_time" in data["error"]
    ), f"Unexpected error message: {data}"


def test_metadata_endpoint_records_query_finish():
    """A successful metadata request must record a QueryFinish entry in system.query_log.

    The metadata endpoints execute their generated SQL through executeQuery and must complete the
    resulting BlockIO with finishExecutedQuery, exactly like the PromQL query path. BlockIO's
    destructor only resets the pipeline and never runs the finish/exception callbacks, so before
    this was fixed a successful /api/v1/series, /api/v1/labels or /api/v1/label/<name>/values request
    never emitted QueryFinish (and kept the query slot occupied until the whole HTTP response had
    been written).
    """
    def count_label_query_finish():
        node.query("SYSTEM FLUSH LOGS")
        # The generated /api/v1/labels query is `SELECT DISTINCT arrayJoin(mapKeys(tags)) AS label_key ...`.
        # `query NOT LIKE '%query_log%'` excludes this counting query itself, which otherwise matches the
        # `AS label_key`/`mapKeys` patterns (they appear in its own text) and would inflate the count with a
        # self-referential QueryFinish entry, letting the assertion below pass even if the real request
        # stopped emitting QueryFinish.
        return int(
            node.query(
                """
                SELECT count()
                FROM system.query_log
                WHERE type = 'QueryFinish'
                  AND query LIKE '%AS label_key%'
                  AND query LIKE '%mapKeys%'
                  AND query NOT LIKE '%query_log%'
                """
            ).strip()
        )

    # Other tests in this module also issue /api/v1/labels, so a matching QueryFinish entry may already
    # be present in system.query_log. Record a baseline and require the count to strictly increase for the
    # request made below, otherwise the test would still pass on a stale entry even if this specific request
    # stopped emitting QueryFinish again.
    baseline = count_label_query_finish()

    get_json_from_api("/api/v1/labels")

    # The /api/v1/labels endpoint runs a generated `SELECT DISTINCT arrayJoin(mapKeys(tags)) AS label_key ...`
    # query. finishExecutedQuery records the QueryFinish entry only after the HTTP body has been flushed and
    # the entry then reaches system.query_log asynchronously, so flush the logs and retry to avoid a race.
    finished = baseline
    for _ in range(30):
        finished = count_label_query_finish()
        if finished > baseline:
            break
        time.sleep(0.5)

    assert finished > baseline, (
        f"Expected a new QueryFinish entry in system.query_log for the /api/v1/labels query "
        f"(baseline {baseline}), got {finished}"
    )
