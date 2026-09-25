import json
import math
import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
from .prometheus_test_utils import execute_query_via_http_api


# A real Prometheus scrapes itself with native histograms enabled and sends them to ClickHouse over remote-write.
# Prometheus exposes a native histogram about itself (`prometheus_http_request_duration_seconds`). Native histograms with
# custom buckets never arrive this way: no Prometheus version sends them over remote-write 1.0 (see test_native_histograms.py
# for hand-crafted ones).

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
    with_prometheus_writer=True,
    prometheus_writer_native_histograms=True,
    handle_prometheus_remote_write=(9093, "/write"),
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        node.query("CREATE TABLE prometheus ENGINE=TimeSeries")
        wait_for_histograms()
        yield cluster
    finally:
        cluster.shutdown()


def wait_for_histograms():
    assert_eq_with_retry(node, "SELECT count() > 0 FROM timeSeriesHistograms(prometheus)", "1", retry_count=120)


def execute_query_in_prometheus_writer(query, timestamp=None):
    return json.loads(
        execute_query_via_http_api(
            cluster.prometheus_ip["writer"], cluster.prometheus_port["writer"], "/api/v1/query", query, timestamp
        )
    )


def get_scalar_from_prometheus_writer(query, timestamp=None):
    result = execute_query_in_prometheus_writer(query, timestamp)["result"]
    assert len(result) == 1, result
    return float(result[0]["value"][1])


def wait_for_positive_scalar_in_prometheus_writer(query):
    # Prometheus sees its own counters through its self-scrape, which lags behind what it has just sent.
    for _ in range(60):
        value = get_scalar_from_prometheus_writer(query)
        if value > 0:
            return value
        time.sleep(0.5)
    raise AssertionError(f"{query} stayed 0")


def get_last_stored_histogram(condition):
    """Returns the newest stored histogram matching the condition as a dict with the metric name, the tags, the timestamp in ms,
    the count and the sum."""
    return json.loads(
        node.query(
            f"""
            SELECT metric_name, tags, toUnixTimestamp64Milli(h.timestamp) AS timestamp_ms,
                   if(h.is_float, h.count_float, toFloat64(h.count_int)) AS count, h.sum AS sum
            FROM timeSeriesHistograms(prometheus) AS h
            JOIN timeSeriesTags(prometheus) AS t ON h.id = t.id
            WHERE {condition}
            ORDER BY h.timestamp DESC
            LIMIT 1
            FORMAT JSONEachRow
            """
        )
    )


def test_histogram_matches_prometheus():
    # The histogram stored in ClickHouse must be the one Prometheus has in its own storage at that time.
    stored = get_last_stored_histogram("h.schema BETWEEN -4 AND 8")
    # The `tags` map of the tags table includes `__name__`, which the selector already carries as the metric name.
    labels = ", ".join(f"{name}={json.dumps(value)}" for name, value in stored["tags"].items() if name != "__name__")
    selector = f'{stored["metric_name"]}{{{labels}}}'
    timestamp = stored["timestamp_ms"] / 1000

    assert get_scalar_from_prometheus_writer(f"histogram_count({selector})", timestamp) == stored["count"]
    assert math.isclose(get_scalar_from_prometheus_writer(f"histogram_sum({selector})", timestamp), stored["sum"], rel_tol=1e-12)


def test_nothing_is_lost():
    # Prometheus counts the histograms it sent and the ones the receiver rejected; ClickHouse counts the ones it dropped.
    wait_for_positive_scalar_in_prometheus_writer("sum(prometheus_remote_storage_histograms_total)")
    # The dropped counter has one series per reason, so it has no series at all when nothing was dropped.
    assert get_scalar_from_prometheus_writer("sum(prometheus_remote_storage_histograms_failed_total) or vector(0)") == 0
    assert get_scalar_from_prometheus_writer("sum(prometheus_remote_storage_histograms_dropped_total) or vector(0)") == 0
    assert node.query("SELECT sum(value) FROM system.events WHERE event = 'PrometheusRemoteWriteDroppedHistograms'") == "0\n"
    assert not node.contains_in_log("Invalid histogram sample")
