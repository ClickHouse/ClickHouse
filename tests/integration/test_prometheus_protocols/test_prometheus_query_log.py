"""
Integration tests that assert Prometheus Query API operations (/api/v1/query and
/api/v1/query_range) are reflected in system.query_log with read_rows/read_bytes.
"""

import time
import urllib.parse

import pytest

from helpers.cluster import ClickHouseCluster
from .prometheus_test_utils import (
    convert_time_series_to_protobuf,
    get_response_to_http_api,
    send_protobuf_to_remote_write,
    extract_data_from_http_api_response,
)


QUERY_LOG_COUNT_SQL = (
    "SELECT count() FROM system.query_log "
    "WHERE type = 'QueryFinish' AND read_rows > 0 AND read_bytes > 0"
)


def assert_query_log_count_increased(count_before, retry_count=30, sleep_time=1):
    """PromQL evaluation may log multiple internal SQL queries; assert the count grows."""
    node.query("SYSTEM FLUSH LOGS query_log")
    for _ in range(retry_count):
        count_after = int(node.query(QUERY_LOG_COUNT_SQL).strip())
        if count_after > count_before:
            return
        time.sleep(sleep_time)
    raise AssertionError(
        f"Expected query_log count to increase from {count_before}, got {count_after}"
    )


cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/prometheus.xml",
        "configs/config.d/query_log.xml",
    ],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
    handle_prometheus_remote_write=(9093, "/write"),
)


def send_test_data_to_node():
    """Send minimal test data via remote write so a later PromQL query returns data."""
    time_series = [({"__name__": "up", "job": "prometheus"}, {1753176654.832: 1})]
    protobuf = convert_time_series_to_protobuf(time_series)
    send_protobuf_to_remote_write(node.ip_address, 9093, "/write", protobuf)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        node.query("CREATE TABLE prometheus ENGINE=TimeSeries")
        send_test_data_to_node()
        yield cluster
    finally:
        cluster.shutdown()


def test_query_api_appears_in_query_log_with_read_rows():
    """
    After a Prometheus Query API (/api/v1/query) request, there should be a row in
    system.query_log with type = 'QueryFinish', read_rows > 0, and read_bytes > 0.
    """
    timestamp = 1753176757.89
    promql = "up"

    node.query("SYSTEM FLUSH LOGS query_log")
    count_before = int(node.query(QUERY_LOG_COUNT_SQL).strip())

    escaped_query = urllib.parse.quote_plus(promql, safe="")
    url = f"http://{node.ip_address}:9093/api/v1/query?query={escaped_query}&time={timestamp}"
    response = get_response_to_http_api(url)
    extract_data_from_http_api_response(response)

    assert_query_log_count_increased(count_before)


def test_query_range_api_appears_in_query_log_with_read_rows():
    """
    After a Prometheus query_range API (/api/v1/query_range) request, there should
    be a row in system.query_log with type = 'QueryFinish', read_rows > 0, and
    read_bytes > 0.
    """
    node.query("SYSTEM FLUSH LOGS query_log")
    count_before = int(node.query(QUERY_LOG_COUNT_SQL).strip())

    escaped_query = urllib.parse.quote_plus("up", safe="")
    url = (
        f"http://{node.ip_address}:9093/api/v1/query_range"
        f"?query={escaped_query}&start=1753176650&end=1753176760&step=15"
    )
    response = get_response_to_http_api(url)
    extract_data_from_http_api_response(response)

    assert_query_log_count_increased(count_before)
