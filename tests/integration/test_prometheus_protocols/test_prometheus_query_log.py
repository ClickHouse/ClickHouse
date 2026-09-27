"""
Integration tests that assert Prometheus HTTP handler operations are reflected in
system.query_log: Query API requests (/api/v1/query and /api/v1/query_range) with
read_rows/read_bytes and remote-write requests (/write) with written_rows/written_bytes.
"""

import urllib.parse
import uuid

import pytest
import requests

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
from .prometheus_test_utils import (
    convert_metrics_metadata_to_protobuf,
    convert_time_series_to_protobuf,
    get_response_to_http_api,
    get_response_to_remote_write,
    send_protobuf_to_remote_write,
    extract_data_from_http_api_response,
)
from .prometheus_test_utils import remote_pb2, types_pb2


def query_log_has_finish_for_query_id_sql(query_id):
    return (
        f"SELECT count() > 0 FROM system.query_log "
        f"WHERE type = 'QueryFinish' AND query_id = '{query_id}' "
        f"AND read_rows > 0 AND read_bytes > 0"
    )


def query_log_has_single_finish_with_written_rows_sql(query_id):
    return (
        f"SELECT count() = 1 AND countIf("
        f"query_id = '{query_id}' "
        f"AND written_rows > 0 AND written_bytes > 0 "
        f"AND is_internal = 0 AND is_initial_query = 1) = 1 "
        f"FROM system.query_log "
        f"WHERE type = 'QueryFinish' AND initial_query_id = '{query_id}'"
    )


def timeseries_metrics_has_metric_family_sql(table, metric_family_name):
    return (
        f"SELECT count() > 0 FROM timeSeriesMetrics({table}) "
        f"WHERE metric_family_name = '{metric_family_name}'"
    )


def timeseries_data_has_metric_sql(table, metric_name):
    return (
        f"SELECT count() > 0 FROM timeSeriesData({table}) AS data "
        f"JOIN timeSeriesTags({table}) AS tags ON data.id = tags.id "
        f"WHERE tags.metric_name = '{metric_name}'"
    )


def assert_query_log_has_finish_for_query_id(query_id, retry_count=30, sleep_time=1):
    """Assert the Prometheus request produced at least one correlated QueryFinish row."""
    node.query("SYSTEM FLUSH LOGS query_log")
    assert_eq_with_retry(
        node,
        query_log_has_finish_for_query_id_sql(query_id),
        "1\n",
        retry_count=retry_count,
        sleep_time=sleep_time,
    )


def assert_query_log_has_single_finish_with_written_rows(
    query_id, retry_count=30, sleep_time=1
):
    """Assert the remote write produced one QueryFinish row with written rows/bytes."""
    node.query("SYSTEM FLUSH LOGS query_log")
    assert_eq_with_retry(
        node,
        query_log_has_single_finish_with_written_rows_sql(query_id),
        "1\n",
        retry_count=retry_count,
        sleep_time=sleep_time,
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
    system.query_log with type = 'QueryFinish', read_rows > 0, and read_bytes > 0,
    correlated to the request via X-ClickHouse-Query-Id.
    """
    timestamp = 1753176757.89
    promql = "up"
    query_id = f"prometheus-query-log-test-{uuid.uuid4()}"

    escaped_query = urllib.parse.quote_plus(promql, safe="")
    url = f"http://{node.ip_address}:9093/api/v1/query?query={escaped_query}&time={timestamp}"
    response = get_response_to_http_api(
        url, headers={"X-ClickHouse-Query-Id": query_id}
    )
    extract_data_from_http_api_response(response)

    assert_query_log_has_finish_for_query_id(query_id)


def test_remote_write_appears_in_query_log_with_written_rows():
    """
    A remote write to /write should produce one parent TimeSeries row in
    system.query_log with written_rows and written_bytes.
    """
    query_id = f"prometheus-query-log-test-{uuid.uuid4()}"

    time_series = [
        (
            {"__name__": "remote_write_query_log_test", "job": "test"},
            {1753176700.0: 42},
        )
    ]
    protobuf = convert_time_series_to_protobuf(time_series)
    send_protobuf_to_remote_write(
        node.ip_address,
        9093,
        "/write",
        protobuf,
        headers={"X-ClickHouse-Query-Id": query_id},
    )

    assert_query_log_has_single_finish_with_written_rows(query_id)


def test_remote_write_metadata_appears_in_query_log_with_written_rows():
    """
    A remote write to /write containing metrics metadata should produce one parent
    TimeSeries row in system.query_log with written_rows and written_bytes.
    """
    query_id = f"prometheus-query-log-test-{uuid.uuid4()}"

    metrics_metadata = [
        (
            "remote_write_query_log_test",
            "GAUGE",
            "Test metric for query_log tracking.",
            "",
        )
    ]
    protobuf = convert_metrics_metadata_to_protobuf(metrics_metadata)
    send_protobuf_to_remote_write(
        node.ip_address,
        9093,
        "/write",
        protobuf,
        headers={"X-ClickHouse-Query-Id": query_id},
    )

    assert_query_log_has_single_finish_with_written_rows(query_id)


def test_remote_write_time_series_and_metadata_together_are_stored():
    """
    A single remote write carrying both timeseries and metadata should produce one
    parent TimeSeries QueryFinish row, store the sample in the data inner table, and
    store the metadata in the metrics inner table. This exercises the mixed block
    where metadata rows are padded alongside the time series.
    """
    query_id = f"prometheus-query-log-test-{uuid.uuid4()}"
    metric_name = "remote_write_combined_test"

    write_request = convert_time_series_to_protobuf(
        [({"__name__": metric_name, "job": "test"}, {1753176710.0: 7})]
    )
    metadata = convert_metrics_metadata_to_protobuf(
        [(metric_name, "GAUGE", "Combined test metric.", "")]
    )
    write_request.metadata.extend(metadata.metadata)

    send_protobuf_to_remote_write(
        node.ip_address,
        9093,
        "/write",
        write_request,
        headers={"X-ClickHouse-Query-Id": query_id},
    )

    assert_query_log_has_single_finish_with_written_rows(query_id)

    assert_eq_with_retry(
        node,
        timeseries_data_has_metric_sql("prometheus", metric_name),
        "1\n",
    )
    assert_eq_with_retry(
        node,
        timeseries_metrics_has_metric_family_sql("prometheus", metric_name),
        "1\n",
    )


def test_remote_write_rejects_time_series_without_metric_name():
    """
    A remote write whose timeseries has no non-empty __name__ label must fail
    instead of being accepted and dropped.
    """
    protobuf = convert_time_series_to_protobuf(
        [({"job": "test"}, {1753176720.0: 1})]
    )
    response = get_response_to_remote_write(
        node.ip_address, 9093, "/write", protobuf
    )
    assert response.status_code != requests.codes.no_content
    assert "Metric name is missing" in response.text


def test_remote_write_accepts_empty_then_nonempty_metric_name():
    """
    An empty __name__ label before a non-empty one must still be accepted: the
    empty value is left in tags for TimeSeriesSink empty-value handling, and
    the first non-empty __name__ becomes the metric name.
    """
    metric_name = "empty_then_nonempty_name_test"
    write_request = remote_pb2.WriteRequest()
    timeseries = types_pb2.TimeSeries()
    timeseries.labels.append(types_pb2.Label(name="__name__", value=""))
    timeseries.labels.append(types_pb2.Label(name="__name__", value=metric_name))
    timeseries.labels.append(types_pb2.Label(name="job", value="test"))
    timeseries.samples.append(types_pb2.Sample(timestamp=1753176721 * 1000, value=7))
    write_request.timeseries.append(timeseries)

    response = get_response_to_remote_write(
        node.ip_address, 9093, "/write", write_request
    )
    assert response.status_code == requests.codes.no_content
    assert_eq_with_retry(
        node,
        timeseries_data_has_metric_sql("prometheus", metric_name),
        "1\n",
    )


def test_query_range_api_appears_in_query_log_with_read_rows():
    """
    After a Prometheus query_range API (/api/v1/query_range) request, there should
    be a row in system.query_log with type = 'QueryFinish', read_rows > 0, and
    read_bytes > 0, correlated to the request via X-ClickHouse-Query-Id.
    """
    query_id = f"prometheus-query-log-test-{uuid.uuid4()}"

    escaped_query = urllib.parse.quote_plus("up", safe="")
    url = (
        f"http://{node.ip_address}:9093/api/v1/query_range"
        f"?query={escaped_query}&start=1753176650&end=1753176760&step=15"
    )
    response = get_response_to_http_api(
        url, headers={"X-ClickHouse-Query-Id": query_id}
    )
    extract_data_from_http_api_response(response)

    assert_query_log_has_finish_for_query_id(query_id)
