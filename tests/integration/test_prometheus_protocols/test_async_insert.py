import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
from .prometheus_test_utils import (
    convert_metrics_metadata_to_protobuf,
    convert_time_series_to_protobuf,
    execute_query_via_http_api,
    get_response_to_remote_write,
    send_protobuf_to_remote_write,
)


cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def cleanup_after_test():
    try:
        yield
    finally:
        node.query("DROP TABLE IF EXISTS prometheus SYNC")
        node.query("DROP TABLE IF EXISTS samples SYNC")


def get_async_insert_query_count():
    return int(
        node.query(
            "SELECT sum(value) FROM system.events WHERE event = 'AsyncInsertQuery'"
        )
    )


def test_async_insert_acknowledged_after_flush():
    node.query("CREATE TABLE prometheus ENGINE=TimeSeries")

    async_insert_queries_before = get_async_insert_query_count()

    time_series = [
        (
            {"__name__": "async_metric", "job": "test"},
            {1724112000: 1.5, 1724112015: 2.5},
        )
    ]
    send_protobuf_to_remote_write(
        node.ip_address,
        9093,
        "/write?async_insert=1",
        convert_time_series_to_protobuf(time_series),
    )

    metrics_metadata = [("async_metric", "GAUGE", "Test metric", "seconds")]
    send_protobuf_to_remote_write(
        node.ip_address,
        9093,
        "/write?async_insert=1",
        convert_metrics_metadata_to_protobuf(metrics_metadata),
    )

    assert node.query("SELECT count() FROM timeSeriesData(prometheus)") == "2\n"
    assert (
        node.query(
            "SELECT tags['job'] FROM timeSeriesTags(prometheus) "
            "WHERE metric_name = 'async_metric'"
        )
        == "test\n"
    )
    assert (
        node.query(
            "SELECT type, help, unit FROM timeSeriesMetrics(prometheus) "
            "WHERE metric_family_name = 'async_metric'"
        )
        == "gauge\tTest metric\tseconds\n"
    )

    assert get_async_insert_query_count() == async_insert_queries_before + 2


def test_async_insert_no_acknowledgement_on_failure():
    node.query(
        "CREATE TABLE samples (id UUID, timestamp DateTime64(3), value Float64, "
        "CONSTRAINT reject_all CHECK value < 0) ENGINE=MergeTree ORDER BY (id, timestamp)"
    )
    node.query("CREATE TABLE prometheus ENGINE=TimeSeries DATA samples")

    time_series = [({"__name__": "async_metric"}, {1724112000: 1.5})]
    response = get_response_to_remote_write(
        node.ip_address,
        9093,
        "/write?async_insert=1",
        convert_time_series_to_protobuf(time_series),
    )

    assert not response.ok
    assert "VIOLATED_CONSTRAINT" in response.text
    assert node.query("SELECT count() FROM samples") == "0\n"


def test_async_insert_flush_timeout_returns_503():
    node.query("CREATE TABLE prometheus ENGINE=TimeSeries")

    # With a zero wait timeout and a 3-second flush deadline the wait always times out
    # before the flush happens.
    time_series = [({"__name__": "timeout_metric"}, {1724112000: 1.5})]
    response = get_response_to_remote_write(
        node.ip_address,
        9093,
        "/write?async_insert=1&wait_for_async_insert_timeout=0"
        "&async_insert_use_adaptive_busy_timeout=0&async_insert_busy_timeout_max_ms=3000",
        convert_time_series_to_protobuf(time_series),
    )

    # The remote-write spec makes senders drop data on 4xx statuses (other than 429),
    # so the wait timeout must map to a retryable 503, not 408.
    assert response.status_code == 503
    assert "Wait for asynchronous insert timeout" in response.text

    # The data stays in the server's asynchronous insert queue and is flushed later.
    assert_eq_with_retry(
        node, "SELECT count() FROM timeSeriesSamples(prometheus)", "1", retry_count=60
    )

    # A PromQL instant query at the sample's timestamp returns the flushed data.
    assert execute_query_via_http_api(
        node.ip_address, 9093, "/api/v1/query", "timeout_metric", timestamp=1724112000
    ) == (
        '{"resultType": "vector", "result": '
        '[{"metric": {"__name__": "timeout_metric"}, "value": [1724112000, "1.5"]}]}'
    )
