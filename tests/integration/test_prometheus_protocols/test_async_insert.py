import time
import uuid

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


def get_async_insert_query_count():
    return int(
        node.query(
            "SELECT sum(value) FROM system.events WHERE event = 'AsyncInsertQuery'"
        )
    )


def get_profile_event(event):
    return int(
        node.query(
            f"SELECT sum(value) FROM system.events WHERE event = '{event}'"
        )
    )


def test_async_insert_metric_families_deduplication_cache():
    node.query("CREATE TABLE prometheus ENGINE=TimeSeries")

    metadata = [("cached_metric", "GAUGE", "Test metric", "seconds")]
    protobuf = convert_metrics_metadata_to_protobuf(metadata)
    hits_before = get_profile_event(
        "TimeSeriesMetricFamiliesDeduplicationCacheHits"
    )

    # Every remote write is acknowledged after its data is flushed,
    # so the second one finds the metric family in the deduplication cache.
    for _ in range(2):
        send_protobuf_to_remote_write(
            node.ip_address, 9093, "/write?async_insert=1", protobuf
        )

    assert (
        node.query("SELECT count() FROM timeSeriesMetricFamilies(prometheus)")
        == "1\n"
    )
    assert (
        get_profile_event("TimeSeriesMetricFamiliesDeduplicationCacheHits")
        == hits_before + 1
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

    assert node.query("SELECT sum(length(samples)) FROM timeSeriesData(prometheus)") == "2\n"
    assert (
        node.query(
            "SELECT tags['job'] FROM timeSeriesTags(prometheus) "
            "WHERE metric_name = 'async_metric'"
        )
        == "test\n"
    )
    assert (
        node.query(
            "SELECT type, help, unit FROM timeSeriesMetricFamilies(prometheus) "
            "WHERE metric_family = 'async_metric'"
        )
        == "gauge\tTest metric\tseconds\n"
    )

    assert get_async_insert_query_count() == async_insert_queries_before + 2


def test_async_insert_no_acknowledgement_on_failure():
    # Every bucket of samples goes to its own partition of the samples table, and the request limits
    # an insert block to one partition, so a request with two samples in different buckets fails.
    node.query(
        "CREATE TABLE prometheus ENGINE=TimeSeries "
        "SAMPLES INNER ENGINE=AggregatingMergeTree PARTITION BY bucket ORDER BY (id, bucket)"
    )

    time_series = [({"__name__": "async_metric"}, {1724112000: 1.5, 1724115601: 2.5})]
    response = get_response_to_remote_write(
        node.ip_address,
        9093,
        "/write?async_insert=1&max_partitions_per_insert_block=1",
        convert_time_series_to_protobuf(time_series),
    )

    assert not response.ok
    assert "TOO_MANY_PARTS" in response.text
    assert node.query("SELECT count() FROM timeSeriesSamples(prometheus)") == "0\n"


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


def test_async_insert_flush_timeout_is_clamped():
    node.query("CREATE TABLE prometheus ENGINE=TimeSeries")

    def remote_write(metric_name, wait_for_async_insert_timeout):
        # A 3-second flush deadline: a wait shorter than that times out, a longer one does not.
        return get_response_to_remote_write(
            node.ip_address,
            9093,
            f"/write?async_insert=1&wait_for_async_insert_timeout={wait_for_async_insert_timeout}"
            "&async_insert_use_adaptive_busy_timeout=0&async_insert_busy_timeout_max_ms=3000",
            convert_time_series_to_protobuf(
                [({"__name__": metric_name}, {1724112000: 1.5})]
            ),
        )

    # 1e10 seconds is accepted by the setting but overflows the millisecond-to-nanosecond
    # conversion that `wait_for` performs; capped at one year it outlasts the flush.
    response = remote_write("clamped_pos_metric", 10000000000)
    assert response.status_code == 204
    # No retry here: the acknowledgement means the wait outlasted the flush.
    assert node.query("SELECT count() FROM timeSeriesSamples(prometheus)") == "1\n"

    # An in-range timeout is used as given, so the reported number is the one that applied.
    response = remote_write("in_range_metric", 0.001)
    assert response.status_code == 503
    assert "Wait for asynchronous insert timeout (1 ms) exceeded" in response.text

    # A timed-out wait keeps the data in the queue, so every sample arrives after the flush.
    assert_eq_with_retry(
        node, "SELECT count() FROM timeSeriesSamples(prometheus)", "2", retry_count=60
    )


def test_async_insert_matches_sync_and_skips_handler_target_pipelines():
    ts1 = (int(time.time()) // 3600) * 3600 - 120
    ts2 = ts1 + 15
    time_series = [
        ({"__name__": "parity_metric", "job": "api"}, {ts1: 1.5, ts2: 2.5}),
        ({"__name__": "parity_up", "job": "api"}, {ts1: 1.0}),
    ]
    metrics_metadata = [
        ("parity_metric", "COUNTER", "Parity test metric", "requests"),
        ("parity_up", "GAUGE", "Target up", ""),
    ]
    ts_pb = convert_time_series_to_protobuf(time_series)
    meta_pb = convert_metrics_metadata_to_protobuf(metrics_metadata)

    def snapshot_inner_tables():
        tables = sorted(
            node.query(
                "SELECT name FROM system.tables "
                "WHERE database = 'default' AND name LIKE '.inner_id.%'"
            ).splitlines()
        )
        assert len(tables) == 5, tables
        result = {}
        for table in tables:
            kind = table.split(".")[2]
            result[kind] = node.query(
                f"SELECT count(), groupBitXor(cityHash64(*)) FROM default.`{table}` FINAL"
            ).strip()
        return result

    def handler_inner_table_count(query_id):
        node.query("SYSTEM FLUSH LOGS query_log")
        return int(
            node.query(
                "SELECT arrayCount(t -> t LIKE '%.inner_id.%', tables) "
                f"FROM system.query_log WHERE type = 'QueryFinish' AND query_id = '{query_id}'"
            ).strip()
        )

    node.query("CREATE TABLE prometheus ENGINE=TimeSeries")
    qid_async_ts = f"rw-async-ts-{uuid.uuid4()}"
    qid_async_meta = f"rw-async-meta-{uuid.uuid4()}"
    send_protobuf_to_remote_write(
        node.ip_address, 9093, "/write?async_insert=1&log_queries=1", ts_pb,
        headers={"X-ClickHouse-Query-Id": qid_async_ts},
    )
    send_protobuf_to_remote_write(
        node.ip_address, 9093, "/write?async_insert=1&log_queries=1", meta_pb,
        headers={"X-ClickHouse-Query-Id": qid_async_meta},
    )
    async_snapshot = snapshot_inner_tables()
    assert handler_inner_table_count(qid_async_ts) == 0
    assert handler_inner_table_count(qid_async_meta) == 0

    node.query("DROP TABLE prometheus SYNC")
    node.query("CREATE TABLE prometheus ENGINE=TimeSeries")
    qid_sync_ts = f"rw-sync-ts-{uuid.uuid4()}"
    qid_sync_meta = f"rw-sync-meta-{uuid.uuid4()}"
    send_protobuf_to_remote_write(
        node.ip_address, 9093, "/write?async_insert=0&log_queries=1", ts_pb,
        headers={"X-ClickHouse-Query-Id": qid_sync_ts},
    )
    send_protobuf_to_remote_write(
        node.ip_address, 9093, "/write?async_insert=0&log_queries=1", meta_pb,
        headers={"X-ClickHouse-Query-Id": qid_sync_meta},
    )
    sync_snapshot = snapshot_inner_tables()
    assert handler_inner_table_count(qid_sync_ts) == 4
    assert handler_inner_table_count(qid_sync_meta) == 1

    assert async_snapshot == sync_snapshot
    for kind, value in async_snapshot.items():
        count = int(value.split("\t")[0])
        assert count > 0, (kind, value)

