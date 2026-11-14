"""
Integration tests that assert Prometheus HTTP handler operations are reflected in
system.query_log: Query API requests (/api/v1/query and /api/v1/query_range) with
read_rows/read_bytes and remote-write requests (/write) with written_rows/written_bytes.
Also checks that a traced Query API request is reflected in system.opentelemetry_span_log.
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


def timeseries_metrics_has_metric_family_sql(table, metric_family):
    return (
        f"SELECT count() > 0 FROM timeSeriesMetricFamilies({table}) "
        f"WHERE metric_family = '{metric_family}'"
    )


def timeseries_data_has_metric_sql(table, metric_name):
    return (
        f"SELECT count() > 0 FROM timeSeriesData({table}) AS data "
        f"JOIN timeSeriesTags({table}) AS tags ON data.id = tags.id "
        f"WHERE tags.metric_name = '{metric_name}'"
    )


def span_log_trace_filter_sql(trace_id):
    return f"trace_id = UUIDNumToString(toFixedString(unhex('{trace_id}'), 16))"


def span_log_handler_span_sql(trace_id):
    return (
        "SYSTEM FLUSH LOGS opentelemetry_span_log; "
        "SELECT count(), any(kind), any(hex(parent_span_id)), any(attribute['http.method']), "
        "any(startsWith(attribute['clickhouse.uri'], '/api/v1/query')) "
        f"FROM system.opentelemetry_span_log WHERE {span_log_trace_filter_sql(trace_id)} "
        "AND operation_name = 'PrometheusRequestHandler'"
    )


def span_log_query_span_under_handler_sql(trace_id):
    return (
        "SYSTEM FLUSH LOGS opentelemetry_span_log; "
        "SELECT count() >= 1, any(attribute['clickhouse.tracestate']), any(attribute['db.statement'] != '') "
        f"FROM system.opentelemetry_span_log WHERE {span_log_trace_filter_sql(trace_id)} "
        "AND operation_name = 'query' AND parent_span_id IN ("
        f"SELECT span_id FROM system.opentelemetry_span_log WHERE {span_log_trace_filter_sql(trace_id)} "
        "AND operation_name = 'PrometheusRequestHandler')"
    )


def span_log_handler_span_uri_sql(trace_id, secret):
    return (
        "SYSTEM FLUSH LOGS opentelemetry_span_log; "
        f"SELECT count(), any(position(attribute['clickhouse.uri'], '{secret}') > 0), "
        "any(position(attribute['clickhouse.uri'], '[HIDDEN]') > 0), any(attribute['clickhouse.http_status'] = '200') "
        f"FROM system.opentelemetry_span_log WHERE {span_log_trace_filter_sql(trace_id)} "
        "AND operation_name = 'PrometheusRequestHandler'"
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
        "configs/config.d/query_masking_rules.xml",
        "configs/config.d/opentelemetry_span_log.xml",
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


def test_query_api_traced_request_appears_in_span_log():
    """
    A Prometheus Query API request carrying W3C trace context headers produces a
    `PrometheusRequestHandler` server span with the given parent, and the `query` span
    of the SQL translated from PromQL is its child and carries the tracestate.
    """
    trace_id = uuid.uuid4().hex
    parent_span_id = "5150000000000515"
    tracestate = "some custom state"

    escaped_query = urllib.parse.quote_plus("up", safe="")
    url = f"http://{node.ip_address}:9093/api/v1/query?query={escaped_query}&time=1753176757.89"
    response = get_response_to_http_api(
        url,
        headers={
            "traceparent": f"00-{trace_id}-{parent_span_id}-01",
            "tracestate": tracestate,
        },
    )
    extract_data_from_http_api_response(response)

    # The handler span is written after the response has been sent, so retry.
    assert_eq_with_retry(
        node,
        span_log_handler_span_sql(trace_id),
        f"1\tSERVER\t{parent_span_id}\tGET\t1\n",
        retry_count=30,
        sleep_time=1,
    )
    assert_eq_with_retry(
        node,
        span_log_query_span_under_handler_sql(trace_id),
        f"1\t{tracestate}\t1\n",
        retry_count=30,
        sleep_time=1,
    )


# The dedicated `query` handler and the combined `api_v1` handler, which selects the implementation after the URI is recorded.
@pytest.mark.parametrize("path_prefix", ["", "/combined"])
@pytest.mark.parametrize(
    "endpoint, query_param_name, promql, expect_ok",
    [
        ("/api/v1/query", "query", 'up{job="prometheus_masking_secret"}', True),
        # A percent-encoded parameter name is decoded to `query` by the server, so it must be hidden too.
        ("/api/v1/query", "%71uery", 'up{job="prometheus_masking_secret"}', True),
        # The URI is recorded before the query is parsed, so an error response must hide it as well.
        ("/api/v1/query", "query", 'up{job="prometheus_masking_secret"', False),
        ("/api/v1/series", "match[]", 'up{job="prometheus_masking_secret"}', True),
    ],
)
def test_query_api_traced_request_hides_promql_in_span_log(path_prefix, endpoint, query_param_name, promql, expect_ok):
    """
    The PromQL text is a query parameter of the request URI, which `query_masking_rules` do not cover,
    so the handler span must record the URI with the PromQL parameters hidden.
    """
    secret = "prometheus_masking_secret"
    trace_id = uuid.uuid4().hex

    escaped_query = urllib.parse.quote_plus(promql, safe="")
    url = f"http://{node.ip_address}:9093{path_prefix}{endpoint}?{query_param_name}={escaped_query}&time=1753176757.89"
    response = get_response_to_http_api(
        url, headers={"traceparent": f"00-{trace_id}-5150000000000515-01"}
    )
    assert (response.status_code == requests.codes.ok) == expect_ok, response.text

    assert_eq_with_retry(
        node,
        span_log_handler_span_uri_sql(trace_id, secret),
        f"1\t0\t1\t{int(expect_ok)}\n",
        retry_count=30,
        sleep_time=1,
    )


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


def test_remote_write_skips_time_series_without_metric_name():
    """
    A timeseries without a non-empty __name__ label is skipped and counted,
    and the rest of the batch, including its metadata, is still written.
    """
    metric_name = "skip_nameless_test"
    skipped_series_sql = (
        "SELECT sum(value) FROM system.events "
        "WHERE event = 'PrometheusRemoteWriteSkippedSeries'"
    )
    skipped_before = int(node.query(skipped_series_sql))

    write_request = convert_time_series_to_protobuf(
        [
            ({"job": metric_name}, {1753176720.0: 1}),
            ({"__name__": metric_name, "job": metric_name}, {1753176720.0: 2}),
        ]
    )
    metadata = convert_metrics_metadata_to_protobuf(
        [(metric_name, "GAUGE", "Skip nameless test metric.", "")]
    )
    write_request.metadata.extend(metadata.metadata)

    response = get_response_to_remote_write(
        node.ip_address, 9093, "/write", write_request
    )
    assert response.status_code == requests.codes.no_content

    # Only the named series and its sample are stored.
    assert_eq_with_retry(
        node,
        f"SELECT tags.metric_name, arrayJoin(data.samples).2 "
        f"FROM timeSeriesData(prometheus) AS data "
        f"JOIN timeSeriesTags(prometheus) AS tags ON data.id = tags.id "
        f"WHERE tags.tags['job'] = '{metric_name}'",
        f"{metric_name}\t2\n",
    )
    assert (
        node.query(
            f"SELECT count() FROM timeSeriesTags(prometheus) "
            f"WHERE tags['job'] = '{metric_name}'"
        )
        == "1\n"
    )
    assert_eq_with_retry(
        node,
        timeseries_metrics_has_metric_family_sql("prometheus", metric_name),
        "1\n",
    )
    assert int(node.query(skipped_series_sql)) == skipped_before + 1


def test_remote_write_skips_time_series_without_metric_name_with_bad_labels():
    """
    As in Prometheus, a timeseries without __name__ is skipped even if its other
    labels are invalid: an empty label name or one label name given twice.
    """
    metric_name = "skip_nameless_bad_labels_test"
    skipped_series_sql = (
        "SELECT sum(value) FROM system.events "
        "WHERE event = 'PrometheusRemoteWriteSkippedSeries'"
    )
    skipped_before = int(node.query(skipped_series_sql))

    write_request = remote_pb2.WriteRequest()
    for labels in [
        [("", "x")],
        [("job", "a"), ("job", "b")],
        [("__name__", metric_name), ("job", metric_name)],
    ]:
        timeseries = types_pb2.TimeSeries()
        for name, value in labels:
            timeseries.labels.append(types_pb2.Label(name=name, value=value))
        timeseries.samples.append(
            types_pb2.Sample(timestamp=1753176722 * 1000, value=1)
        )
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
    assert int(node.query(skipped_series_sql)) == skipped_before + 2


def test_remote_write_skips_named_time_series_with_bad_labels():
    """
    As in Prometheus, a named timeseries with an empty label name or one label
    name given two values is skipped, and the rest of the batch is written.
    """
    metric_name = "skip_named_bad_labels_test"
    skipped_series_sql = (
        "SELECT sum(value) FROM system.events "
        "WHERE event = 'PrometheusRemoteWriteSkippedSeries'"
    )
    skipped_before = int(node.query(skipped_series_sql))

    write_request = remote_pb2.WriteRequest()
    for labels in [
        [("__name__", metric_name), ("", "x")],
        [("__name__", metric_name), ("job", "a"), ("job", "b")],
        [("__name__", metric_name), ("__name__", "skip_named_other_name")],
        [("__name__", metric_name), ("job", metric_name)],
    ]:
        timeseries = types_pb2.TimeSeries()
        for name, value in labels:
            timeseries.labels.append(types_pb2.Label(name=name, value=value))
        timeseries.samples.append(
            types_pb2.Sample(timestamp=1753176723 * 1000, value=1)
        )
        write_request.timeseries.append(timeseries)

    response = get_response_to_remote_write(
        node.ip_address, 9093, "/write", write_request
    )
    assert response.status_code == requests.codes.no_content

    # Only the valid series is stored.
    assert_eq_with_retry(
        node,
        timeseries_data_has_metric_sql("prometheus", metric_name),
        "1\n",
    )
    assert (
        node.query(
            f"SELECT groupArray(tags['job']) FROM timeSeriesTags(prometheus) "
            f"WHERE metric_name IN ('{metric_name}', 'skip_named_other_name')"
        )
        == f"['{metric_name}']\n"
    )
    assert int(node.query(skipped_series_sql)) == skipped_before + 3


def test_remote_write_skipped_time_series_do_not_reach_materialized_views():
    """
    A skipped timeseries gets no row in the insert, so a materialized view on the
    TimeSeries table does not see it and cannot fail the request because of it.
    """
    metric_name = "skip_view_test"
    node.query(
        "CREATE TABLE skip_view_dst "
        "(metric_name String, metric_family String, num_samples UInt64) "
        "ENGINE=MergeTree ORDER BY ()"
    )
    # The view fails the insert on a row that has neither a metric name nor metadata.
    node.query(
        "CREATE MATERIALIZED VIEW skip_view TO skip_view_dst AS "
        "SELECT metric_name, metric_family, length(samples) AS num_samples "
        "FROM prometheus "
        "WHERE NOT throwIf(metric_name = '' AND metric_family = '')"
    )
    try:
        write_request = remote_pb2.WriteRequest()
        for labels in [
            [("job", metric_name)],
            [("__name__", metric_name), ("job", metric_name)],
            [("__name__", metric_name), ("job", "a"), ("job", "b")],
        ]:
            timeseries = types_pb2.TimeSeries()
            for name, value in labels:
                timeseries.labels.append(types_pb2.Label(name=name, value=value))
            timeseries.samples.append(
                types_pb2.Sample(timestamp=1753176724 * 1000, value=1)
            )
            write_request.timeseries.append(timeseries)
        metadata = convert_metrics_metadata_to_protobuf(
            [(metric_name, "GAUGE", "Skip view test metric.", "")]
        )
        write_request.metadata.extend(metadata.metadata)

        response = get_response_to_remote_write(
            node.ip_address, 9093, "/write", write_request
        )
        assert response.status_code == requests.codes.no_content

        # The view gets the valid series and the metadata, but no skipped series.
        view_rows_sql = "SELECT * FROM skip_view_dst ORDER BY ALL"
        expected_view_rows = f"\t{metric_name}\t0\n{metric_name}\t\t1\n"
        assert node.query(view_rows_sql) == expected_view_rows

        # A request with only skipped series is accepted and adds nothing to the view.
        write_request = convert_time_series_to_protobuf(
            [({"job": metric_name}, {1753176724.0: 1})]
        )
        response = get_response_to_remote_write(
            node.ip_address, 9093, "/write", write_request
        )
        assert response.status_code == requests.codes.no_content
        assert node.query(view_rows_sql) == expected_view_rows
    finally:
        node.query("DROP VIEW skip_view")
        node.query("DROP TABLE skip_view_dst")


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


def query_log_comment(query_id):
    return node.query(
        f"SELECT log_comment FROM system.query_log "
        f"WHERE type = 'QueryFinish' AND query_id = '{query_id}'"
    )


def run_instant_query(promql, query_id, extra_params=""):
    escaped_query = urllib.parse.quote_plus(promql, safe="")
    url = (
        f"http://{node.ip_address}:9093/api/v1/query"
        f"?query={escaped_query}&time=1753176757.89{extra_params}"
    )
    response = get_response_to_http_api(
        url, headers={"X-ClickHouse-Query-Id": query_id}
    )
    extract_data_from_http_api_response(response)
    assert_query_log_has_finish_for_query_id(query_id)


def test_query_api_records_promql_as_log_comment():
    """The query_log row of a Query API request carries the PromQL text in log_comment."""
    promql = 'sum by (job) (up{job="prometheus"})'
    query_id = f"prometheus-query-log-test-{uuid.uuid4()}"
    run_instant_query(promql, query_id)
    assert query_log_comment(query_id) == f"{promql}\n"


def test_query_api_keeps_user_log_comment():
    """A log_comment passed with the request is kept instead of the PromQL text."""
    query_id = f"prometheus-query-log-test-{uuid.uuid4()}"
    run_instant_query("up", query_id, "&log_comment=my_dashboard")
    assert query_log_comment(query_id) == "my_dashboard\n"


def test_query_api_keeps_empty_user_log_comment():
    """An empty log_comment passed with the request is kept too."""
    query_id = f"prometheus-query-log-test-{uuid.uuid4()}"
    run_instant_query("up", query_id, "&log_comment=")
    assert query_log_comment(query_id) == "\n"


def test_query_api_masks_promql_in_log_comment():
    """The PromQL text in log_comment is masked by query_masking_rules, like the query text."""
    query_id = f"prometheus-query-log-test-{uuid.uuid4()}"
    run_instant_query('up{job="prometheus_masking_secret"}', query_id)
    assert (
        node.query(
            f"SELECT log_comment, Settings['log_comment'] FROM system.query_log "
            f"WHERE type = 'QueryFinish' AND query_id = '{query_id}'"
        )
        == 'up{job="******"}\tup{job="******"}\n'
    )
