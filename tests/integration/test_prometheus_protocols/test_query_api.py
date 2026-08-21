import urllib
import uuid

import pytest
import requests

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
from .prometheus_test_utils import (
    convert_time_series_to_protobuf,
    execute_query_via_http_api,
    execute_range_query_via_http_api,
    extract_data_from_http_api_response,
    extract_error_from_http_api_response,
    get_response_to_http_api_query,
    send_protobuf_to_remote_write,
)


cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
    handle_prometheus_remote_read=(9093, "/read"),
    handle_prometheus_remote_write=(9093, "/write"),
)


# The query pipeline runs on its own threads and hands blocks to the HTTP thread through
# a small bounded buffer. To make an error land *after* some results have been written,
# the row limit must sit well above that buffer: `max_block_size=1` puts one series in
# each block, so the pipeline fills the buffer and then blocks, and cannot reach the limit
# until the HTTP thread has taken and written out a block. Without that margin the
# pipeline reaches the limit while the buffer is still unread, the error wins the race,
# and the response becomes a plain error envelope instead.
STREAM_ERROR_SERIES_COUNT = 32
STREAM_ERROR_ROW_LIMIT = 16


def send_to_clickhouse(time_series):
    protobuf = convert_time_series_to_protobuf(time_series)
    send_protobuf_to_remote_write(node.ip_address, 9093, "/write", protobuf)


def send_test_data():
    # `post_body_metric` is used by the GET-vs-POST tests.
    send_to_clickhouse(
        [({"__name__": "post_body_metric", "job": "test"}, {1000.0: 1.0, 1001.0: 2.0})]
    )
    # `foo` (3 series) is used by the tests that expect the error before any output.
    send_to_clickhouse(
        [
            ({"__name__": "foo", "shape": "square", "size": "s"}, {110: 4, 130: 40}),
            ({"__name__": "foo", "shape": "triangle", "size": "m"}, {110: 8, 120: 80}),
            ({"__name__": "foo", "shape": "circle", "size": "l"}, {110: 16, 130: 16, 150: 16}),
        ]
    )
    # `stream_error` is used by the tests that expect the error only after results have
    # already been written.
    send_to_clickhouse(
        [
            ({"__name__": "stream_error", "instance": f"i{i:02}"}, {110: i, 130: i * 2})
            for i in range(STREAM_ERROR_SERIES_COUNT)
        ]
    )


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        node.query("CREATE TABLE prometheus ENGINE=TimeSeries")
        node.query(
            "CREATE TABLE prometheus_seconds "
            "(time_series Array(Tuple(DateTime64(0), Float64))) ENGINE=TimeSeries"
        )
        node.query(
            "INSERT INTO prometheus_seconds (metric_name, tags, time_series) VALUES"
            " ('foo_seconds_old', {'shape': 'circle'}, [(toDateTime64(150, 0), 16)]),"
            " ('foo_seconds_exact', {'shape': 'circle'}, [(toDateTime64(151, 0), 17)])"
        )
        send_test_data()
        yield cluster
    finally:
        cluster.shutdown()


# `/api/v1/query` must accept params via a POST `application/x-www-form-urlencoded`
# body just as it does via the URL query string. Run the same instant query both
# ways and assert the response data is identical.
def test_query_post_urlencoded():
    host, port = node.ip_address, 9093
    query = "post_body_metric"
    t = 1000
    get_data = execute_query_via_http_api(host, port, "/api/v1/query", query, timestamp=t)
    url = f"http://{host}:{port}/api/v1/query"
    post_resp = requests.post(
        url,
        data={"query": query, "time": str(t)},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    post_data = extract_data_from_http_api_response(post_resp)
    assert get_data == post_data


# `/api/v1/query_range` must accept params via a POST `application/x-www-form-urlencoded`
# body just as it does via the URL query string. Run the same range query both
# ways and assert the response data is identical.
def test_range_query_post_urlencoded():
    host, port = node.ip_address, 9093
    query = "post_body_metric"
    start_s, end_s, step = 999, 1002, "1"
    get_data = execute_range_query_via_http_api(
        host, port, "/api/v1/query_range", query, start_s, end_s, step
    )
    url = f"http://{host}:{port}/api/v1/query_range"
    post_resp = requests.post(
        url,
        data={
            "query": query,
            "start": str(start_s),
            "end": str(end_s),
            "step": step,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    post_data = extract_data_from_http_api_response(post_resp)
    assert get_data == post_data


def test_range_query_rejects_non_positive_step_for_equal_start_and_end():
    for step in (0, -1):
        error = execute_range_query_via_http_api(
            node.ip_address,
            9093,
            "/api/v1/query_range",
            "vector(1)",
            10,
            10,
            step,
            expect_error=True,
        )
        assert "step must be positive" in error


def test_range_query_accepts_positive_step_for_equal_start_and_end():
    result = execute_range_query_via_http_api(
        node.ip_address,
        9093,
        "/api/v1/query_range",
        "post_body_metric",
        1000,
        1000,
        1,
    )
    assert result == '{"resultType": "matrix", "result": [{"metric": {"__name__": "post_body_metric", "job": "test"}, "values": [[1000, "1"]]}]}'


def test_format_query_get():
    response = requests.get(
        f"http://{node.ip_address}:9093/api/v1/format_query",
        params={"query": "foo/bar"},
    )
    assert response.status_code == requests.codes.ok, response.text
    data = response.json()
    assert data == {"status": "success", "data": "foo / bar"}


def test_format_query_allows_trailing_comment_without_newline():
    response = requests.get(
        f"http://{node.ip_address}:9093/api/v1/format_query",
        params={"query": "foo # comment"},
    )
    assert response.status_code == requests.codes.ok, response.text
    data = response.json()
    assert data == {"status": "success", "data": "foo"}


def test_format_query_uses_prometheus_string_escapes():
    query = r'label_replace(foo, "label", "\a\v\000\001\037\177", "source", "regex")'
    response = requests.get(
        f"http://{node.ip_address}:9093/api/v1/format_query",
        params={"query": query},
    )
    assert response.status_code == requests.codes.ok, response.text
    data = response.json()
    assert data == {"status": "success", "data": r'label_replace(foo, "label", "\a\v\x00\x01\x1f\x7f", "source", "regex")'}


def test_format_query_post_urlencoded():
    response = requests.post(
        f"http://{node.ip_address}:9093/dynamic_table/api/v1/format_query",
        data={"query": "# comment\nsum by (job) (rate(http_requests_total[5m]))"},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    assert response.status_code == requests.codes.ok, response.text
    data = response.json()
    assert data == {"status": "success", "data": "sum by (job) (rate(http_requests_total[5m]))"}


def test_format_query_rounds_timestamp_to_milliseconds():
    response = requests.get(
        f"http://{node.ip_address}:9093/api/v1/format_query",
        params={"query": "foo @ 1.23456789"},
    )
    assert response.status_code == requests.codes.ok, response.text
    data = response.json()
    assert data["status"] == "success"
    assert data["data"] == "foo @ 1.235"


def test_format_query_sorts_matchers_like_prometheus():
    response = requests.get(
        f"http://{node.ip_address}:9093/api/v1/format_query",
        params={"query": 'foo{z="last",a="first"}'},
    )
    assert response.status_code == requests.codes.ok, response.text
    data = response.json()
    assert data == {"status": "success", "data": 'foo{a="first",z="last"}'}


def test_format_query_formats_long_expressions_on_multiple_lines():
    query = 'label_replace(foo, "label", "this string is long enough to make the PromQL call exceed the line length limit", "source", "regex")'
    response = requests.get(
        f"http://{node.ip_address}:9093/api/v1/format_query",
        params={"query": query},
    )
    assert response.status_code == requests.codes.ok, response.text
    data = response.json()
    assert data == {
        "status": "success",
        "data": "label_replace(\n"
        "  foo,\n"
        '  "label",\n'
        '  "this string is long enough to make the PromQL call exceed the line length limit",\n'
        '  "source",\n'
        '  "regex"\n'
        ")",
    }


@pytest.mark.parametrize("query", ["", "foo +"])
def test_format_query_rejects_invalid_query(query):
    response = requests.get(
        f"http://{node.ip_address}:9093/api/v1/format_query",
        params={"query": query},
    )
    assert response.status_code == requests.codes.bad_request, response.text
    data = response.json()
    assert data["status"] == "error"
    assert data["errorType"] == "bad_data"


def test_format_query_does_not_require_a_time_series_table():
    response = requests.get(
        f"http://{node.ip_address}:9093/dynamic_table/api/v1/format_query",
        params={"query": "foo/bar"},
    )
    assert response.status_code == requests.codes.ok, response.text
    data = response.json()
    assert data == {"status": "success", "data": "foo / bar"}


def test_query_lookback_delta():
    query = 'foo{shape="circle"}'
    timestamp = 151

    expected = '{"resultType": "vector", "result": [{"metric": {"__name__": "foo", "shape": "circle", "size": "l"}, "value": [151, "16"]}]}'
    assert execute_query_via_http_api(node.ip_address, 9093, "/api/v1/query", query, timestamp=timestamp) == expected

    assert (
        execute_query_via_http_api(
            node.ip_address, 9093, "/api/v1/query", query, timestamp=timestamp,
            params={"lookback_delta": "500ms"},
        )
        == '{"resultType": "vector", "result": []}'
    )

    for lookback_delta in ("0", "-1"):
        assert (
            execute_query_via_http_api(
                node.ip_address, 9093, "/api/v1/query", query, timestamp=timestamp,
                params={"lookback_delta": lookback_delta},
            )
            == expected
        )

    error = execute_query_via_http_api(
        node.ip_address, 9093, "/api/v1/query", query, timestamp=timestamp,
        params={"lookback_delta": "banana"}, expect_error=True,
    )
    assert "Cannot parse duration" in error


def test_query_lookback_delta_low_timestamp_precision():
    old_sample = execute_query_via_http_api(
        node.ip_address, 9093, "/dynamic_table/api/v1/query",
        'foo_seconds_old{shape="circle"}', timestamp=151,
        params={"table": "prometheus_seconds", "lookback_delta": "500ms"},
    )
    assert old_sample == '{"resultType": "vector", "result": []}'

    exact_sample = execute_query_via_http_api(
        node.ip_address, 9093, "/dynamic_table/api/v1/query",
        'foo_seconds_exact{shape="circle"}', timestamp=151,
        params={"table": "prometheus_seconds", "lookback_delta": "500ms"},
    )
    assert exact_sample == '{"resultType": "vector", "result": [{"metric": {"__name__": "foo_seconds_exact", "shape": "circle"}, "value": [151, "17"]}]}'


def test_range_query_lookback_delta():
    query = 'foo{shape="circle"}'

    expected = '{"resultType": "matrix", "result": [{"metric": {"__name__": "foo", "shape": "circle", "size": "l"}, "values": [[150, "16"]]}]}'
    assert (
        execute_range_query_via_http_api(
            node.ip_address, 9093, "/api/v1/query_range", query, 150, 151, 1,
            params={"lookback_delta": "0.5"},
        )
        == expected
    )


# Malformed PromQL is rejected at parse time.
# The response must be a well-formed Prometheus error response `{"status":"error",...}`
def test_error_while_parsing():
    response = get_response_to_http_api_query(
        node.ip_address, 9093, "/api/v1/query", "((", 150,
    )
    error_message = extract_error_from_http_api_response(response)
    assert "while parsing PromQL query" in error_message


# Checks the case when an exception appears before any block has been written to the response buffer.
# The response must be a well-formed Prometheus error response `{"status":"error",...}`
def test_error_before_first_block():
    response = get_response_to_http_api_query(
        node.ip_address, 9093, "/api/v1/query",
        "topk(+Inf, last_over_time(foo[10]))[50:10]", 150,
    )
    error_message = extract_error_from_http_api_response(response)
    assert "k of aggregation operator is too large" in error_message


# Checks the case when an exception appears after some blocks have been written
# to the response buffer, but before the response buffer has been sent to the client.
# The response must be a well-formed Prometheus error response `{"status":"error",...}`
def test_error_after_first_block():
    # `result_overflow_mode=throw` makes the query throw once `STREAM_ERROR_ROW_LIMIT` rows
    # have been written. The response buffer is left at its default size, so those rows are
    # still buffered and the handler can drop them and write the error response instead.
    url = (
        f"http://{node.ip_address}:9093/api/v1/query_range"
        f"?query={urllib.parse.quote_plus('stream_error')}"
        f"&start=100&end=200&step=10"
        f"&max_block_size=1"
        f"&max_result_rows={STREAM_ERROR_ROW_LIMIT}"
        f"&result_overflow_mode=throw"
    )
    response = requests.get(url)
    error_message = extract_error_from_http_api_response(response)
    assert "Limit for result exceeded" in error_message


# Checks the case when an exception appears after some blocks have been written
# to the response buffer and after the response buffer has been sent to the client.
# The handler can no longer change the status code or produce a well-formed Prometheus
# error response `{"status":"error",...}`, so it aborts the chunked stream
# by writing an `__exception__` marker block and skipping the terminating empty chunk.
def test_query_after_response_sent():
    # Same as `test_error_after_first_block`, except `http_response_buffer_size=1` flushes
    # the response buffer as soon as the first block is written. The head is therefore
    # already on the wire when the query throws, and the handler has no way back.
    url = (
        f"http://{node.ip_address}:9093/api/v1/query_range"
        f"?query={urllib.parse.quote_plus('stream_error')}"
        f"&start=100&end=200&step=10"
        f"&http_response_buffer_size=1"
        f"&max_block_size=1"
        f"&max_result_rows={STREAM_ERROR_ROW_LIMIT}"
        f"&result_overflow_mode=throw"
    )
    with requests.get(url, stream=True) as response:
        assert response.status_code == 200, (
            f"expected head to be sent before the throw, "
            f"got {response.status_code}: {response.text!r}"
        )
        assert response.headers.get("Transfer-Encoding") == "chunked", (
            f"expected chunked transfer, got headers={dict(response.headers)!r}"
        )

        received = b""
        with pytest.raises(requests.exceptions.ChunkedEncodingError):
            for piece in response.iter_content(chunk_size=None):
                received += piece

        # What the client got must be a truncated success response, not an error response:
        # the success envelope and some `stream_error` results were already written when the
        # query threw. Without this the test would also pass if the stream were aborted
        # before anything was written.
        assert received.startswith(b'{"status":"success"'), received
        assert b"stream_error" in received, received

        # The stream ends with the `__exception__` marker block that replaces the
        # terminating empty chunk, and it carries the error that caused the abort. This
        # distinguishes a deliberate abort from the connection merely dropping.
        assert b"__exception__" in received, received
        assert b"Limit for result exceeded" in received, received


def test_table_query_param():
    query = 'foo{shape="square"}'
    timestamp = 150

    expected = '{"resultType": "vector", "result": [{"metric": {"__name__": "foo", "shape": "square", "size": "s"}, "value": [150, "40"]}]}'
    assert execute_query_via_http_api(node.ip_address, 9093, "/api/v1/query", query, timestamp=timestamp) == expected

    # Both database and table names come from the URL query as two separate parameters
    # `database` and `table`.
    assert (
        execute_query_via_http_api(
            node.ip_address, 9093, "/dynamic_table/api/v1/query", query, timestamp=timestamp,
            params={"database": "default", "table": "prometheus"},
        )
        == expected
    )

    # A single `table` parameter carries the qualified `database.table` name.
    assert (
        execute_query_via_http_api(
            node.ip_address, 9093, "/dynamic_table/api/v1/query", query, timestamp=timestamp,
            params={"table": "default.prometheus"},
        )
        == expected
    )

    # A request without a `table` parameter fails.
    error = execute_query_via_http_api(
        node.ip_address, 9093, "/dynamic_table/api/v1/query", query, timestamp=timestamp,
        expect_error=True,
    )
    assert "table name is not set" in error

    # The table name comes from the URL query, and the database name comes from the configuration.
    assert (
        execute_query_via_http_api(
            node.ip_address, 9093, "/dynamic_table_and_fixed_db/api/v1/query", query, timestamp=timestamp,
            params={"table": "prometheus"},
        )
        == expected
    )

    # The configured database cannot be overridden by the `database` query parameter.
    error = execute_query_via_http_api(
        node.ip_address, 9093, "/dynamic_table_and_fixed_db/api/v1/query", query, timestamp=timestamp,
        params={"database": "default", "table": "prometheus"}, expect_error=True,
    )
    assert "cannot be overridden" in error

    # A qualified `<table>default.prometheus</table>` in the configuration (the static `/api/v1/query`
    # handler) sets the database too, so it cannot be overridden by the `database` query parameter.
    error = execute_query_via_http_api(
        node.ip_address, 9093, "/api/v1/query", query, timestamp=timestamp,
        params={"database": "other"}, expect_error=True,
    )
    assert "cannot be overridden" in error


def test_generated_sql_always_runs_with_analyzer():
    # The SQL generated for PromQL marks shared subqueries AS MATERIALIZED, which only the
    # analyzer honors, so the handler forces the analyzer and enable_materialized_cte
    # regardless of the caller's enable_analyzer. The materialization itself is covered by
    # 04816_promql_shared_subqueries_materialized; here it is enough to check the settings
    # the generated query ran with.
    for path, time_params in (
        ("/api/v1/query", "time=1000"),
        ("/api/v1/query_range", "start=999&end=1002&step=1"),
    ):
        query_id = f"promql-analyzer-{uuid.uuid4()}"
        url = (
            f"http://{node.ip_address}:9093{path}"
            f"?query=post_body_metric&{time_params}&enable_analyzer=0"
        )
        response = requests.get(url, headers={"X-ClickHouse-Query-Id": query_id})
        extract_data_from_http_api_response(response)  # raises unless a success envelope

        node.query("SYSTEM FLUSH LOGS query_log")
        # The response is flushed to the client before the QueryFinish row is queued, so a
        # flush can run before there is anything to flush and the row then waits for the
        # background interval. Retry for longer than that interval.
        assert_eq_with_retry(
            node,
            "SELECT Settings['allow_experimental_analyzer'], "
            "Settings['enable_materialized_cte'] "
            f"FROM system.query_log WHERE type = 'QueryFinish' AND query_id = '{query_id}'",
            "1\t1\n",
            retry_count=30,
            sleep_time=1,
        )
