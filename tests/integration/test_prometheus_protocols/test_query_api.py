import urllib

import pytest
import requests

from helpers.cluster import ClickHouseCluster
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


# Generic ClickHouse HTTP parameters (`query_id`, `quota_key`, `stacktrace`, `role`) are excluded
# from setting-like parameters by the shared HTTP context setup (`makeContext`), and the Prometheus
# endpoints must keep those exclusions when they reserve their own parameters. Otherwise a request
# like `/api/v1/query?...&query_id=x` fails with an "unknown setting" error.
def test_generic_http_params_are_not_settings():
    # `query_id`, `quota_key` and `stacktrace` are generic HTTP parameters handled by the base
    # HTTP handler, not ClickHouse settings. If any of them were misinterpreted as a setting, the
    # request would fail with an "Unknown setting ..." error instead of succeeding.
    url = (
        f"http://{node.ip_address}:9093/api/v1/query"
        f"?query=post_body_metric&time=1000"
        f"&query_id=prometheus_api_query_id"
        f"&quota_key=prometheus_api_quota"
        f"&stacktrace=1"
    )
    response = requests.get(url)
    assert response.status_code == 200, f"got {response.status_code}: {response.text}"
    assert response.json()["status"] == "success"

    # The supplied query id must reach the executed query instead of being applied as a setting.
    node.query("SYSTEM FLUSH LOGS query_log")
    assert (
        node.query(
            "SELECT count() > 0 FROM system.query_log WHERE query_id = 'prometheus_api_query_id'"
        )
        == "1\n"
    )

    # `role` is likewise a generic session parameter, not a setting. Passing a role that exists but
    # is not granted to the handler's user must fail inside the session role machinery (a role/access
    # error naming the role) rather than with "Unknown setting role", which proves the parameter was
    # routed to `SET ROLE` instead of the settings parser.  Granting the role to the `default` user
    # is not possible here because it lives in the read-only XML access storage, so an ungranted role
    # is the reliable positive signal that the parameter is not treated as a setting.
    node.query("CREATE ROLE IF NOT EXISTS prometheus_api_role")
    response = requests.get(
        f"http://{node.ip_address}:9093/api/v1/query"
        f"?query=post_body_metric&time=1000&role=prometheus_api_role"
    )
    assert response.status_code != 200, f"expected a failure, got {response.text}"
    assert "Unknown setting" not in response.text, response.text
    assert "prometheus_api_role" in response.text, response.text


# The standard Prometheus HTTP API parameters that are not implemented yet (`timeout`,
# `stats`) are reserved by the handler, so a valid Prometheus request such as
# `/api/v1/query?query=up&timeout=5s` must fail with an explicit "not supported"
# error from the Prometheus handler instead of an "Unknown setting" error from the
# generic HTTP settings parser.
def test_unsupported_prometheus_params_rejected_explicitly():
    for endpoint in ["/api/v1/query", "/api/v1/query_range"]:
        for param in ["timeout=5s", "stats=all"]:
            url = (
                f"http://{node.ip_address}:9093{endpoint}"
                f"?query=post_body_metric&time=1000&start=1000&end=1001&step=1&{param}"
            )
            response = requests.get(url)
            assert response.status_code != 200, f"expected a failure, got {response.text}"
            assert "Unknown setting" not in response.text, response.text
            result = response.json()
            assert result["status"] == "error", response.text
            name = param.split("=")[0]
            assert f"'{name}' parameter" in result["error"], response.text
            assert "not supported" in result["error"], response.text


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
