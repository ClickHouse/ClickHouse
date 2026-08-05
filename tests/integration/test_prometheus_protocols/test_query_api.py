import pytest

from helpers.cluster import ClickHouseCluster
from .prometheus_test_utils import *


cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
    handle_prometheus_remote_read=(9093, "/read"),
    handle_prometheus_remote_write=(9093, "/write"),
)


def send_to_clickhouse(time_series):
    protobuf = convert_time_series_to_protobuf(time_series)
    send_protobuf_to_remote_write(node.ip_address, 9093, "/write", protobuf)


def send_test_data():
    # `post_body_metric` is used by the GET-vs-POST tests.
    send_to_clickhouse(
        [({"__name__": "post_body_metric", "job": "test"}, {1000.0: 1.0, 1001.0: 2.0})]
    )
    # `foo` (3 series) is used by the error-envelope tests — multiple series
    # let `max_result_rows=1` trigger after the first row.
    send_to_clickhouse(
        [
            ({"__name__": "foo", "shape": "square", "size": "s"}, {110: 4, 130: 40}),
            ({"__name__": "foo", "shape": "triangle", "size": "m"}, {110: 8, 120: 80}),
            ({"__name__": "foo", "shape": "circle", "size": "l"}, {110: 16, 130: 16, 150: 16}),
        ]
    )


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        node.query("CREATE TABLE prometheus ENGINE=TimeSeries")
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
    # Here `max_block_size=1` + `max_result_rows=1` + `result_overflow_mode=throw`
    # makes the second pull throw.
    url = (
        f"http://{node.ip_address}:9093/api/v1/query_range"
        f"?query={urllib.parse.quote_plus('foo')}"
        f"&start=100&end=200&step=10"
        f"&max_block_size=1"
        f"&max_result_rows=1"
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
    # Here `max_block_size=1` + `max_result_rows=1` + `result_overflow_mode=throw`
    # makes the second pull() throw; and `http_response_buffer_size=1`
    # makes the response buffer flush after the very first byte written.
    url = (
        f"http://{node.ip_address}:9093/api/v1/query_range"
        f"?query={urllib.parse.quote_plus('foo')}"
        f"&start=100&end=200&step=10"
        f"&http_response_buffer_size=1"
        f"&max_block_size=1"
        f"&max_result_rows=1"
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
        
        with pytest.raises(requests.exceptions.ChunkedEncodingError):
            response.content  # Reading property response.content hits the chunked-stream abort
