import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
from .prometheus_test_utils import (
    WRITE_V1_CONTENT_TYPE,
    WRITE_V2_CONTENT_TYPE,
    assert_remote_write_v2_written_headers,
    convert_read_request_to_protobuf,
    convert_time_series_to_protobuf,
    convert_time_series_to_write_v2_protobuf,
    execute_query_via_http_api,
    get_response_to_remote_read,
    get_response_to_remote_write,
    receive_protobuf_from_remote_read,
    send_protobuf_to_remote_write,
)
import re
import requests
import time


cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
    with_prometheus_writer=True,
    with_prometheus_reader=True,
    handle_prometheus_remote_write=(9093, "/write"),
    handle_prometheus_remote_read=(9093, "/read"),
)


# Waits until Prometheus scrapes some data and sends it to ClickHouse via the RemoteWrite protocol.
def wait_for_scraped_data():
    start_time = time.monotonic()
    assert_eq_with_retry(
        node, "SELECT count() > 0 FROM timeSeriesData(prometheus)", "1"
    )
    elapsed = time.monotonic() - start_time
    data_num_rows = int(node.query("SELECT count() FROM timeSeriesData(prometheus)"))
    tags_num_rows = int(node.query("SELECT count() FROM timeSeriesTags(prometheus)"))
    metrics_num_rows = int(
        node.query("SELECT count() FROM timeSeriesMetricFamilies(prometheus)")
    )
    print(f"After waiting {elapsed} seconds got numbers of rows:")
    print(
        f"data: {data_num_rows} rows, tags: {tags_num_rows} rows, metrics: {metrics_num_rows} rows"
    )


# Sends lots of data to ClickHouse via the RemoteWrite protocol.
def send_big_data(metric_name="big_data", start_time=1724112000, end_time=1724115600, count=75000):
    time_series = []
    step = (end_time - start_time) / count
    for i in range(0, count):
        timestamp = start_time + i * step
        value = i
        time_series.append(({"__name__": metric_name}, {timestamp: value}))
    protobuf = convert_time_series_to_protobuf(time_series)
    send_protobuf_to_remote_write(node.ip_address, 9093, "/write", protobuf)


# Executes a query in the "prometheus_reader" service. This service uses the RemoteRead protocol to get data from ClickHouse.
def execute_query_in_prometheus_reader(query, timestamp):
    return execute_query_via_http_api(
        cluster.prometheus_ip["reader"],
        cluster.prometheus_port["reader"],
        "/api/v1/query",
        query,
        timestamp,
    )


# Executes a query in the "prometheus_writer" service. This service sends data to ClickHouse via the RemoteWrite protocol.
def execute_query_in_prometheus_writer(query, timestamp):
    return execute_query_via_http_api(
        cluster.prometheus_ip["writer"],
        cluster.prometheus_port["writer"],
        "/api/v1/query",
        query,
        timestamp,
    )


# Executes a query in both prometheus services - the results should be the same regardless of
# whether the data comes through ClickHouse or now.
def execute_query_in_prometheus(query, timestamp):
    r1 = execute_query_in_prometheus_reader(query, timestamp)
    r2 = execute_query_in_prometheus_writer(query, timestamp)
    assert r1 == r2
    return r1


# Executes a prometheus query in ClickHouse
def execute_query_in_clickhouse(query, timestamp):
    return node.query(
        f"SELECT * FROM prometheusQuery(prometheus, '{query}', {timestamp})"
    )


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        node.query("CREATE TABLE prometheus ENGINE=TimeSeries")
        wait_for_scraped_data()
        send_big_data()
        yield cluster
    finally:
        cluster.shutdown()


def test_handle_normal_scrape():
    query = "up"
    evaluation_time = time.time()
    result = execute_query_in_prometheus(query, evaluation_time)
    print(f"result={result}")
    pattern = '\\{"resultType": "vector", "result": \\[\\{"metric": \\{"__name__": "up", "instance": "localhost:9090", "job": "prometheus"}, "value": \\[[0-9]+(\\.[0-9]*)?, "1"]}]}'
    assert re.match(pattern, result)
    chresult = execute_query_in_clickhouse(query, evaluation_time)
    print(f"chresult={chresult}")
    chpattern = "\\[\\('__name__','up'\\),\\('instance','localhost:9090'\\),\\('job','prometheus'\\)]\t[^\t]*\t1\n"
    assert re.match(chpattern, chresult)


def test_remote_read_auth():
    read_request = convert_read_request_to_protobuf(
        "^up$", time.time() - 300, time.time()
    )
    print(f"read_request={read_request}")

    read_response = receive_protobuf_from_remote_read(
        node.ip_address,
        9093,
        "read_auth_ok",
        read_request,
    )
    print(f"read_response = {read_response}")
    assert len(read_response.results) > 0
    assert len(read_response.results[0].timeseries) > 0
    assert len(read_response.results[0].timeseries[0].samples) > 0

    auth_fail_response = get_response_to_remote_read(
        node.ip_address,
        9093,
        "read_auth_fail",
        read_request,
    )
    assert auth_fail_response.status_code == requests.codes.forbidden


def test_remote_read_big_data():
    read_request = convert_read_request_to_protobuf(
        "^big_data$", 1724112000, 1724115600
    )

    read_response = receive_protobuf_from_remote_read(
        node.ip_address,
        9093,
        "read_auth_ok",
        read_request)

    assert len(read_response.results) == 1
    assert len(read_response.results[0].timeseries) == 1
    assert len(read_response.results[0].timeseries[0].samples) == 75000


def test_remote_write_zstd():
    start_time = 1724116000
    count = 100
    time_series = []
    for i in range(0, count):
        time_series.append(({"__name__": "zstd_data"}, {start_time + i: float(i)}))
    protobuf = convert_time_series_to_protobuf(time_series)

    send_protobuf_to_remote_write(
        node.ip_address, 9093, "/write", protobuf, content_encoding="zstd"
    )

    read_request = convert_read_request_to_protobuf(
        "^zstd_data$", start_time, start_time + count
    )
    read_response = receive_protobuf_from_remote_read(
        node.ip_address, 9093, "read_auth_ok", read_request
    )
    assert len(read_response.results) == 1
    assert len(read_response.results[0].timeseries) == 1
    assert len(read_response.results[0].timeseries[0].samples) == count


def test_remote_write_unsupported_content_encoding():
    time_series = [({"__name__": "gzip_data"}, {1724117000: 1.0})]
    protobuf = convert_time_series_to_protobuf(time_series)

    response = get_response_to_remote_write(
        node.ip_address, 9093, "/write", protobuf, content_encoding="gzip"
    )
    assert response.status_code == requests.codes.unsupported_media_type
    assert "Content-Encoding" in response.text


def test_remote_write_unsupported_content_type():
    time_series = [({"__name__": "text_data"}, {1724117000: 1.0})]
    protobuf = convert_time_series_to_protobuf(time_series)

    response = get_response_to_remote_write(
        node.ip_address, 9093, "/write", protobuf, content_type="text/plain"
    )
    assert response.status_code == requests.codes.unsupported_media_type
    assert "Content-Type" in response.text


def post_undecodable_body(path, content_encoding):
    """A body the client encoded wrongly, sent verbatim so nothing compresses it on the way out."""
    return requests.post(
        f"http://{node.ip_address}:9093/{path.strip('/')}",
        data=b"this is not a compressed protobuf",
        headers={
            "Content-Encoding": content_encoding,
            "Content-Type": "application/x-protobuf",
            "User-Agent": requests.utils.default_user_agent(),
            "X-Prometheus-Remote-Write-Version": "0.1.0",
        },
    )


@pytest.mark.parametrize("content_encoding", ["snappy", "zstd"])
def test_remote_write_undecodable_body(content_encoding):
    """A body that cannot be decompressed is the sender's permanent fault. It must not be answered
    with a 5xx, which Prometheus retries for as long as it keeps the batch."""
    response = post_undecodable_body("/write", content_encoding)
    assert response.status_code == requests.codes.bad_request, response.text
    assert "Cannot decode the request body" in response.text


def test_remote_read_undecodable_body():
    response = requests.get(
        f"http://{node.ip_address}:9093/read_auth_ok",
        data=b"this is not a compressed protobuf",
        headers={
            "Content-Encoding": "snappy",
            "Accept-Encoding": "snappy",
            "Content-Type": "application/x-protobuf",
            "User-Agent": requests.utils.default_user_agent(),
            "X-Prometheus-Remote-Read-Version": "0.1.0",
        },
    )
    assert response.status_code == requests.codes.bad_request, response.text
    assert "Cannot decode the request body" in response.text


def _send_write_v2(time_series, content_encoding="snappy", metadata=None):
    protobuf = convert_time_series_to_write_v2_protobuf(time_series)
    if metadata:
        metric_type, help_text, unit = metadata
        series_metadata = protobuf.timeseries[0].metadata
        series_metadata.type = series_metadata.MetricType.Value(metric_type)
        series_metadata.help_ref = len(protobuf.symbols)
        protobuf.symbols.append(help_text)
        series_metadata.unit_ref = len(protobuf.symbols)
        protobuf.symbols.append(unit)
    send_protobuf_to_remote_write(
        node.ip_address,
        9093,
        "/write",
        protobuf,
        content_encoding=content_encoding,
        headers={
            "Content-Type": WRITE_V2_CONTENT_TYPE,
            "X-Prometheus-Remote-Write-Version": "2.0.0",
        },
    )


def _read_samples(metric_name, start_time, end_time):
    read_request = convert_read_request_to_protobuf(
        f"^{metric_name}$", start_time, end_time
    )
    read_response = receive_protobuf_from_remote_read(
        node.ip_address, 9093, "read_auth_ok", read_request
    )
    assert len(read_response.results) == 1
    return read_response.results[0].timeseries


def test_remote_write_v2_single_metric():
    start_time = 1724118000
    time_series = [
        (
            {"__name__": "rw2_up", "job": "testjob", "instance": "localhost:9090"},
            {start_time: 1.0},
        )
    ]
    _send_write_v2(time_series)
    series = _read_samples("rw2_up", start_time, start_time + 1)
    assert len(series) == 1
    assert len(series[0].samples) == 1
    assert series[0].samples[0].value == 1.0


def test_remote_write_v2_multiple_metrics():
    start_time = 1724118100
    time_series = [
        (
            {"__name__": "rw2_up", "job": "testjob", "instance": "localhost:9091"},
            {start_time: 1.0},
        ),
        (
            {"__name__": "rw2_up", "job": "testjob", "instance": "localhost:9092"},
            {start_time: 1.0},
        ),
        (
            {"__name__": "rw2_http_requests_total", "job": "api"},
            {start_time: 100.0},
        ),
    ]
    _send_write_v2(time_series)
    series = _read_samples("rw2_http_requests_total", start_time, start_time + 1)
    assert len(series) == 1
    assert len(series[0].samples) == 1
    assert series[0].samples[0].value == 100.0


def test_remote_write_v2_multiple_samples():
    start_time = 1724118200
    count = 5
    samples = {start_time + i: float(i) for i in range(count)}
    time_series = [({"__name__": "rw2_multi_sample"}, samples)]
    _send_write_v2(time_series)
    series = _read_samples("rw2_multi_sample", start_time, start_time + count)
    assert len(series) == 1
    assert len(series[0].samples) == count
    assert [sample.value for sample in series[0].samples] == [float(i) for i in range(count)]


def test_remote_write_v2_metadata():
    start_time = 1724118250
    time_series = [({"__name__": "rw2_metadata"}, {start_time: 1.0})]
    _send_write_v2(
        time_series,
        metadata=(
            "METRIC_TYPE_COUNTER",
            "Total number of remote write v2 requests",
            "requests",
        ),
    )
    assert node.query(
        "SELECT metric_family, type, unit, help "
        "FROM timeSeriesMetrics(prometheus) "
        "WHERE metric_family = 'rw2_metadata'"
    ) == "rw2_metadata\tcounter\trequests\tTotal number of remote write v2 requests\n"


def test_remote_write_v2_rejects_metadata_only():
    metric_name = "rw2_metadata_only"
    protobuf = convert_time_series_to_write_v2_protobuf(
        [({"__name__": metric_name}, {})]
    )
    series_metadata = protobuf.timeseries[0].metadata
    series_metadata.type = series_metadata.MetricType.Value("METRIC_TYPE_COUNTER")
    response = get_response_to_remote_write(
        node.ip_address,
        9093,
        "/write",
        protobuf,
        content_type=WRITE_V2_CONTENT_TYPE,
        headers={"X-Prometheus-Remote-Write-Version": "2.0.0"},
    )
    assert response.status_code == requests.codes.bad_request
    assert_remote_write_v2_written_headers(response, 0)
    assert node.query(
        "SELECT count() FROM timeSeriesMetrics(prometheus) "
        f"WHERE metric_family = '{metric_name}'"
    ) == "0\n"


def test_remote_write_v2_zstd():
    start_time = 1724118300
    time_series = [({"__name__": "rw2_zstd_data"}, {start_time: 42.0})]
    _send_write_v2(time_series, content_encoding="zstd")
    series = _read_samples("rw2_zstd_data", start_time, start_time + 1)
    assert len(series) == 1
    assert series[0].samples[0].value == 42.0


@pytest.mark.parametrize(
    "case, include_float, separate_histogram_series",
    [
        ("histogram_only", False, False),
        ("separate_float_and_histogram_series", True, True),
        ("samples_and_histograms_in_one_series", True, False),
    ],
)
def test_remote_write_v2_rejects_native_histograms(
    case, include_float, separate_histogram_series
):
    start_time = 1724118350
    metric_name = f"rw2_{case}"
    protobuf = convert_time_series_to_write_v2_protobuf(
        [({"__name__": metric_name}, {start_time: 42.0} if include_float else {})]
    )
    histogram_metric_name = metric_name
    histogram_series = protobuf.timeseries[0]
    if separate_histogram_series:
        histogram_metric_name = f"{metric_name}_histogram"
        protobuf.symbols.append(histogram_metric_name)
        histogram_series = protobuf.timeseries.add(
            labels_refs=[
                list(protobuf.symbols).index("__name__"),
                len(protobuf.symbols) - 1,
            ]
        )
    histogram_series.histograms.add(timestamp=start_time * 1000)

    response = get_response_to_remote_write(
        node.ip_address,
        9093,
        "/write",
        protobuf,
        content_type=WRITE_V2_CONTENT_TYPE,
        headers={"X-Prometheus-Remote-Write-Version": "2.0.0"},
    )
    assert response.status_code == requests.codes.bad_request
    assert_remote_write_v2_written_headers(response, 0)
    assert _read_samples(metric_name, start_time, start_time + 1) == []
    assert _read_samples(histogram_metric_name, start_time, start_time + 1) == []


def test_remote_write_v2_invalid_first_symbol():
    metric_name = "rw2_invalid_first_symbol"
    protobuf = convert_time_series_to_write_v2_protobuf(
        [({"__name__": metric_name}, {1724118375: 1.0})]
    )
    protobuf.symbols[0] = "not-empty"
    response = get_response_to_remote_write(
        node.ip_address,
        9093,
        "/write",
        protobuf,
        content_type=WRITE_V2_CONTENT_TYPE,
    )
    assert response.status_code == requests.codes.bad_request
    assert_remote_write_v2_written_headers(response, 0)
    assert node.query(
        "SELECT count() FROM timeSeriesMetrics(prometheus) "
        f"WHERE metric_family = '{metric_name}'"
    ) == "0\n"


def test_remote_write_wrong_schema():
    v1_request = convert_time_series_to_protobuf(
        [({"__name__": "rw1_as_rw2"}, {1724118380: 1.0})]
    )
    response = get_response_to_remote_write(
        node.ip_address,
        9093,
        "/write",
        v1_request,
        content_type=WRITE_V2_CONTENT_TYPE,
    )
    assert response.status_code == requests.codes.bad_request

    v2_request = convert_time_series_to_write_v2_protobuf(
        [({"__name__": "rw2_as_rw1"}, {1724118385: 1.0})]
    )
    response = get_response_to_remote_write(
        node.ip_address,
        9093,
        "/write",
        v2_request,
    )
    assert response.status_code == requests.codes.bad_request


def test_remote_write_v2_missing_metric_name():
    time_series = [({"job": "testjob", "instance": "localhost:9090"}, {1724118400: 1.0})]
    protobuf = convert_time_series_to_write_v2_protobuf(time_series)
    response = get_response_to_remote_write(
        node.ip_address,
        9093,
        "/write",
        protobuf,
        content_type=WRITE_V2_CONTENT_TYPE,
        headers={"X-Prometheus-Remote-Write-Version": "2.0.0"},
    )
    assert response.status_code == requests.codes.bad_request


@pytest.mark.parametrize(
    "proto",
    [
        "unknown.Request",
        "prometheus.WriteRequest2",
        "io.prometheus.write.v2.Request.bad",
    ],
)
def test_remote_write_v2_unsupported_proto_parameter(proto):
    time_series = [({"__name__": "rw2_bad_proto"}, {1724118500: 1.0})]
    protobuf = convert_time_series_to_write_v2_protobuf(time_series)
    response = get_response_to_remote_write(
        node.ip_address,
        9093,
        "/write",
        protobuf,
        content_type=f"application/x-protobuf;proto={proto}",
        headers={"X-Prometheus-Remote-Write-Version": "2.0.0"},
    )
    assert response.status_code == requests.codes.unsupported_media_type
    assert "Content-Type" in response.text
    assert "X-Prometheus-Remote-Write-Samples-Written" not in response.headers
    assert "X-Prometheus-Remote-Write-Histograms-Written" not in response.headers
    assert "X-Prometheus-Remote-Write-Exemplars-Written" not in response.headers


def test_remote_write_v2_disabled_setting():
    start_time = 1724118600
    v2_request = convert_time_series_to_write_v2_protobuf(
        [({"__name__": "rw2_disabled_setting"}, {start_time: 1.0})]
    )
    v2_response = get_response_to_remote_write(
        node.ip_address,
        9093,
        "/write?enable_prometheus_remote_write_v2=0",
        v2_request,
        content_type=WRITE_V2_CONTENT_TYPE,
        headers={"X-Prometheus-Remote-Write-Version": "2.0.0"},
    )
    assert v2_response.status_code == requests.codes.bad_request
    assert "enable_prometheus_remote_write_v2" in v2_response.text
    assert_remote_write_v2_written_headers(v2_response, 0)
    assert node.query(
        "SELECT count() FROM timeSeriesData(prometheus) "
        "WHERE id IN (SELECT id FROM timeSeriesTags(prometheus) "
        "WHERE metric_name = 'rw2_disabled_setting')"
    ) == "0\n"

    v1_request = convert_time_series_to_protobuf(
        [({"__name__": "rw1_with_v2_disabled"}, {start_time: 7.0})]
    )
    send_protobuf_to_remote_write(
        node.ip_address,
        9093,
        "/write?enable_prometheus_remote_write_v2=0",
        v1_request,
    )
    series = _read_samples("rw1_with_v2_disabled", start_time, start_time + 1)
    assert len(series) == 1
    assert series[0].samples[0].value == 7.0


def test_remote_write_explicit_v1_content_type():
    start_time = 1724118700
    protobuf = convert_time_series_to_protobuf(
        [({"__name__": "rw1_explicit_proto"}, {start_time: 3.0})]
    )
    send_protobuf_to_remote_write(
        node.ip_address,
        9093,
        "/write",
        protobuf,
        headers={"Content-Type": WRITE_V1_CONTENT_TYPE},
    )
    series = _read_samples("rw1_explicit_proto", start_time, start_time + 1)
    assert len(series) == 1
    assert series[0].samples[0].value == 3.0


def test_remote_write_v2_written_sample_count():
    start_time = 1724118800
    samples = {start_time + i: float(i) for i in range(3)}
    protobuf = convert_time_series_to_write_v2_protobuf(
        [({"__name__": "rw2_written_count"}, samples)]
    )
    response = get_response_to_remote_write(
        node.ip_address,
        9093,
        "/write",
        protobuf,
        content_type=WRITE_V2_CONTENT_TYPE,
        headers={"X-Prometheus-Remote-Write-Version": "2.0.0"},
    )
    assert response.status_code == requests.codes.no_content
    assert_remote_write_v2_written_headers(response, 3)
    series = _read_samples("rw2_written_count", start_time, start_time + 3)
    assert len(series) == 1
    assert len(series[0].samples) == 3


def test_remote_write_v2_async_insert_timeout_omits_written_headers():
    start_time = 1724118850
    metric_name = "rw2_async_timeout"
    protobuf = convert_time_series_to_write_v2_protobuf(
        [({"__name__": metric_name}, {start_time: 1.0})]
    )
    response = get_response_to_remote_write(
        node.ip_address,
        9093,
        "/write?async_insert=1&wait_for_async_insert_timeout=0"
        "&async_insert_use_adaptive_busy_timeout=0&async_insert_busy_timeout_max_ms=3000",
        protobuf,
        content_type=WRITE_V2_CONTENT_TYPE,
        headers={"X-Prometheus-Remote-Write-Version": "2.0.0"},
    )
    assert response.status_code == requests.codes.service_unavailable
    assert "X-Prometheus-Remote-Write-Samples-Written" not in response.headers
    assert "X-Prometheus-Remote-Write-Histograms-Written" not in response.headers
    assert "X-Prometheus-Remote-Write-Exemplars-Written" not in response.headers
    assert_eq_with_retry(
        node,
        "SELECT count() FROM timeSeriesData(prometheus) "
        "WHERE id IN (SELECT id FROM timeSeriesTags(prometheus) "
        f"WHERE metric_name = '{metric_name}')",
        "1",
        retry_count=60,
    )


def test_remote_write_v2_rejects_sample_start_timestamp():
    start_time = 1724118875
    metric_name = "rw2_start_timestamp"
    protobuf = convert_time_series_to_write_v2_protobuf(
        [({"__name__": metric_name}, {start_time: 1.0})]
    )
    protobuf.timeseries[0].samples[0].start_timestamp = (start_time - 10) * 1000
    response = get_response_to_remote_write(
        node.ip_address,
        9093,
        "/write",
        protobuf,
        content_type=WRITE_V2_CONTENT_TYPE,
        headers={"X-Prometheus-Remote-Write-Version": "2.0.0"},
    )
    assert response.status_code == requests.codes.bad_request
    assert_remote_write_v2_written_headers(response, 0)
    assert _read_samples(metric_name, start_time, start_time + 1) == []


def test_remote_write_v2_rejects_exemplars():
    start_time = 1724118900
    metric_name = "rw2_exemplars"
    protobuf = convert_time_series_to_write_v2_protobuf(
        [({"__name__": metric_name}, {start_time: 1.0})]
    )
    protobuf.timeseries[0].exemplars.add(timestamp=start_time * 1000, value=1.0)
    response = get_response_to_remote_write(
        node.ip_address,
        9093,
        "/write",
        protobuf,
        content_type=WRITE_V2_CONTENT_TYPE,
        headers={"X-Prometheus-Remote-Write-Version": "2.0.0"},
    )
    assert response.status_code == requests.codes.bad_request
    assert_remote_write_v2_written_headers(response, 0)
    series = _read_samples(metric_name, start_time, start_time + 1)
    assert series == []
