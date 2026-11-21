import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry, wait_condition
from .prometheus_test_utils import (
    convert_read_request_to_protobuf,
    convert_time_series_to_protobuf,
    execute_query_via_http_api,
    extract_protobuf_from_remote_read_response,
    get_response_to_remote_read,
    get_response_to_remote_write,
    receive_protobuf_from_remote_read,
    remote_pb2,
    send_protobuf_to_remote_write,
    types_pb2,
)
import hashlib
import json
import re
import requests
import struct
import time
from pathlib import Path


# The "writer" Prometheus scrapes itself with native histograms enabled and sends them to ClickHouse over remote-write together with
# the float samples. Prometheus exposes a native histogram about itself (`prometheus_http_request_duration_seconds`). Native histograms
# with custom buckets never arrive this way: Prometheus 3.7 and later don't send them over remote-write 1.0 (see test_native_histograms.py
# for hand-crafted ones).

cluster = ClickHouseCluster(__file__)

# The handlers (configs/prometheus.xml) are added after the table exists, see `start_cluster`.
node = cluster.add_instance(
    "node",
    user_configs=["configs/allow_experimental_time_series_table.xml"],
    with_prometheus_writer=True,
    prometheus_writer_native_histograms=True,
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


# Waits until Prometheus sends the first native histograms: they arrive later than the first float samples.
def wait_for_histograms():
    assert_eq_with_retry(node, "SELECT sum(length(histograms)) > 0 FROM timeSeriesSamples(prometheus)", "1", retry_count=120)


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
        # Open the remote-write listener after its destination exists. Earlier connection failures are retryable;
        # an `UNKNOWN_TABLE` response (HTTP 404) makes Prometheus drop the request and permanently increment its
        # failed-sample and failed-histogram counters, which `test_nothing_is_lost` checks.
        node.copy_file_to_container(
            Path(__file__).parent / "configs" / "prometheus.xml",
            "/etc/clickhouse-server/config.d/prometheus.xml",
        )
        node.query("SYSTEM RELOAD CONFIG")
        wait_for_scraped_data()
        wait_for_histograms()
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


def get_scalar_from_prometheus_writer(query):
    result = json.loads(execute_query_in_prometheus_writer(query, None))["result"]
    assert len(result) == 1, result
    return float(result[0]["value"][1])


def wait_for_positive_scalar_in_prometheus_writer(query):
    # Prometheus sees its own counters through its self-scrape, which lags behind what it has just sent.
    return wait_condition(lambda: get_scalar_from_prometheus_writer(query), lambda value: value > 0, max_attempts=60, delay=0.5)


def expand_buckets(spans, values, cumulative):
    """The populated buckets of one side as {bucket index: count}, the spans expanded as Prometheus iterates them. The values of
    integer buckets arrive as deltas (`cumulative`), the values of float buckets as counts. Empty buckets are dropped: the client pads
    small gaps with them, and Prometheus inserts more into the samples of a chunk when a later sample brings new buckets, while
    ClickHouse returns the buckets as sent."""
    buckets, index, position, count = {}, 0, 0, 0
    for bucket_span in spans:
        index += bucket_span.offset
        for _ in range(bucket_span.length):
            count = count + values[position] if cumulative else values[position]
            if count != 0:
                buckets[index] = count
            index += 1
            position += 1
    return buckets


def canonical_histogram(histogram):
    """The content of a `prompb.Histogram` regardless of how its buckets are cut into spans and of the counter reset hint, which
    Prometheus derives from its chunk boundaries. The sum is taken as bits, so that a stale marker (a NaN) compares equal to itself."""
    is_float = histogram.HasField("count_float")
    if is_float:
        count, zero_count = histogram.count_float, histogram.zero_count_float
        positive = expand_buckets(histogram.positive_spans, histogram.positive_counts, cumulative=False)
        negative = expand_buckets(histogram.negative_spans, histogram.negative_counts, cumulative=False)
    else:
        count, zero_count = histogram.count_int, histogram.zero_count_int
        positive = expand_buckets(histogram.positive_spans, histogram.positive_deltas, cumulative=True)
        negative = expand_buckets(histogram.negative_spans, histogram.negative_deltas, cumulative=True)
    return (
        is_float, histogram.timestamp, count, zero_count, struct.pack("<d", histogram.sum), histogram.schema, histogram.zero_threshold,
        positive, negative, list(histogram.custom_values),
    )


def remote_read_scraped_series(host, port, path, end_timestamp_ms):
    """Every time series scraped by the "writer" Prometheus (the job `prometheus`, so the series sent to ClickHouse by the tests
    directly are left out) with samples in [0, end_timestamp_ms], read over the remote-read protocol: {sorted labels: prompb.TimeSeries}."""
    read_request = remote_pb2.ReadRequest()
    query = read_request.queries.add()
    query.start_timestamp_ms = 0
    query.end_timestamp_ms = end_timestamp_ms
    query.matchers.append(types_pb2.LabelMatcher(type=types_pb2.LabelMatcher.Type.EQ, name="job", value="prometheus"))
    response = extract_protobuf_from_remote_read_response(get_response_to_remote_read(host, port, path, read_request))
    return {
        tuple(sorted((label.name, label.value) for label in series.labels)): series for series in response.results[0].timeseries
    }


def canonical_series(series, max_timestamp_ms):
    """The samples of a `prompb.TimeSeries` up to `max_timestamp_ms` in a form comparable across systems: the float samples as
    (timestamp, bits), so that NaN values compare, and the histograms as `canonical_histogram`."""
    floats = [(sample.timestamp, struct.pack("<d", sample.value)) for sample in series.samples if sample.timestamp <= max_timestamp_ms]
    histograms = [canonical_histogram(histogram) for histogram in series.histograms if histogram.timestamp <= max_timestamp_ms]
    return floats, histograms


def test_remote_read_matches_prometheus():
    # Everything Prometheus scraped and sent is read back from both systems over the remote-read protocol and compared series by
    # series and sample by sample after canonicalization, since the same buckets come back cut into other spans and with other
    # counter reset hints, which each system derives from its storage.
    # Prometheus keeps scraping while the test runs and sends every series in order, so what ClickHouse holds of a series is a
    # prefix of what Prometheus holds: the comparison stops at the last timestamp ClickHouse has stored for the series, and
    # ClickHouse is read first, so that Prometheus has everything ClickHouse has.
    end_timestamp_ms = int(time.time() * 1000) + 3_600_000
    from_clickhouse = remote_read_scraped_series(node.ip_address, 9093, "/read", end_timestamp_ms)
    from_prometheus = remote_read_scraped_series(
        cluster.prometheus_ip["writer"], cluster.prometheus_port["writer"], "/api/v1/read", end_timestamp_ms
    )

    only_in_clickhouse = sorted(set(from_clickhouse) - set(from_prometheus))
    assert not only_in_clickhouse, f"Series unknown to Prometheus: {only_in_clickhouse[:5]}"

    canonical = {}
    for labels, series in from_clickhouse.items():
        max_timestamp_ms = max([sample.timestamp for sample in series.samples] + [histogram.timestamp for histogram in series.histograms])
        canonical[labels] = (canonical_series(series, max_timestamp_ms), canonical_series(from_prometheus[labels], max_timestamp_ms))

    mismatches = {labels: pair for labels, pair in canonical.items() if pair[0] != pair[1]}
    assert not mismatches, f"{len(mismatches)} of {len(canonical)} series differ, e.g. {next(iter(mismatches.items()))}"

    # A digest of the compared data for the log; the counts make sure both kinds of samples took part.
    digest = hashlib.sha256(repr(sorted((labels, clickhouse) for labels, (clickhouse, _) in canonical.items())).encode()).hexdigest()
    num_floats = sum(len(floats) for (floats, _), _ in canonical.values())
    num_histograms = sum(len(histograms) for (_, histograms), _ in canonical.values())
    print(f"Remote-read digest {digest[:16]}: {len(canonical)} series, {num_floats} float samples, {num_histograms} histograms")
    assert num_floats > 0 and num_histograms > 0


def test_nothing_is_lost():
    # Prometheus counts the samples and histograms it sent and the ones the receiver rejected; ClickHouse counts the histograms it dropped.
    wait_for_positive_scalar_in_prometheus_writer("sum(prometheus_remote_storage_histograms_total)")
    # The failed and dropped counters have one series per reason, so they have no series at all when nothing was lost.
    assert get_scalar_from_prometheus_writer("sum(prometheus_remote_storage_samples_failed_total) or vector(0)") == 0
    assert get_scalar_from_prometheus_writer("sum(prometheus_remote_storage_histograms_failed_total) or vector(0)") == 0
    assert get_scalar_from_prometheus_writer("sum(prometheus_remote_storage_histograms_dropped_total) or vector(0)") == 0
    assert node.query("SELECT sum(value) FROM system.events WHERE event = 'PrometheusRemoteWriteDroppedHistograms'") == "0\n"
