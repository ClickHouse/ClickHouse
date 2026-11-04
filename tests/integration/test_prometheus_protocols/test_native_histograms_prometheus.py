import hashlib
import json
import math
import struct
import time
from pathlib import Path

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry, wait_condition
from .prometheus_test_utils import (
    execute_query_via_http_api,
    extract_protobuf_from_remote_read_response,
    get_response_to_remote_read,
    remote_pb2,
    types_pb2,
)


# A real Prometheus scrapes itself with native histograms enabled and sends them to ClickHouse over remote-write.
# Prometheus exposes a native histogram about itself (`prometheus_http_request_duration_seconds`). Native histograms with
# custom buckets never arrive this way: Prometheus 3.7 and later don't send them over remote-write 1.0 (see test_native_histograms.py
# for hand-crafted ones). The histograms are stored in the `histograms` column of the samples table and are checked through
# the inner tables, in their stored shape.

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    user_configs=["configs/allow_experimental_time_series_table.xml"],
    with_prometheus_writer=True,
    prometheus_writer_native_histograms=True,
    handle_prometheus_remote_write=(9093, "/write"),
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        node.query("CREATE TABLE prometheus ENGINE=TimeSeries")
        # Open the remote-write listener after its destination exists. Earlier connection failures are retryable;
        # an `UNKNOWN_TABLE` response would permanently increment Prometheus's failed-histogram counter.
        node.copy_file_to_container(
            Path(__file__).parent / "configs" / "prometheus.xml",
            "/etc/clickhouse-server/config.d/prometheus.xml",
        )
        node.query("SYSTEM RELOAD CONFIG")
        wait_for_histograms()
        yield cluster
    finally:
        cluster.shutdown()


def wait_for_histograms():
    assert_eq_with_retry(node, "SELECT sum(length(histograms)) > 0 FROM timeSeriesSamples(prometheus)", "1", retry_count=120)


def execute_query_in_prometheus_writer(query, timestamp=None):
    return json.loads(
        execute_query_via_http_api(
            cluster.prometheus_ip["writer"], cluster.prometheus_port["writer"], "/api/v1/query", query, timestamp
        )
    )


def get_scalar_from_prometheus_writer(query, timestamp=None):
    result = execute_query_in_prometheus_writer(query, timestamp)["result"]
    assert len(result) == 1, result
    return float(result[0]["value"][1])


def wait_for_positive_scalar_in_prometheus_writer(query):
    # Prometheus sees its own counters through its self-scrape, which lags behind what it has just sent.
    return wait_condition(lambda: get_scalar_from_prometheus_writer(query), lambda value: value > 0, max_attempts=60, delay=0.5)


def get_last_stored_histogram(condition):
    """Returns the newest stored histogram matching the condition as a dict with the metric name, the tags, the timestamp in ms,
    the count (of the flavour of the histogram, bit 0 of `flags`) and the sum."""
    return json.loads(
        node.query(
            f"""
            SELECT t.metric_name AS metric_name, t.tags AS tags, toUnixTimestamp64Milli(h.timestamp) AS timestamp_ms,
                   if(bitTest(h.flags, 0), h.count_float, toFloat64(h.count_int)) AS count, h.sum AS sum
            FROM timeSeriesSamples(prometheus) AS s ARRAY JOIN s.histograms AS h
            JOIN timeSeriesTags(prometheus) AS t ON s.id = t.id
            WHERE {condition}
            ORDER BY h.timestamp DESC
            LIMIT 1
            FORMAT JSONEachRow
            """
        )
    )


def test_histogram_matches_prometheus():
    # The histogram stored in ClickHouse must be the one Prometheus has in its own storage at that time.
    stored = get_last_stored_histogram("h.schema BETWEEN -4 AND 8")
    # The `tags` map of the tags table includes `__name__`, which the selector already carries as the metric name.
    labels = ", ".join(f"{name}={json.dumps(value)}" for name, value in stored["tags"].items() if name != "__name__")
    selector = f'{stored["metric_name"]}{{{labels}}}'
    timestamp = stored["timestamp_ms"] / 1000

    assert get_scalar_from_prometheus_writer(f"histogram_count({selector})", timestamp) == stored["count"]
    assert math.isclose(get_scalar_from_prometheus_writer(f"histogram_sum({selector})", timestamp), stored["sum"], rel_tol=1e-12)


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


def remote_read_all(host, port, path, end_timestamp_ms):
    """Every time series with samples in [0, end_timestamp_ms] read over the remote-read protocol: {sorted labels: prompb.TimeSeries}."""
    read_request = remote_pb2.ReadRequest()
    query = read_request.queries.add()
    query.start_timestamp_ms = 0
    query.end_timestamp_ms = end_timestamp_ms
    query.matchers.append(types_pb2.LabelMatcher(type=types_pb2.LabelMatcher.Type.RE, name="__name__", value=".+"))
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
    # The semantic digest of remote read against Prometheus: everything Prometheus scraped and sent is read back from both systems
    # over the remote-read protocol and compared series by series and sample by sample after canonicalization, since the same
    # buckets come back cut into other spans and with other counter reset hints, which each system derives from its storage.
    # Prometheus keeps scraping while the test runs and sends every series in order, so what ClickHouse holds of a series is a
    # prefix of what Prometheus holds: the comparison stops at the last timestamp ClickHouse has stored for the series, and
    # ClickHouse is read first, so that Prometheus has everything ClickHouse has.
    end_timestamp_ms = int(time.time() * 1000) + 3_600_000
    from_clickhouse = remote_read_all(node.ip_address, 9093, "/read", end_timestamp_ms)
    from_prometheus = remote_read_all(
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
    # Prometheus counts the histograms it sent and the ones the receiver rejected; ClickHouse counts the ones it dropped.
    wait_for_positive_scalar_in_prometheus_writer("sum(prometheus_remote_storage_histograms_total)")
    # The dropped counter has one series per reason, so it has no series at all when nothing was dropped.
    assert get_scalar_from_prometheus_writer("sum(prometheus_remote_storage_histograms_failed_total) or vector(0)") == 0
    assert get_scalar_from_prometheus_writer("sum(prometheus_remote_storage_histograms_dropped_total) or vector(0)") == 0
    assert node.query("SELECT sum(value) FROM system.events WHERE event = 'PrometheusRemoteWriteDroppedHistograms'") == "0\n"
    assert not node.contains_in_log("Invalid histogram sample")
