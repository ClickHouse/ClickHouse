import struct

import pytest
import requests

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV
from .prometheus_test_utils import (
    execute_query_via_http_api,
    execute_range_query_via_http_api,
    get_response_to_remote_write,
    remote_pb2,
    send_protobuf_to_remote_write,
    types_pb2,
)


cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
)

# The Prometheus stale marker: a NaN with this exact payload.
STALE_NAN = struct.unpack("<d", struct.pack("<Q", 0x7FF0000000000002))[0]


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
        node.query("DROP TABLE IF EXISTS default.prometheus SYNC")


def make_write_request(labels, histograms, samples=None):
    write_request = remote_pb2.WriteRequest()
    ts = types_pb2.TimeSeries()
    for name, value in labels.items():
        ts.labels.append(types_pb2.Label(name=name, value=value))
    for histogram in histograms:
        ts.histograms.append(histogram)
    for timestamp_ms, value in (samples or {}).items():
        ts.samples.append(types_pb2.Sample(timestamp=timestamp_ms, value=value))
    write_request.timeseries.append(ts)
    return write_request


def send(write_request):
    send_protobuf_to_remote_write(node.ip_address, 9093, "/write", write_request)


HISTOGRAM_COLUMNS = (
    "timestamp, flags, schema, zero_threshold, count, sum, zero_count,"
    " positive_spans, positive_values, negative_spans, negative_values, custom_values"
)


def query_histograms(columns=HISTOGRAM_COLUMNS):
    return node.query(
        f"SELECT {columns} FROM timeSeriesHistograms(prometheus) ORDER BY timestamp"
    )


def test_int_histogram():
    node.query(
        "CREATE TABLE prometheus ENGINE=TimeSeries SETTINGS store_native_histograms = 1"
    )
    histogram = types_pb2.Histogram(
        count_int=10,
        sum=25.5,
        schema=3,
        zero_threshold=0.001,
        zero_count_int=2,
        positive_spans=[
            types_pb2.BucketSpan(offset=0, length=2),
            types_pb2.BucketSpan(offset=1, length=1),
        ],
        positive_deltas=[3, -1, 1],  # decoded to absolute values [3, 2, 3]
        negative_spans=[types_pb2.BucketSpan(offset=-1, length=1)],
        negative_deltas=[2],
        reset_hint=types_pb2.Histogram.ResetHint.NO,
        timestamp=1704067201000,
    )
    events_before = int(
        node.query(
            "SELECT ifNull(sum(value), 0) FROM system.events WHERE event = 'PrometheusRemoteWriteHistograms'"
        )
    )
    send(make_write_request({"__name__": "test_hist", "job": "test"}, [histogram]))

    # flags: not float, no gauge, no stale marker; counter_reset_hint NO(2) in bits 1-2.
    assert query_histograms() == TSV(
        [
            [
                "2024-01-01 00:00:01.000",
                "4",
                "3",
                "0.001",
                "10",
                "25.5",
                "2",
                "[(0,2),(1,1)]",
                "[3,2,3]",
                "[(-1,1)]",
                "[2]",
                "[]",
            ]
        ]
    )

    # The histogram rows share the series id with the tags table.
    assert node.query(
        "SELECT t.metric_name, t.tags['job'], count()"
        " FROM timeSeriesHistograms(prometheus) AS h"
        " JOIN timeSeriesTags(prometheus) AS t ON h.id = t.id"
        " GROUP BY t.metric_name, t.tags['job']"
    ) == TSV([["test_hist", "test", "1"]])

    # min_time/max_time in the tags table are updated from histogram timestamps.
    assert node.query(
        "SELECT min(min_time), max(max_time) FROM timeSeriesTags(prometheus)"
        " WHERE metric_name = 'test_hist'"
    ) == TSV([["2024-01-01 00:00:01.000", "2024-01-01 00:00:01.000"]])

    events_after = int(
        node.query(
            "SELECT ifNull(sum(value), 0) FROM system.events WHERE event = 'PrometheusRemoteWriteHistograms'"
        )
    )
    assert events_after == events_before + 1


def test_float_histogram_and_nhcb():
    node.query(
        "CREATE TABLE prometheus ENGINE=TimeSeries SETTINGS store_native_histograms = 1"
    )
    float_histogram = types_pb2.Histogram(
        count_float=6.5,
        sum=12.25,
        schema=3,
        zero_threshold=0.001,
        zero_count_float=1.5,
        positive_spans=[types_pb2.BucketSpan(offset=0, length=2)],
        positive_counts=[4.5, 2],  # float histograms carry absolute values
        reset_hint=types_pb2.Histogram.ResetHint.GAUGE,
        timestamp=1704067202000,
    )
    # A histogram with custom bucket boundaries (NHCB): schema -53 + custom_values.
    nhcb_histogram = types_pb2.Histogram(
        count_int=7,
        sum=3.5,
        schema=-53,
        positive_spans=[types_pb2.BucketSpan(offset=0, length=3)],
        positive_deltas=[2, 1, 1],  # decoded to absolute values [2, 3, 4]
        custom_values=[0.1, 0.5, 1],
        timestamp=1704067203000,
    )
    send(
        make_write_request(
            {"__name__": "test_hist_float"}, [float_histogram, nhcb_histogram]
        )
    )

    # Float histogram flags: is_float(1) | counter_reset_hint GAUGE(3)<<1 = 7.
    assert query_histograms() == TSV(
        [
            [
                "2024-01-01 00:00:02.000",
                "7",
                "3",
                "0.001",
                "6.5",
                "12.25",
                "1.5",
                "[(0,2)]",
                "[4.5,2]",
                "[]",
                "[]",
                "[]",
            ],
            [
                "2024-01-01 00:00:03.000",
                "0",
                "-53",
                "0",
                "7",
                "3.5",
                "0",
                "[(0,3)]",
                "[2,3,4]",
                "[]",
                "[]",
                "[0.1,0.5,1]",
            ],
        ]
    )


def test_stale_marker():
    node.query(
        "CREATE TABLE prometheus ENGINE=TimeSeries SETTINGS store_native_histograms = 1"
    )
    stale = types_pb2.Histogram(sum=STALE_NAN, timestamp=1704067204000)
    send(make_write_request({"__name__": "test_hist_stale"}, [stale]))

    # flags: stale marker bit (0x10); the NaN payload is preserved bit-exactly.
    assert node.query(
        "SELECT flags, hex(reinterpretAsUInt64(sum)) FROM timeSeriesHistograms(prometheus)"
    ) == TSV([["16", "7FF0000000000002"]])


def test_mixed_samples_and_histograms():
    node.query(
        "CREATE TABLE prometheus ENGINE=TimeSeries SETTINGS store_native_histograms = 1"
    )
    histogram = types_pb2.Histogram(
        count_int=1,
        sum=0.5,
        schema=0,
        positive_spans=[types_pb2.BucketSpan(offset=0, length=1)],
        positive_deltas=[1],
        timestamp=1704067206000,
    )
    send(
        make_write_request(
            {"__name__": "test_mixed"}, [histogram], samples={1704067205000: 42.0}
        )
    )

    assert node.query(
        "SELECT timestamp, value FROM timeSeriesSamples(prometheus)"
    ) == TSV([["2024-01-01 00:00:05.000", "42"]])
    assert query_histograms("timestamp, count, sum") == TSV(
        [["2024-01-01 00:00:06.000", "1", "0.5"]]
    )
    # min_time comes from the sample, max_time from the histogram.
    assert node.query(
        "SELECT min(min_time), max(max_time) FROM timeSeriesTags(prometheus)"
        " WHERE metric_name = 'test_mixed'"
    ) == TSV([["2024-01-01 00:00:05.000", "2024-01-01 00:00:06.000"]])


def test_table_without_histograms_target_drops_histograms():
    node.query("CREATE TABLE prometheus ENGINE=TimeSeries")
    histogram = types_pb2.Histogram(
        count_int=1,
        sum=0.5,
        schema=0,
        positive_spans=[types_pb2.BucketSpan(offset=0, length=1)],
        positive_deltas=[1],
        timestamp=1704067208000,
    )
    # The write succeeds (204): samples are stored, histograms are dropped with a warning.
    send(
        make_write_request(
            {"__name__": "test_no_target"}, [histogram], samples={1704067207000: 1.0}
        )
    )

    assert node.query(
        "SELECT timestamp, value FROM timeSeriesSamples(prometheus)"
    ) == TSV([["2024-01-01 00:00:07.000", "1"]])
    assert node.contains_in_log("Dropping 1 native histogram samples")
    assert (
        node.query(
            "SELECT value FROM system.events WHERE event = 'PrometheusRemoteWriteDroppedHistograms'"
        )
        == "1\n"
    )


def test_invalid_histograms_rejected():
    node.query(
        "CREATE TABLE prometheus ENGINE=TimeSeries SETTINGS store_native_histograms = 1"
    )

    def assert_rejected(histogram):
        response = get_response_to_remote_write(
            node.ip_address,
            9093,
            "/write",
            make_write_request({"__name__": "test_bad"}, [histogram]),
        )
        assert response.status_code == requests.codes.bad_request
        assert node.query("SELECT count() FROM timeSeriesHistograms(prometheus)") == "0\n"

    # Spans cover 3 buckets but only 2 delta values are given.
    assert_rejected(
        types_pb2.Histogram(
            count_int=5,
            sum=1.5,
            positive_spans=[types_pb2.BucketSpan(offset=0, length=3)],
            positive_deltas=[3, -1],
            timestamp=1704067209000,
        )
    )
    # Delta decoding yields a negative bucket count.
    assert_rejected(
        types_pb2.Histogram(
            count_int=5,
            sum=1.5,
            positive_spans=[types_pb2.BucketSpan(offset=0, length=2)],
            positive_deltas=[3, -5],
            timestamp=1704067210000,
        )
    )
    # An int count with a float zero count (inconsistent oneof arms).
    assert_rejected(
        types_pb2.Histogram(
            count_int=5,
            sum=1.5,
            zero_count_float=1.0,
            timestamp=1704067211000,
        )
    )
    # A float histogram carrying integer bucket deltas.
    assert_rejected(
        types_pb2.Histogram(
            count_float=5.0,
            sum=1.5,
            zero_count_float=1.0,
            positive_spans=[types_pb2.BucketSpan(offset=0, length=1)],
            positive_deltas=[3],
            timestamp=1704067212000,
        )
    )
    # An int histogram carrying float bucket counts.
    assert_rejected(
        types_pb2.Histogram(
            count_int=5,
            sum=1.5,
            zero_count_int=1,
            positive_spans=[types_pb2.BucketSpan(offset=0, length=1)],
            positive_counts=[3.0],
            timestamp=1704067213000,
        )
    )
    # Delta decoding overflows int64.
    assert_rejected(
        types_pb2.Histogram(
            count_int=1,
            sum=0.0,
            positive_spans=[types_pb2.BucketSpan(offset=0, length=2)],
            positive_deltas=[9223372036854775807, 1],
            timestamp=1704067214000,
        )
    )
    # An out-of-range counter reset hint (proto3 enums are open).
    assert_rejected(
        types_pb2.Histogram(
            count_int=1,
            sum=0.0,
            reset_hint=7,
            timestamp=1704067215000,
        )
    )
    # An out-of-range bucket schema.
    assert_rejected(
        types_pb2.Histogram(
            count_int=1,
            sum=0.0,
            schema=42,
            timestamp=1704067216000,
        )
    )
    # A negative count. The int arms are unsigned, so only the float ones can carry one.
    assert_rejected(
        types_pb2.Histogram(
            count_float=-1.0,
            sum=0.0,
            zero_count_float=0.0,
            timestamp=1704067217000,
        )
    )
    # A negative zero count.
    assert_rejected(
        types_pb2.Histogram(
            count_float=1.0,
            sum=0.0,
            zero_count_float=-1.0,
            timestamp=1704067218000,
        )
    )
    # A negative float bucket count.
    assert_rejected(
        types_pb2.Histogram(
            count_float=1.0,
            sum=0.0,
            zero_count_float=0.0,
            positive_spans=[types_pb2.BucketSpan(offset=0, length=1)],
            positive_counts=[-1.0],
            timestamp=1704067219000,
        )
    )
    # An integer count above 2^53, which the Float64 carrier cannot hold exactly: rejected rather
    # than rounded, so a stored integer histogram always reads back the count it arrived with.
    assert_rejected(
        types_pb2.Histogram(
            count_int=(1 << 53) + 1,
            sum=0.0,
            timestamp=1704067220000,
        )
    )
    # The same bound applies to a decoded bucket count.
    assert_rejected(
        types_pb2.Histogram(
            count_int=1,
            sum=0.0,
            positive_spans=[types_pb2.BucketSpan(offset=0, length=1)],
            positive_deltas=[(1 << 53) + 1],
            timestamp=1704067221000,
        )
    )
    # A custom-bucket histogram whose spans reach past the bounds they declare: storing it would
    # fail only later, when writeHistogram renders it and finds no bound for its buckets.
    assert_rejected(
        types_pb2.Histogram(
            count_int=2,
            sum=0.0,
            schema=-53,
            positive_spans=[types_pb2.BucketSpan(offset=0, length=2)],
            positive_deltas=[1, 1],
            timestamp=1704067222000,
        )
    )
    # A custom-bucket histogram with a negative bucket index (no -Inf lower bound exists there).
    assert_rejected(
        types_pb2.Histogram(
            count_int=1,
            sum=0.0,
            schema=-53,
            positive_spans=[types_pb2.BucketSpan(offset=-1, length=1)],
            positive_deltas=[1],
            timestamp=1704067223000,
        )
    )
    # A custom-bucket histogram carrying negative buckets: custom buckets are positive-side only.
    assert_rejected(
        types_pb2.Histogram(
            count_int=1,
            sum=0.0,
            schema=-53,
            negative_spans=[types_pb2.BucketSpan(offset=0, length=1)],
            negative_deltas=[1],
            timestamp=1704067224000,
        )
    )
    # Custom bucket bounds on an exponential schema, where no reader knows what to do with them.
    assert_rejected(
        types_pb2.Histogram(
            count_int=1,
            sum=0.0,
            schema=0,
            custom_values=[1.0, 2.0],
            timestamp=1704067225000,
        )
    )
    # An undefined bucket schema in the gap between the exponential range and custom buckets.
    assert_rejected(
        types_pb2.Histogram(
            count_int=1,
            sum=0.0,
            schema=-20,
            timestamp=1704067226000,
        )
    )


# Valid custom-bucket payloads are accepted: their spans stay within the declared bounds
# (one past the last bound is allowed, its upper limit is +Inf).
def test_nhcb_accepted():
    node.query(
        "CREATE TABLE prometheus ENGINE=TimeSeries SETTINGS store_native_histograms = 1"
    )
    send(
        make_write_request(
            {"__name__": "test_hist_nhcb_bounds"},
            [
                types_pb2.Histogram(
                    count_int=4,
                    sum=2.0,
                    schema=-53,
                    positive_spans=[types_pb2.BucketSpan(offset=0, length=3)],
                    positive_deltas=[2, 1, 0],  # decoded to absolute values [2, 3, 3]
                    custom_values=[0.5, 1.0],  # buckets 0..1 bounded, bucket 2 unbounded (+Inf)
                    timestamp=1704067227000,
                )
            ],
        )
    )

    assert node.query(
        "SELECT count FROM timeSeriesHistograms(prometheus)"
    ) == TSV([["4"]])


# The HTTP JSON rendering of a coarse exponential schema: bucket indexes are negative on both
# sides here, so the boundary computation must scale by the (negative) schema without UB and
# produce the same boundaries Prometheus does (2^(idx * 2) for schema -1).
def test_http_json_coarse_schema():
    node.query(
        "CREATE TABLE prometheus ENGINE=TimeSeries SETTINGS store_native_histograms = 1"
    )
    histogram = types_pb2.Histogram(
        count_int=4,
        sum=-20.75,
        schema=-1,
        zero_threshold=0.001,
        zero_count_int=0,
        positive_spans=[types_pb2.BucketSpan(offset=-1, length=1)],
        positive_deltas=[1],  # bucket index -1: (2^-4, 2^-2]
        negative_spans=[types_pb2.BucketSpan(offset=1, length=2)],
        negative_deltas=[1, 1],  # bucket indexes 1, 2: [-4, -1) and [-16, -4)
        timestamp=1704067201000,
    )
    send(make_write_request({"__name__": "test_hist_coarse", "job": "test"}, [histogram]))

    data = execute_query_via_http_api(
        node.ip_address, 9093, "/api/v1/query", "test_hist_coarse", timestamp=1704067201
    )
    assert data == {
        "resultType": "vector",
        "result": [
            {
                "metric": {"__name__": "test_hist_coarse", "job": "test"},
                "histogram": [
                    1704067201,
                    {
                        "count": "4",
                        "sum": "-20.75",
                        "buckets": [
                            [1, "-16", "-4", "2"],
                            [1, "-4", "-1", "1"],
                            [0, "0.0625", "0.25", "1"],
                        ],
                    },
                ],
            }
        ],
    }


# The HTTP JSON rendering of an NHCB (schema -53) histogram: every custom bucket uses boundary
# rule 0, including the first one, whose lower bound is -Inf.
def test_http_json_nhcb():
    node.query(
        "CREATE TABLE prometheus ENGINE=TimeSeries SETTINGS store_native_histograms = 1"
    )
    histogram = types_pb2.Histogram(
        count_int=3,
        sum=4.5,
        schema=-53,
        zero_threshold=0.0,
        zero_count_int=0,
        positive_spans=[types_pb2.BucketSpan(offset=0, length=2)],
        positive_deltas=[2, -1],  # decoded to absolute values [2, 1]
        custom_values=[1.0, 2.5],
        timestamp=1704067202000,
    )
    send(make_write_request({"__name__": "test_hist_nhcb", "job": "test"}, [histogram]))

    data = execute_query_via_http_api(
        node.ip_address, 9093, "/api/v1/query", "test_hist_nhcb", timestamp=1704067202
    )
    assert data == {
        "resultType": "vector",
        "result": [
            {
                "metric": {"__name__": "test_hist_nhcb", "job": "test"},
                "histogram": [
                    1704067202,
                    {
                        "count": "3",
                        "sum": "4.5",
                        "buckets": [
                            [0, "-Inf", "1", "2"],
                            [0, "1", "2.5", "1"],
                        ],
                    },
                ],
            }
        ],
    }


# A series whose histogram samples are all stale markers must be dropped from a query_range
# response entirely: emitting it would produce a matrix element with neither "values" nor
# "histograms".
def test_http_query_range_stale_only_series():
    node.query(
        "CREATE TABLE prometheus ENGINE=TimeSeries SETTINGS store_native_histograms = 1"
    )
    stale = types_pb2.Histogram(sum=STALE_NAN, timestamp=1704067203000)
    send(make_write_request({"__name__": "test_hist_stale_range", "job": "test"}, [stale]))

    data = execute_range_query_via_http_api(
        node.ip_address,
        9093,
        "/api/v1/query_range",
        "test_hist_stale_range",
        1704067200,
        1704067260,
        15,
    )
    assert data == {"resultType": "matrix", "result": []}
