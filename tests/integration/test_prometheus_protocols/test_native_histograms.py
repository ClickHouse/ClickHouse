import struct

import pytest
import requests

from helpers.cluster import ClickHouseCluster
from .prometheus_test_utils import (
    extract_protobuf_from_remote_read_response,
    get_response_to_remote_read,
    get_response_to_remote_write,
    remote_pb2,
    send_protobuf_to_remote_write,
    types_pb2,
)


# Native histograms over the remote-write protocol: the requests are hand-crafted, so every corner of the protocol can be
# exercised, including what a real Prometheus never sends. See test_native_histograms_prometheus.py for a real sender.

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/prometheus.xml", "configs/config.d/query_log.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
)

# The stale marker: a NaN with a specific payload, sent by Prometheus in `sum` when a series goes stale.
STALE_NAN = struct.unpack("<d", struct.pack("<Q", 0x7FF0000000000002))[0]

TIMESTAMP_MS = 1724112000000  # 2024-08-20 00:00:00 UTC

HISTOGRAMS_QUERY = """
    SELECT toUnixTimestamp64Milli(timestamp), is_float, counter_reset_hint, schema, zero_threshold, sum,
           positive_spans, negative_spans, custom_values,
           count_int, zero_count_int, positive_values_int, negative_values_int,
           count_float, zero_count_float, positive_values_float, negative_values_float
    FROM timeSeriesHistograms(prometheus)
    {where}
    ORDER BY timestamp
    FORMAT TSV
"""


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def create_table():
    node.query("CREATE TABLE prometheus ENGINE=TimeSeries")
    try:
        yield
    finally:
        node.query("DROP TABLE IF EXISTS prometheus SYNC")


def span(offset, length):
    return types_pb2.BucketSpan(offset=offset, length=length)


def make_write_request(*series):
    """`series` are (labels, histograms, samples) tuples; `samples` is a dict {timestamp_ms: value} and may be omitted."""
    write_request = remote_pb2.WriteRequest()
    for labels, histograms, *rest in series:
        time_series = types_pb2.TimeSeries()
        for name, value in labels.items():
            time_series.labels.append(types_pb2.Label(name=name, value=value))
        time_series.histograms.extend(histograms)
        for timestamp_ms, value in (rest[0] if rest else {}).items():
            time_series.samples.append(types_pb2.Sample(timestamp=timestamp_ms, value=value))
        write_request.timeseries.append(time_series)
    return write_request


def send(write_request, path="/write"):
    send_protobuf_to_remote_write(node.ip_address, 9093, path, write_request)


def read_histograms(where=""):
    return node.query(HISTOGRAMS_QUERY.format(where=where))


def check_histogram_round_trip(histogram, expected_row):
    """Sends one histogram sample of one time series and checks everything it must leave behind: the stored row
    (`expected_row` is its TSV form, see HISTOGRAMS_QUERY), the time series it is attached to, the time range of the series,
    and the profile events."""
    events_before = get_events()
    send(make_write_request(({"__name__": "hist", "job": "api"}, [histogram])))

    assert read_histograms() == expected_row + "\n"
    assert node.query("SELECT metric_name, tags['job'], length(histograms.timestamp), length(samples) FROM prometheus") == "hist\tapi\t1\t0\n"
    assert node.query(
        "SELECT metric_name, toUnixTimestamp64Milli(min_time), toUnixTimestamp64Milli(max_time) FROM timeSeriesTags(prometheus)"
    ) == f"hist\t{histogram.timestamp}\t{histogram.timestamp}\n"

    events_after = get_events()
    assert events_after["PrometheusRemoteWriteHistograms"] == events_before["PrometheusRemoteWriteHistograms"] + 1
    assert events_after["PrometheusRemoteWriteDroppedHistograms"] == events_before["PrometheusRemoteWriteDroppedHistograms"]


def get_event(name):
    return int(node.query(f"SELECT sum(value) FROM system.events WHERE event = '{name}'"))


def get_events():
    return {name: get_event(name) for name in ("PrometheusRemoteWriteHistograms", "PrometheusRemoteWriteDroppedHistograms")}


def test_integer_histogram():
    # Two positive spans covering the bucket indexes -2, -1 and 1, one negative span covering the index 0.
    # The deltas [3, -1, 1] decode to the absolute counts [3, 2, 3]; with the zero bucket they sum up to `count_int`.
    histogram = types_pb2.Histogram(
        count_int=10,
        sum=12.5,
        schema=3,
        zero_threshold=0.001,
        zero_count_int=2,
        positive_spans=[span(-2, 2), span(1, 1)],
        positive_deltas=[3, -1, 1],
        negative_spans=[span(0, 1)],
        negative_deltas=[0],
        reset_hint=types_pb2.Histogram.ResetHint.NO,
        timestamp=TIMESTAMP_MS,
    )
    check_histogram_round_trip(
        histogram, f"{TIMESTAMP_MS}\tfalse\t2\t3\t0.001\t12.5\t[(-2,2),(1,1)]\t[(0,1)]\t[]\t10\t2\t[3,2,3]\t[0]\t0\t0\t[]\t[]"
    )


def test_float_histogram():
    # Float bucket counts are copied as sent; the integer columns stay at their defaults.
    histogram = types_pb2.Histogram(
        count_float=5.5,
        sum=3.25,
        schema=0,
        zero_threshold=0,
        zero_count_float=0.5,
        positive_spans=[span(0, 2)],
        positive_counts=[2.5, 2.5],
        reset_hint=types_pb2.Histogram.ResetHint.GAUGE,
        timestamp=TIMESTAMP_MS,
    )
    check_histogram_round_trip(histogram, f"{TIMESTAMP_MS}\ttrue\t3\t0\t0\t3.25\t[(0,2)]\t[]\t[]\t0\t0\t[]\t[]\t5.5\t0.5\t[2.5,2.5]\t[]")


def test_custom_buckets_histogram():
    # Custom buckets (NHCB): `schema = -53`, the upper bounds in `custom_values`, the last bucket up to +Inf has no bound.
    # No Prometheus version sends these over remote-write 1.0, but other senders (e.g. the OpenTelemetry Collector) do.
    histogram = types_pb2.Histogram(
        count_int=9,
        sum=4.2,
        schema=-53,
        custom_values=[0.1, 0.5, 1],
        positive_spans=[span(0, 3)],
        positive_deltas=[2, 1, 1],
        timestamp=TIMESTAMP_MS,
    )
    check_histogram_round_trip(histogram, f"{TIMESTAMP_MS}\tfalse\t0\t-53\t0\t4.2\t[(0,3)]\t[]\t[0.1,0.5,1]\t9\t0\t[2,3,4]\t[]\t0\t0\t[]\t[]")


def test_stale_marker_and_unset_count():
    # A stale marker is a histogram whose `sum` is the stale NaN; Prometheus sends nothing else. The NaN payload must survive
    # bit-exactly, that is how staleness is detected. A histogram without a `count` arm is an integer histogram with count 0.
    stale = types_pb2.Histogram(sum=STALE_NAN, timestamp=TIMESTAMP_MS)
    check_histogram_round_trip(stale, f"{TIMESTAMP_MS}\tfalse\t0\t0\t0\tnan\t[]\t[]\t[]\t0\t0\t[]\t[]\t0\t0\t[]\t[]")
    assert node.query("SELECT hex(reinterpretAsUInt64(sum)) FROM timeSeriesHistograms(prometheus)") == "7FF0000000000002\n"


def test_samples_and_histograms_together():
    histogram = types_pb2.Histogram(count_int=1, sum=1, positive_spans=[span(0, 1)], positive_deltas=[1], timestamp=TIMESTAMP_MS + 2000)
    send(
        make_write_request(
            # A series with both kinds of samples: the time range of the series covers both.
            ({"__name__": "mixed"}, [histogram], {TIMESTAMP_MS: 1.0, TIMESTAMP_MS + 1000: 2.0}),
            # A float-only series in the same request gets an empty histograms group.
            ({"__name__": "float_only"}, [], {TIMESTAMP_MS: 3.0}),
            # A histogram-only series still gets its tags row.
            ({"__name__": "hist_only"}, [histogram]),
        )
    )

    assert node.query("SELECT metric_name, length(samples), length(histograms.timestamp) FROM prometheus ORDER BY metric_name") == (
        "float_only\t1\t0\n"
        "hist_only\t0\t1\n"
        "mixed\t2\t1\n"
    )
    assert node.query(
        "SELECT metric_name, toUnixTimestamp64Milli(min_time), toUnixTimestamp64Milli(max_time) "
        "FROM timeSeriesTags(prometheus) ORDER BY metric_name"
    ) == (
        f"float_only\t{TIMESTAMP_MS}\t{TIMESTAMP_MS}\n"
        f"hist_only\t{TIMESTAMP_MS + 2000}\t{TIMESTAMP_MS + 2000}\n"
        f"mixed\t{TIMESTAMP_MS}\t{TIMESTAMP_MS + 2000}\n"
    )
    assert node.query("SELECT count() FROM timeSeriesSamples(prometheus)") == "3\n"
    assert node.query("SELECT count() FROM timeSeriesHistograms(prometheus)") == "2\n"


def test_integer_counts_are_exact():
    # Integer counts are stored as UInt64, so counts which a Float64 can't represent read back exactly.
    bucket = (1 << 53) + 1
    zero_count = (1 << 60) + 3
    histogram = types_pb2.Histogram(
        count_int=bucket + zero_count,
        sum=1,
        zero_count_int=zero_count,
        positive_spans=[span(0, 1)],
        positive_deltas=[bucket],
        timestamp=TIMESTAMP_MS,
    )
    send(make_write_request(({"__name__": "big"}, [histogram])))

    assert node.query("SELECT count_int, zero_count_int, positive_values_int FROM timeSeriesHistograms(prometheus)") == (
        f"{bucket + zero_count}\t{zero_count}\t[{bucket}]\n"
    )


def test_async_insert():
    # The block with the `histograms.*` columns goes through the asynchronous insert queue (serialized, merged and flushed)
    # instead of straight into the table.
    histogram = types_pb2.Histogram(count_int=1, sum=1, positive_spans=[span(0, 1)], positive_deltas=[1], timestamp=TIMESTAMP_MS)
    send(make_write_request(({"__name__": "async_hist"}, [histogram], {TIMESTAMP_MS: 1.0})), path="/write?async_insert=1")

    assert node.query("SELECT count() FROM timeSeriesHistograms(prometheus)") == "1\n"
    assert node.query("SELECT count() FROM timeSeriesSamples(prometheus)") == "1\n"


def test_request_without_histograms_inserts_the_same_columns():
    # A request without histograms must produce the same INSERT as before histograms existed: no `histograms.*` columns.
    # The URL parameters are settings of the insert, so `log_comment` marks the insert of each request in the query log.
    histogram = types_pb2.Histogram(count_int=1, sum=1, positive_spans=[span(0, 1)], positive_deltas=[1], timestamp=TIMESTAMP_MS)
    send(make_write_request(({"__name__": "with_hist"}, [histogram])), path="/write?log_comment=with_histograms")
    send(make_write_request(({"__name__": "without_hist"}, [], {TIMESTAMP_MS: 1.0})), path="/write?log_comment=without_histograms")

    node.query("SYSTEM FLUSH LOGS query_log")
    inserts = node.query(
        "SELECT log_comment, query LIKE '%histograms.timestamp%' FROM system.query_log "
        "WHERE type = 'QueryFinish' AND query_kind = 'Insert' AND log_comment IN ('with_histograms', 'without_histograms') "
        "ORDER BY log_comment"
    )
    assert inserts == "with_histograms\t1\nwithout_histograms\t0\n"


def test_many_histograms():
    num_series = 100
    num_histograms_per_series = 3
    series = []
    for i in range(num_series):
        histograms = [
            types_pb2.Histogram(
                count_int=j + 1,
                sum=j,
                positive_spans=[span(0, 1)],
                positive_deltas=[j + 1],
                timestamp=TIMESTAMP_MS + j * 1000,
            )
            for j in range(num_histograms_per_series)
        ]
        series.append(({"__name__": "many", "instance": str(i)}, histograms))

    events_before = get_events()
    send(make_write_request(*series))
    events_after = get_events()

    total = num_series * num_histograms_per_series
    assert node.query("SELECT count() FROM timeSeriesHistograms(prometheus)") == f"{total}\n"
    assert node.query("SELECT count() FROM timeSeriesTags(prometheus)") == f"{num_series}\n"
    assert events_after["PrometheusRemoteWriteHistograms"] == events_before["PrometheusRemoteWriteHistograms"] + total


def expect_rejected(histogram, expected_error):
    response = get_response_to_remote_write(
        node.ip_address, 9093, "/write", make_write_request(({"__name__": "bad"}, [histogram]))
    )
    assert response.status_code == requests.codes.bad_request, response.text
    assert expected_error in response.text, response.text
    # The whole request is rejected before anything is written, the tags included.
    for table_function in ("timeSeriesTags", "timeSeriesSamples", "timeSeriesHistograms"):
        assert node.query(f"SELECT count() FROM {table_function}(prometheus)") == "0\n"


def test_invalid_histograms_are_rejected():
    # What the conversion itself rejects: things the columns can't represent.
    expect_rejected(
        types_pb2.Histogram(count_int=1, sum=1, positive_spans=[span(0, 2)], positive_deltas=[1, -2], timestamp=TIMESTAMP_MS),
        "positive side: bucket #1 has a negative count -1 after decoding the deltas",
    )
    expect_rejected(
        types_pb2.Histogram(count_int=1, sum=1, negative_spans=[span(0, 2)], negative_deltas=[(1 << 63) - 1, 1], timestamp=TIMESTAMP_MS),
        "negative side: the bucket counts overflow Int64 while decoding the deltas",
    )
    expect_rejected(
        types_pb2.Histogram(count_int=1, sum=1, zero_count_float=1, timestamp=TIMESTAMP_MS),
        "an integer histogram (count_float is not set) has zero_count_float instead of zero_count_int",
    )
    expect_rejected(
        types_pb2.Histogram(count_float=1, sum=1, zero_count_int=1, timestamp=TIMESTAMP_MS),
        "a float histogram (count_float is set) has zero_count_int instead of zero_count_float",
    )
    expect_rejected(
        types_pb2.Histogram(count_float=1, sum=1, positive_spans=[span(0, 1)], positive_deltas=[1], timestamp=TIMESTAMP_MS),
        "a float histogram (count_float is set) has integer bucket deltas instead of float bucket counts",
    )
    expect_rejected(
        types_pb2.Histogram(count_int=1, sum=1, positive_spans=[span(0, 1)], positive_counts=[1], timestamp=TIMESTAMP_MS),
        "an integer histogram (count_float is not set) has float bucket counts instead of integer bucket deltas",
    )
    # Values which would wrap into valid ones if they were cast to the column types without a check: -309 into -53, 260 into 4.
    expect_rejected(types_pb2.Histogram(sum=1, schema=-309, timestamp=TIMESTAMP_MS), "schema -309 is out of the range -128..127")
    expect_rejected(types_pb2.Histogram(sum=1, schema=300, timestamp=TIMESTAMP_MS), "schema 300 is out of the range -128..127")
    expect_rejected(types_pb2.Histogram(sum=1, reset_hint=260, timestamp=TIMESTAMP_MS), "reset_hint 260 is out of the range 0..255")

    # What the validation of the sink rejects: the same rules apply to INSERT queries, see 05235_timeseries_histograms_validation.
    expect_rejected(types_pb2.Histogram(sum=1, schema=9, timestamp=TIMESTAMP_MS), "schema 9 is invalid")
    expect_rejected(types_pb2.Histogram(sum=1, reset_hint=4, timestamp=TIMESTAMP_MS), "counter_reset_hint 4 is out of the range 0..3")
    expect_rejected(
        types_pb2.Histogram(count_int=1, sum=1, positive_spans=[span(0, 2)], positive_deltas=[1], timestamp=TIMESTAMP_MS),
        "positive side: the spans need 2 buckets, but 1 buckets are given",
    )
    expect_rejected(
        types_pb2.Histogram(count_int=5, sum=1, positive_spans=[span(0, 1)], positive_deltas=[1], timestamp=TIMESTAMP_MS),
        "1 observations are found in the buckets, but count_int is 5",
    )
    expect_rejected(
        types_pb2.Histogram(
            count_int=2, sum=1, schema=-53, custom_values=[1, 0.5], positive_spans=[span(0, 2)], positive_deltas=[1, 0], timestamp=TIMESTAMP_MS
        ),
        "custom_values must be strictly increasing",
    )


def test_max_buckets_setting():
    node.query("DROP TABLE prometheus SYNC")
    node.query("CREATE TABLE prometheus ENGINE=TimeSeries SETTINGS histograms_max_buckets = 2")
    expect_rejected(
        types_pb2.Histogram(count_int=3, sum=1, positive_spans=[span(0, 3)], positive_deltas=[1, 0, 0], timestamp=TIMESTAMP_MS),
        "3 buckets exceed the limit of 2 buckets per histogram",
    )
    send(
        make_write_request(
            ({"__name__": "ok"}, [types_pb2.Histogram(count_int=2, sum=1, positive_spans=[span(0, 2)], positive_deltas=[1, 0], timestamp=TIMESTAMP_MS)])
        )
    )
    assert node.query("SELECT count() FROM timeSeriesHistograms(prometheus)") == "1\n"


def test_table_without_histograms_drops_them():
    # A table of a version before 7 has no histograms table. The histograms of a request are dropped with a warning,
    # the float samples of the same request are written and the request succeeds: rejecting it would lose the samples too.
    node.query("DROP TABLE prometheus SYNC")
    node.query("CREATE TABLE prometheus ENGINE=TimeSeries SETTINGS version = 5")
    histogram = types_pb2.Histogram(count_int=1, sum=1, positive_spans=[span(0, 1)], positive_deltas=[1], timestamp=TIMESTAMP_MS)

    events_before = get_events()
    send(make_write_request(({"__name__": "old_table"}, [histogram], {TIMESTAMP_MS: 1.0})))
    events_after = get_events()

    assert node.query("SELECT count() FROM timeSeriesSamples(prometheus)") == "1\n"
    assert events_after["PrometheusRemoteWriteHistograms"] == events_before["PrometheusRemoteWriteHistograms"] + 1
    assert events_after["PrometheusRemoteWriteDroppedHistograms"] == events_before["PrometheusRemoteWriteDroppedHistograms"] + 1
    assert node.contains_in_log("Dropped 1 native histogram samples: the table has no histograms table because its version 5 is older than 7")


def remote_read(start_ms, end_ms):
    """Reads every time series in [start_ms, end_ms] over remote read: {sorted labels: prompb.TimeSeries}."""
    read_request = remote_pb2.ReadRequest()
    query = read_request.queries.add()
    query.start_timestamp_ms = start_ms
    query.end_timestamp_ms = end_ms
    query.matchers.append(types_pb2.LabelMatcher(type=types_pb2.LabelMatcher.Type.RE, name="__name__", value=".+"))
    response = get_response_to_remote_read(node.ip_address, 9093, "/read", read_request)
    read_response = extract_protobuf_from_remote_read_response(response)
    return {tuple(sorted((label.name, label.value) for label in series.labels)): series for series in read_response.results[0].timeseries}


def labels_key(**labels):
    return tuple(sorted(labels.items()))


def test_remote_read_returns_histograms_exactly():
    # Every histogram written by remote write is read back by remote read as the same message. Both arms of the `count` and
    # `zero_count` oneofs are set explicitly, as Prometheus sends them, so the messages compare equal. Every field has a
    # distinct value, so a mix-up of two fields of the same type (e.g. the positive and the negative side) can't go unnoticed.
    integer_histogram = types_pb2.Histogram(
        count_int=15, zero_count_int=2, sum=12.5, schema=3, zero_threshold=0.001,
        positive_spans=[span(-2, 2), span(1, 1)], positive_deltas=[3, -1, 1], negative_spans=[span(0, 1)], negative_deltas=[5],
        reset_hint=types_pb2.Histogram.ResetHint.NO, timestamp=TIMESTAMP_MS,
    )
    float_histogram = types_pb2.Histogram(
        count_float=5.75, zero_count_float=0.5, sum=3.25, schema=0, zero_threshold=0.002,
        positive_spans=[span(0, 2)], positive_counts=[2.5, 2.5], negative_spans=[span(1, 1)], negative_counts=[0.25],
        reset_hint=types_pb2.Histogram.ResetHint.GAUGE, timestamp=TIMESTAMP_MS + 1000,
    )
    custom_buckets_histogram = types_pb2.Histogram(
        count_int=9, zero_count_int=0, sum=4.2, schema=-53, custom_values=[0.1, 0.5, 1],
        positive_spans=[span(0, 3)], positive_deltas=[2, 1, 1], timestamp=TIMESTAMP_MS + 2000,
    )
    bucket = (1 << 53) + 1
    zero_count = (1 << 60) + 3
    big_histogram = types_pb2.Histogram(
        count_int=bucket + zero_count, zero_count_int=zero_count, sum=1, positive_spans=[span(0, 1)], positive_deltas=[bucket],
        timestamp=TIMESTAMP_MS,
    )

    send(
        make_write_request(
            ({"__name__": "hist", "job": "api"}, [integer_histogram, float_histogram, custom_buckets_histogram], {TIMESTAMP_MS + 3000: 1.5}),
            ({"__name__": "big"}, [big_histogram]),
            ({"__name__": "float_only"}, [], {TIMESTAMP_MS: 2.0}),
        )
    )

    series = remote_read(TIMESTAMP_MS, TIMESTAMP_MS + 10000)
    assert sorted(series) == [labels_key(__name__="big"), labels_key(__name__="float_only"), labels_key(__name__="hist", job="api")]

    hist = series[labels_key(__name__="hist", job="api")]
    assert list(hist.histograms) == [integer_histogram, float_histogram, custom_buckets_histogram]
    assert [(sample.timestamp, sample.value) for sample in hist.samples] == [(TIMESTAMP_MS + 3000, 1.5)]

    assert list(series[labels_key(__name__="big")].histograms) == [big_histogram]

    float_only = series[labels_key(__name__="float_only")]
    assert [(sample.timestamp, sample.value) for sample in float_only.samples] == [(TIMESTAMP_MS, 2.0)]
    assert len(float_only.histograms) == 0

    # The time range of the read applies to histograms too.
    series = remote_read(TIMESTAMP_MS + 500, TIMESTAMP_MS + 1500)
    assert list(series[labels_key(__name__="hist", job="api")].histograms) == [float_histogram]


def test_remote_read_returns_stale_markers_bit_exactly():
    stale = types_pb2.Histogram(count_int=0, zero_count_int=0, sum=STALE_NAN, timestamp=TIMESTAMP_MS)
    send(make_write_request(({"__name__": "stale"}, [stale])))

    [histogram] = remote_read(TIMESTAMP_MS, TIMESTAMP_MS + 1000)[labels_key(__name__="stale")].histograms
    assert struct.pack("<d", histogram.sum) == struct.pack("<Q", 0x7FF0000000000002)
    assert histogram.count_int == 0
    assert histogram.timestamp == TIMESTAMP_MS


def test_remote_read_resolves_conflicting_samples():
    # Prometheus keeps the sample written first and rejects the others. ClickHouse doesn't keep the order of writes, so it
    # resolves conflicts when reading: identical samples collapse into one, of different histograms the one with the greatest
    # count wins regardless of the order of writes, and a float sample beats a histogram sample.
    def histogram(count):
        return types_pb2.Histogram(
            count_int=count, zero_count_int=0, sum=1, positive_spans=[span(0, 1)], positive_deltas=[count], timestamp=TIMESTAMP_MS
        )

    for request in [
        make_write_request(({"__name__": "identical"}, [histogram(3)]), ({"__name__": "smaller_first"}, [histogram(3)]),
                           ({"__name__": "greater_first"}, [histogram(5)]), ({"__name__": "float_and_histogram"}, [histogram(3)])),
        make_write_request(({"__name__": "identical"}, [histogram(3)]), ({"__name__": "smaller_first"}, [histogram(5)]),
                           ({"__name__": "greater_first"}, [histogram(3)]), ({"__name__": "float_and_histogram"}, [], {TIMESTAMP_MS: 7.0})),
    ]:
        send(request)

    series = remote_read(TIMESTAMP_MS, TIMESTAMP_MS + 1000)
    assert list(series[labels_key(__name__="identical")].histograms) == [histogram(3)]
    assert list(series[labels_key(__name__="smaller_first")].histograms) == [histogram(5)]
    assert list(series[labels_key(__name__="greater_first")].histograms) == [histogram(5)]

    float_and_histogram = series[labels_key(__name__="float_and_histogram")]
    assert [(sample.timestamp, sample.value) for sample in float_and_histogram.samples] == [(TIMESTAMP_MS, 7.0)]
    assert len(float_and_histogram.histograms) == 0
