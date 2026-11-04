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


# Native histograms over the remote-write and remote-read protocols: the requests are hand-crafted, so every corner of the protocol
# can be exercised, including what a real Prometheus never sends. See test_native_histograms_prometheus.py for a real sender.
# A TimeSeries table of the current version stores the histograms in the `histograms` column of its samples table, in the same
# rows as the float samples (one row per series and bucket); the stored rows are checked through the inner tables, so the checks
# of the writes don't depend on the outer read path. Remote read returns the stored histograms as Prometheus messages (the tests
# at the end of the file).

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
)

# The stale marker: a NaN with a specific payload, sent by Prometheus in `sum` when a series goes stale.
STALE_NAN = struct.unpack("<d", struct.pack("<Q", 0x7FF0000000000002))[0]

TIMESTAMP_MS = 1724112000000  # 2024-08-20 00:00:00 UTC

# The stored shape of a histogram sample: the elements of the `histograms` tuple after `timestamp` (see TimeSeriesHistogramsColumns).
HISTOGRAMS_QUERY = """
    SELECT toUnixTimestamp64Milli(h.timestamp), h.flags, h.counter_reset_hint, h.schema, h.zero_threshold, h.sum,
           h.count_int, h.zero_count_int, h.positive_int, h.negative_int,
           h.count_float, h.zero_count_float, h.positive_float, h.negative_float, h.custom_values
    FROM timeSeriesSamples(prometheus) ARRAY JOIN histograms AS h
    ORDER BY h.timestamp
    FORMAT TSV
"""

# The rows of the samples table joined with the series: the number of samples of each kind in the row.
ROWS_QUERY = """
    SELECT t.metric_name, length(s.samples), length(s.histograms)
    FROM timeSeriesSamples(prometheus) AS s JOIN timeSeriesTags(prometheus) AS t ON s.id = t.id
    ORDER BY t.metric_name
    FORMAT TSV
"""

TIME_RANGES_QUERY = """
    SELECT t.metric_name, toUnixTimestamp64Milli(min(r.min_time)), toUnixTimestamp64Milli(max(r.max_time))
    FROM timeSeriesTimeRanges(prometheus) AS r JOIN timeSeriesTags(prometheus) AS t ON r.id = t.id
    GROUP BY t.metric_name
    ORDER BY t.metric_name
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


def read_histograms():
    return node.query(HISTOGRAMS_QUERY)


def count_histograms():
    return int(node.query("SELECT sum(length(histograms)) FROM timeSeriesSamples(prometheus)"))


def count_samples():
    return int(node.query("SELECT sum(length(samples)) FROM timeSeriesSamples(prometheus)"))


def get_event(name):
    return int(node.query(f"SELECT sum(value) FROM system.events WHERE event = '{name}'"))


def get_events():
    return {name: get_event(name) for name in ("PrometheusRemoteWriteHistograms", "PrometheusRemoteWriteDroppedHistograms")}


def check_histogram_round_trip(histogram, expected_row):
    """Sends one histogram sample of one time series and checks everything it must leave behind: the stored element
    (`expected_row` is its TSV form, see HISTOGRAMS_QUERY), the row of the samples table it is in, the time range of the series,
    and the profile events."""
    events_before = get_events()
    send(make_write_request(({"__name__": "hist", "job": "api"}, [histogram])))

    assert read_histograms() == expected_row + "\n"
    assert node.query(ROWS_QUERY) == "hist\t0\t1\n"
    assert node.query(TIME_RANGES_QUERY) == f"hist\t{histogram.timestamp}\t{histogram.timestamp}\n"

    events_after = get_events()
    assert events_after["PrometheusRemoteWriteHistograms"] == events_before["PrometheusRemoteWriteHistograms"] + 1
    assert events_after["PrometheusRemoteWriteDroppedHistograms"] == events_before["PrometheusRemoteWriteDroppedHistograms"]


def test_integer_histogram():
    # Two positive spans covering the bucket indexes -2, -1 and 1, one negative span covering the index 0 with an empty bucket.
    # The deltas [3, -1, 1] decode to the absolute counts [3, 2, 3]; with the zero bucket they sum up to `count_int`. The spans
    # become pairs of the bucket index and the count, the empty bucket included; the float flavour stays at zeros and empty arrays.
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
        histogram, f"{TIMESTAMP_MS}\t0\t2\t3\t0.001\t12.5\t10\t2\t[(-2,3),(-1,2),(1,3)]\t[(0,0)]\t0\t0\t[]\t[]\t[]"
    )


def test_float_histogram():
    # Float bucket counts are copied as sent into the float flavour (bit 0 of `flags`); the integer flavour stays at its defaults.
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
    check_histogram_round_trip(histogram, f"{TIMESTAMP_MS}\t1\t3\t0\t0\t3.25\t0\t0\t[]\t[]\t5.5\t0.5\t[(0,2.5),(1,2.5)]\t[]\t[]")


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
    check_histogram_round_trip(histogram, f"{TIMESTAMP_MS}\t0\t0\t-53\t0\t4.2\t9\t0\t[(0,2),(1,3),(2,4)]\t[]\t0\t0\t[]\t[]\t[0.1,0.5,1]")


def test_stale_marker_and_unset_count():
    # A stale marker is a histogram whose `sum` is the stale NaN; Prometheus sends nothing else. The NaN payload must survive
    # bit-exactly, that is how staleness is detected. A histogram without a `count` arm is an integer histogram with count 0.
    stale = types_pb2.Histogram(sum=STALE_NAN, timestamp=TIMESTAMP_MS)
    check_histogram_round_trip(stale, f"{TIMESTAMP_MS}\t0\t0\t0\t0\tnan\t0\t0\t[]\t[]\t0\t0\t[]\t[]\t[]")
    assert node.query("SELECT hex(reinterpretAsUInt64(h.sum)) FROM timeSeriesSamples(prometheus) ARRAY JOIN histograms AS h") == "7FF0000000000002\n"


def test_samples_and_histograms_together():
    histogram = types_pb2.Histogram(count_int=1, sum=1, positive_spans=[span(0, 1)], positive_deltas=[1], timestamp=TIMESTAMP_MS + 2000)
    send(
        make_write_request(
            # A series with both kinds of samples within one bucket: one row with both arrays, the time range covers both.
            ({"__name__": "mixed"}, [histogram], {TIMESTAMP_MS: 1.0, TIMESTAMP_MS + 1000: 2.0}),
            # A float-only series in the same request gets an empty `histograms` array.
            ({"__name__": "float_only"}, [], {TIMESTAMP_MS: 3.0}),
            # A histogram-only series gets its tags row, its time range and a row with an empty `samples` array.
            ({"__name__": "hist_only"}, [histogram]),
        )
    )

    assert node.query(ROWS_QUERY) == "float_only\t1\t0\nhist_only\t0\t1\nmixed\t2\t1\n"
    assert node.query(TIME_RANGES_QUERY) == (
        f"float_only\t{TIMESTAMP_MS}\t{TIMESTAMP_MS}\n"
        f"hist_only\t{TIMESTAMP_MS + 2000}\t{TIMESTAMP_MS + 2000}\n"
        f"mixed\t{TIMESTAMP_MS}\t{TIMESTAMP_MS + 2000}\n"
    )
    assert node.query("SELECT count() FROM timeSeriesSamples(prometheus)") == "3\n"
    assert node.query(
        "SELECT toUnixTimestamp64Milli(s.min_time), toUnixTimestamp64Milli(s.max_time) "
        "FROM timeSeriesSamples(prometheus) AS s JOIN timeSeriesTags(prometheus) AS t ON s.id = t.id WHERE t.metric_name = 'mixed'"
    ) == f"{TIMESTAMP_MS}\t{TIMESTAMP_MS + 2000}\n"



def test_async_insert():
    # The block with the `histograms.*` columns goes through the asynchronous insert queue (serialized, merged and flushed)
    # instead of straight into the table.
    histogram = types_pb2.Histogram(count_int=1, sum=1, positive_spans=[span(0, 1)], positive_deltas=[1], timestamp=TIMESTAMP_MS)
    send(make_write_request(({"__name__": "async_hist"}, [histogram], {TIMESTAMP_MS: 1.0})), path="/write?async_insert=1")

    assert count_histograms() == 1
    assert count_samples() == 1



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
    assert count_histograms() == total
    # The histograms of a series are within one bucket: one row per series.
    assert node.query("SELECT count() FROM timeSeriesSamples(prometheus)") == f"{num_series}\n"
    assert node.query("SELECT count() FROM timeSeriesTags(prometheus)") == f"{num_series}\n"
    assert events_after["PrometheusRemoteWriteHistograms"] == events_before["PrometheusRemoteWriteHistograms"] + total


def expect_rejected(histogram, expected_error):
    response = get_response_to_remote_write(
        node.ip_address, 9093, "/write", make_write_request(({"__name__": "bad"}, [histogram]))
    )
    assert response.status_code == requests.codes.bad_request, response.text
    assert expected_error in response.text, response.text
    # The whole request is rejected before anything is written, the tags included.
    for table_function in ("timeSeriesTags", "timeSeriesTimeRanges", "timeSeriesSamples"):
        assert node.query(f"SELECT count() FROM {table_function}(prometheus)") == "0\n"


def test_invalid_histograms_are_rejected():
    # What the conversion to the columns rejects (see makePrometheusHistogramsBlock): things the columns can't represent.
    expect_rejected(
        types_pb2.Histogram(count_int=1, sum=1, positive_spans=[span(0, 2)], positive_deltas=[1, -2], timestamp=TIMESTAMP_MS),
        "positive side: bucket #1 has a negative count -1 after decoding the deltas",
    )
    expect_rejected(
        types_pb2.Histogram(count_int=1, sum=1, negative_spans=[span(0, 2)], negative_deltas=[(1 << 63) - 1, 1], timestamp=TIMESTAMP_MS),
        "negative side: the bucket counts overflow Int64 while decoding the deltas",
    )
    expect_rejected(
        types_pb2.Histogram(count_int=1, sum=1, positive_spans=[span(0, 2)], positive_deltas=[1], timestamp=TIMESTAMP_MS),
        "positive side: the spans need 2 buckets, but 1 buckets are given",
    )
    expect_rejected(
        types_pb2.Histogram(
            count_int=2, sum=1, positive_spans=[span(2147483647, 1), span(1, 1)], positive_deltas=[1, 0], timestamp=TIMESTAMP_MS
        ),
        "positive side: the bucket indexes overflow Int32",
    )
    # Values which would wrap into valid ones if they were cast to the column types without a check: -309 into -53, 260 into 4.
    expect_rejected(types_pb2.Histogram(sum=1, schema=-309, timestamp=TIMESTAMP_MS), "schema -309 is out of the range -128..127")
    expect_rejected(types_pb2.Histogram(sum=1, schema=300, timestamp=TIMESTAMP_MS), "schema 300 is out of the range -128..127")
    expect_rejected(types_pb2.Histogram(sum=1, reset_hint=260, timestamp=TIMESTAMP_MS), "reset_hint 260 is out of the range 0..255")

    # A count above Int64 would wrap into a negative one.
    expect_rejected(
        types_pb2.Histogram(count_int=1 << 63, sum=1, positive_spans=[span(0, 1)], positive_deltas=[1], timestamp=TIMESTAMP_MS),
        "count_int 9223372036854775808 is out of the range",
    )

    # The validation of the sink is covered by 05459_timeseries_histograms_validation (the same rules apply to INSERT queries);
    # one case checks that its error reaches the response of the protocol.
    expect_rejected(types_pb2.Histogram(sum=1, schema=9, timestamp=TIMESTAMP_MS), "schema 9 is invalid")


def test_max_buckets_setting():
    # The limit itself is covered by 05460_timeseries_histograms_max_buckets; here the rejection reaches the response of the protocol.
    node.query("DROP TABLE prometheus SYNC")
    node.query("CREATE TABLE prometheus ENGINE=TimeSeries SETTINGS histograms_max_buckets = 2")
    expect_rejected(
        types_pb2.Histogram(count_int=3, sum=1, positive_spans=[span(0, 3)], positive_deltas=[1, 0, 0], timestamp=TIMESTAMP_MS),
        "3 buckets exceed the limit of 2 buckets per histogram",
    )


def test_table_of_an_older_version_drops_histograms():
    # A table of a version without the `histograms` column (see TimeSeriesVersion.h) can't store them. The histograms of a request
    # are dropped with a warning, the float samples of the same request are written and the request succeeds: rejecting it
    # would lose the samples too.
    node.query("DROP TABLE prometheus SYNC")
    node.query("CREATE TABLE prometheus ENGINE=TimeSeries SETTINGS version = 11")
    histogram = types_pb2.Histogram(count_int=1, sum=1, positive_spans=[span(0, 1)], positive_deltas=[1], timestamp=TIMESTAMP_MS)

    events_before = get_events()
    send(make_write_request(({"__name__": "old_table"}, [histogram], {TIMESTAMP_MS: 1.0})))
    events_after = get_events()

    assert count_samples() == 1
    assert events_after["PrometheusRemoteWriteHistograms"] == events_before["PrometheusRemoteWriteHistograms"] + 1
    assert events_after["PrometheusRemoteWriteDroppedHistograms"] == events_before["PrometheusRemoteWriteDroppedHistograms"] + 1
    # The message ends with the version which introduced the column; it isn't pinned here because it changes with rebases.
    assert node.contains_in_log("Dropped 1 native histogram samples: the table stores no histograms because its version 11 is older than ")


def test_table_of_an_older_version_writes_no_phantom_series():
    # A series of the request with histograms only has nothing left to store once they are dropped: it must not become a
    # row of the tags table without any sample, visible through timeSeriesTags and the labels API. The other series of the
    # request are written as usual.
    node.query("DROP TABLE prometheus SYNC")
    node.query("CREATE TABLE prometheus ENGINE=TimeSeries SETTINGS version = 11")
    histogram = types_pb2.Histogram(count_int=1, sum=1, positive_spans=[span(0, 1)], positive_deltas=[1], timestamp=TIMESTAMP_MS)

    events_before = get_events()
    send(make_write_request(
        ({"__name__": "histograms_only"}, [histogram]),
        ({"__name__": "both", "job": "api"}, [histogram], {TIMESTAMP_MS: 1.0}),
        ({"__name__": "samples_only", "job": "api"}, [], {TIMESTAMP_MS: 2.0}),
    ))
    events_after = get_events()

    assert node.query("SELECT metric_name, tags['job'], length(samples) FROM prometheus ORDER BY metric_name") == "both\tapi\t1\nsamples_only\tapi\t1\n"
    assert node.query("SELECT metric_name FROM timeSeriesTags(prometheus) ORDER BY metric_name") == "both\nsamples_only\n"
    assert events_after["PrometheusRemoteWriteDroppedHistograms"] == events_before["PrometheusRemoteWriteDroppedHistograms"] + 2


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
    # resolves conflicts when reading, the same way whatever the order of the writes (the rule of TimeSeriesDuplicateTimestampRule):
    # identical samples collapse into one, of different histograms the one with the greatest count wins, with equal counts the one
    # with the greatest sum, and a float sample beats a histogram sample.
    def histogram(count, total=1):
        return types_pb2.Histogram(
            count_int=count, zero_count_int=0, sum=total, positive_spans=[span(0, 1)], positive_deltas=[count], timestamp=TIMESTAMP_MS
        )

    for request in [
        make_write_request(({"__name__": "identical"}, [histogram(3)]), ({"__name__": "smaller_first"}, [histogram(3)]),
                           ({"__name__": "greater_first"}, [histogram(5)]), ({"__name__": "float_and_histogram"}, [histogram(3)]),
                           ({"__name__": "smaller_sum_first"}, [histogram(3, 1.5)]), ({"__name__": "greater_sum_first"}, [histogram(3, 2.5)])),
        make_write_request(({"__name__": "identical"}, [histogram(3)]), ({"__name__": "smaller_first"}, [histogram(5)]),
                           ({"__name__": "greater_first"}, [histogram(3)]), ({"__name__": "float_and_histogram"}, [], {TIMESTAMP_MS: 7.0}),
                           ({"__name__": "smaller_sum_first"}, [histogram(3, 2.5)]), ({"__name__": "greater_sum_first"}, [histogram(3, 1.5)])),
    ]:
        send(request)

    series = remote_read(TIMESTAMP_MS, TIMESTAMP_MS + 1000)
    assert list(series[labels_key(__name__="identical")].histograms) == [histogram(3)]
    assert list(series[labels_key(__name__="smaller_first")].histograms) == [histogram(5)]
    assert list(series[labels_key(__name__="greater_first")].histograms) == [histogram(5)]
    assert list(series[labels_key(__name__="smaller_sum_first")].histograms) == [histogram(3, 2.5)]
    assert list(series[labels_key(__name__="greater_sum_first")].histograms) == [histogram(3, 2.5)]

    float_and_histogram = series[labels_key(__name__="float_and_histogram")]
    assert [(sample.timestamp, sample.value) for sample in float_and_histogram.samples] == [(TIMESTAMP_MS, 7.0)]
    assert len(float_and_histogram.histograms) == 0
