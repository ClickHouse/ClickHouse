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
# can be exercised, including what a real Prometheus never sends. See test_write_read.py for a real sender.
# A TimeSeries table of the current version stores the histograms in the `histograms` column of its samples table, in the same
# rows as the float samples (one row per series and bucket); the stored rows are checked through the inner tables, so the checks
# of the writes don't depend on the outer read path. Remote read returns the stored histograms as Prometheus messages. How the sink
# lays the samples out in rows and time ranges is covered by the stateless tests (05458_timeseries_histograms_insert).

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
    SELECT toUnixTimestamp64Milli(h.timestamp), h.is_float, h.counter_reset_hint, h.schema, h.zero_threshold, h.sum,
           h.count_int, h.zero_count_int, h.positive_int, h.negative_int,
           h.count_float, h.zero_count_float, h.positive_float, h.negative_float, h.custom_values
    FROM timeSeriesSamples(prometheus) ARRAY JOIN histograms AS h
    ORDER BY h.timestamp
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


def count_samples():
    return int(node.query("SELECT sum(length(samples)) FROM timeSeriesSamples(prometheus)"))


def get_event(name):
    return int(node.query(f"SELECT sum(value) FROM system.events WHERE event = '{name}'"))


def get_events():
    return {name: get_event(name) for name in ("PrometheusRemoteWriteHistograms", "PrometheusRemoteWriteDroppedHistograms")}


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
    # A negative offset of a span after the first one is rejected even when the span is empty and the indexes still increase,
    # as Prometheus does; a span with buckets would make the indexes go backwards.
    expect_rejected(
        types_pb2.Histogram(
            count_int=2, sum=1, positive_spans=[span(0, 1), span(-1, 0), span(1, 1)], positive_deltas=[1, 0], timestamp=TIMESTAMP_MS
        ),
        "positive side: span #1 has a negative offset -1",
    )
    # Values which would wrap into valid ones if they were cast to the column types without a check: -309 into -53, 260 into 4.
    expect_rejected(types_pb2.Histogram(sum=1, schema=-309, timestamp=TIMESTAMP_MS), "schema -309 is out of the range -128..127")
    expect_rejected(types_pb2.Histogram(sum=1, schema=300, timestamp=TIMESTAMP_MS), "schema 300 is out of the range -128..127")
    expect_rejected(types_pb2.Histogram(sum=1, reset_hint=260, timestamp=TIMESTAMP_MS), "reset_hint 260 is out of the range -128..127")

    # A count above Int64 would wrap into a negative one.
    expect_rejected(
        types_pb2.Histogram(count_int=1 << 63, sum=1, positive_spans=[span(0, 1)], positive_deltas=[1], timestamp=TIMESTAMP_MS),
        "count_int 9223372036854775808 is out of the range",
    )

    # The validation of the sink is covered by 05459_timeseries_histograms_validation (the same rules apply to INSERT queries);
    # one case checks that its error reaches the response of the protocol.
    expect_rejected(types_pb2.Histogram(sum=1, schema=9, timestamp=TIMESTAMP_MS), "schema 9 is invalid")


def test_table_of_an_older_version_drops_histograms():
    # A table of a version without the `histograms` column (see TimeSeriesVersion.h) can't store them. The histograms of a request
    # are dropped and counted, the float samples of the same request are written and the request succeeds: rejecting it would lose
    # the samples too. A series of the request with histograms only has nothing left to store once they are dropped: it must not
    # become a row of the tags table without any sample, visible through timeSeriesTags and the labels API.
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

    assert count_samples() == 2
    assert node.query("SELECT metric_name, tags['job'], length(samples) FROM prometheus ORDER BY metric_name") == "both\tapi\t1\nsamples_only\tapi\t1\n"
    assert node.query("SELECT metric_name FROM timeSeriesTags(prometheus) ORDER BY metric_name") == "both\nsamples_only\n"
    assert events_after["PrometheusRemoteWriteHistograms"] == events_before["PrometheusRemoteWriteHistograms"] + 2
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
    # The counter reset hints are left unknown: the first histogram of a series is returned as unknown whatever was stored
    # (see test_remote_read_returns_stored_resets_as_unknown).
    # Custom buckets (NHCB) have `schema = -53` and the upper bounds in `custom_values`. Prometheus 3.7 and later don't send them
    # over remote-write 1.0 (earlier versions send them without the bounds), but other senders (e.g. the OpenTelemetry Collector) do.
    integer_histogram = types_pb2.Histogram(
        count_int=15, zero_count_int=2, sum=12.5, schema=3, zero_threshold=0.001,
        positive_spans=[span(-2, 2), span(1, 1)], positive_deltas=[3, -1, 1], negative_spans=[span(0, 1)], negative_deltas=[5],
        timestamp=TIMESTAMP_MS,
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

    # The stored form of `hist`, so that a mix-up made the same way by the write and by the read can't go unnoticed: the spans become
    # pairs of the bucket index and the count, the deltas of an integer histogram absolute counts ([3, -1, 1] are 3, 2, 3), and
    # each flavour and side has its own element. The integer histogram after the float one gets an unknown hint.
    assert node.query(
        "SELECT h.is_float, h.counter_reset_hint, h.schema, h.positive_int, h.negative_int, h.positive_float, h.negative_float, h.custom_values "
        "FROM timeSeriesSamples(prometheus) AS s ARRAY JOIN s.histograms AS h JOIN timeSeriesTags(prometheus) AS t ON s.id = t.id "
        "WHERE t.metric_name = 'hist' ORDER BY h.timestamp"
    ) == (
        "false\tunknown\t3\t[(-2,3),(-1,2),(1,3)]\t[(0,5)]\t[]\t[]\t[]\n"
        "true\tgauge\t0\t[]\t[]\t[(0,2.5),(1,2.5)]\t[(1,0.25)]\t[]\n"
        "false\tunknown\t-53\t[(0,2),(1,3),(2,4)]\t[]\t[]\t[]\t[0.1,0.5,1]\n"
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


def test_stale_marker_is_stored_and_returned_bit_exactly():
    # A stale marker is a histogram whose `sum` is the stale NaN; Prometheus sends nothing else. The NaN payload must survive
    # bit-exactly, that is how staleness is detected. A histogram without a `count` arm is an integer histogram with count 0.
    stale = types_pb2.Histogram(sum=STALE_NAN, timestamp=TIMESTAMP_MS)
    send(make_write_request(({"__name__": "stale"}, [stale])))

    assert read_histograms() == f"{TIMESTAMP_MS}\tfalse\tunknown\t0\t0\tnan\t0\t0\t[]\t[]\t0\t0\t[]\t[]\t[]\n"
    assert node.query("SELECT hex(reinterpretAsUInt64(h.sum)) FROM timeSeriesSamples(prometheus) ARRAY JOIN histograms AS h") == "7FF0000000000002\n"

    [histogram] = remote_read(TIMESTAMP_MS, TIMESTAMP_MS + 1000)[labels_key(__name__="stale")].histograms
    assert struct.pack("<d", histogram.sum) == struct.pack("<Q", 0x7FF0000000000002)
    assert histogram.count_int == 0
    assert histogram.timestamp == TIMESTAMP_MS


def test_remote_read_resolves_conflicting_samples():
    # Prometheus keeps the sample written first and rejects the others. ClickHouse doesn't keep the order of writes, so it
    # resolves conflicts when reading, the same way whatever the order of the writes: of two histograms the one with the greatest
    # count wins (the rule of TimeSeriesDuplicateTimestampRule, covered by the stateless tests), and a float sample beats a
    # histogram sample, which only the protocol shows: both are stored, and the response keeps the float one.
    def histogram(count):
        return types_pb2.Histogram(
            count_int=count, zero_count_int=0, sum=1, positive_spans=[span(0, 1)], positive_deltas=[count], timestamp=TIMESTAMP_MS
        )

    for request in [
        make_write_request(({"__name__": "smaller_first"}, [histogram(3)]), ({"__name__": "float_and_histogram"}, [histogram(3)])),
        make_write_request(({"__name__": "smaller_first"}, [histogram(5)]), ({"__name__": "float_and_histogram"}, [], {TIMESTAMP_MS: 7.0})),
    ]:
        send(request)

    series = remote_read(TIMESTAMP_MS, TIMESTAMP_MS + 1000)
    assert list(series[labels_key(__name__="smaller_first")].histograms) == [histogram(5)]

    float_and_histogram = series[labels_key(__name__="float_and_histogram")]
    assert [(sample.timestamp, sample.value) for sample in float_and_histogram.samples] == [(TIMESTAMP_MS, 7.0)]
    assert len(float_and_histogram.histograms) == 0


def test_remote_read_clears_hints_after_float_collisions():
    # Drop the first histogram, two consecutive histograms, and one in the middle. Only the first surviving counter hint
    # after each gap becomes unknown; the following hint and the gauge markers must be preserved. The counts increase after
    # the dropped reset, so an unknown hint can only come from the clearing (a stored reset is returned as unknown anyway).
    counts = [10, 11, 1, 2, 3, 4, 5, 6]
    timestamps = [TIMESTAMP_MS + i * 1000 for i in range(len(counts))]
    floats = {timestamps[i]: STALE_NAN for i in (0, 2, 3, 5)}
    requests = []
    for name, hint in (("counter", types_pb2.Histogram.UNKNOWN), ("gauge", types_pb2.Histogram.GAUGE)):
        histograms = [
            types_pb2.Histogram(
                timestamp=timestamp, count_int=count, zero_count_int=count, sum=float(count), reset_hint=hint
            )
            for timestamp, count in zip(timestamps, counts)
        ]
        requests.append(({"__name__": name}, histograms, floats))
    send(make_write_request(*requests))

    series = remote_read(timestamps[0], timestamps[-1])
    for name in ("counter", "gauge"):
        result = series[labels_key(__name__=name)]
        assert [(h.timestamp, h.count_int) for h in result.histograms] == [
            (timestamps[i], counts[i]) for i in (1, 4, 6, 7)
        ]
        assert [sample.timestamp for sample in result.samples] == list(floats)

    assert [h.reset_hint for h in series[labels_key(__name__="counter")].histograms] == [
        types_pb2.Histogram.UNKNOWN,
        types_pb2.Histogram.UNKNOWN,
        types_pb2.Histogram.UNKNOWN,
        types_pb2.Histogram.NO,
    ]
    assert [h.reset_hint for h in series[labels_key(__name__="gauge")].histograms] == [types_pb2.Histogram.GAUGE] * 4


def test_remote_read_returns_stored_resets_as_unknown():
    # Prometheus returns `GAUGE`, `NO` or `UNKNOWN` over remote read, never `YES`: a reset, explicit or detected, starts a new chunk
    # whose first sample is returned with an unknown hint, and PromQL trusts `YES` without checking the data. A stored reset is
    # returned as unknown the same way. A sender's `YES` the data doesn't show (the counts increase) isn't stored at all: the sink
    # detects every hint but the first of a row from the data, so it is returned as `NO`. The first histogram of a series is
    # returned as unknown, as it is stored: the predecessor of the first sample of a row is in another row, and Prometheus never
    # trusts the hint of the first sample of a chunk either. The other hints are returned as stored.
    timestamps = [TIMESTAMP_MS + i * 1000 for i in range(3)]

    def histogram(timestamp, count, hint):
        return types_pb2.Histogram(timestamp=timestamp, count_int=count, zero_count_int=count, sum=float(count), reset_hint=hint)

    explicit_hints = [types_pb2.Histogram.NO, types_pb2.Histogram.YES, types_pb2.Histogram.NO]
    explicit = [histogram(t, count, hint) for t, count, hint in zip(timestamps, [1, 2, 3], explicit_hints)]
    detected = [histogram(t, count, types_pb2.Histogram.UNKNOWN) for t, count in zip(timestamps, [5, 2, 3])]
    send(make_write_request(({"__name__": "explicit"}, explicit), ({"__name__": "detected"}, detected)))

    # Stored: the first sample gets an unknown hint, the decrease of the count is detected as a reset, the increases as no reset,
    # the sender's reset included.
    assert node.query(
        "SELECT metric_name, histograms.counter_reset_hint FROM prometheus WHERE metric_name IN ('explicit', 'detected') ORDER BY metric_name"
    ) == "detected\t['unknown','reset','not_reset']\nexplicit\t['unknown','not_reset','not_reset']\n"

    series = remote_read(timestamps[0], timestamps[-1])
    assert [h.reset_hint for h in series[labels_key(__name__="detected")].histograms] == [
        types_pb2.Histogram.UNKNOWN,
        types_pb2.Histogram.UNKNOWN,
        types_pb2.Histogram.NO,
    ]
    assert [h.reset_hint for h in series[labels_key(__name__="explicit")].histograms] == [
        types_pb2.Histogram.UNKNOWN,
        types_pb2.Histogram.NO,
        types_pb2.Histogram.NO,
    ]


def test_fields_of_the_other_flavour_are_ignored():
    # The `count` arm decides the flavour, as in Prometheus's receiver (`IsFloatHistogram`): the fields of the other flavour are
    # ignored even when they are set, the other arm of `zero_count` and the other kind of bucket values included.
    integer_histogram = types_pb2.Histogram(
        count_int=3, zero_count_float=0.5, sum=1, positive_spans=[span(0, 1)], positive_deltas=[3], positive_counts=[2.5], timestamp=TIMESTAMP_MS
    )
    float_histogram = types_pb2.Histogram(
        count_float=2.5, zero_count_int=1, sum=2, positive_spans=[span(0, 1)], positive_counts=[2.5], positive_deltas=[7], timestamp=TIMESTAMP_MS + 1000
    )
    send(make_write_request(({"__name__": "hist"}, [integer_histogram, float_histogram])))

    assert read_histograms() == (
        f"{TIMESTAMP_MS}\tfalse\tunknown\t0\t0\t1\t3\t0\t[(0,3)]\t[]\t0\t0\t[]\t[]\t[]\n"
        f"{TIMESTAMP_MS + 1000}\ttrue\tunknown\t0\t0\t2\t0\t0\t[]\t[]\t2.5\t0\t[(0,2.5)]\t[]\t[]\n"
    )

