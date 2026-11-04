#pragma once

#include <base/types.h>


namespace DB
{
class TimeSeriesHistogramsColumnsView;

/// The rules deciding the counter reset hint of a histogram sample from its predecessor in the series, shared by TimeSeriesSink
/// (the samples of one INSERT) and `timeSeriesGroupArrayHistograms` (the samples whose predecessor changes when rows are merged),
/// so that a stored hint means the same wherever it was computed. They follow Prometheus's chunk appender (`appendable` in
/// `tsdb/chunkenc/histogram.go`), which verifies every sample against its predecessor at ingestion; PromQL then trusts a known
/// hint without looking at the data (`DetectReset` in `model/histogram/float_histogram.go`).
class TimeSeriesHistogramCounterResetRule
{
public:
    /// Whether `sum` is Prometheus's stale marker: the NaN with the payload 2 (`StaleNaN` in `model/value/value.go`).
    static bool isStaleMarker(Float64 sum);

    /// The hint of sample `current` of `view` given that sample `previous` is its predecessor in the series. A stale marker gets an
    /// unknown hint (Prometheus returns stale markers with only `sum` set). A gauge marker describes the sample itself and an explicit
    /// reset is kept: the sender knew the predecessor, and a reset can't always be told from the data, e.g. after a schema change.
    /// Any other hint is detected from the data (see `detect`).
    static UInt8 hintAfterPredecessor(const TimeSeriesHistogramsColumnsView & view, size_t previous, size_t current);

    /// The hint of sample `current` detected from its data and the data of its predecessor `previous`, whatever hint it was sent with:
    /// a reset when the total count, the zero count or a bucket count decreased, a populated bucket disappeared or the custom bounds
    /// changed; unknown when the samples differ in flavour, schema or zero threshold, or when the predecessor is a gauge or a stale
    /// marker (the comparison is left to the reader, as the chunk appender does); otherwise no reset.
    static UInt8 detect(const TimeSeriesHistogramsColumnsView & view, size_t previous, size_t current);
};

}
