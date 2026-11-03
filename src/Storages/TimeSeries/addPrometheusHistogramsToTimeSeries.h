#pragma once

#include "config.h"
#if USE_PROMETHEUS_PROTOBUFS

#include <base/types.h>
#include <prompb/types.pb.h>


namespace DB
{
class IColumn;

/// Adds the histogram samples of one time series read by remote read to `time_series` as `prompb.Histogram`.
/// `histograms_column` is `timeSeriesGroupArrayHistograms(histograms)` over `timeSeriesSelector` grouped by time series (an array of
/// histogram tuples sorted by timestamp and deduplicated by timestamp, see TimeSeriesHistogramsColumns), `row` is the time series.
/// Its float samples must already be in `time_series`, sorted by timestamp.
///
/// A histogram keeps its original representation: an integer histogram with `count_int`, `zero_count_int` and the bucket deltas,
/// a float one with the float fields. Counter reset hints can be cleared by merging. The pairs of a side become the spans and the
/// buckets, adjacent indexes making one span. Prometheus keeps the
/// first of conflicting samples at one timestamp, which isn't known here, so a float sample beats a histogram sample at the same
/// timestamp (as in Prometheus within one request); histogram duplicates were resolved by the aggregate (the greatest count wins).
void addPrometheusHistogramsToTimeSeries(
    const IColumn & histograms_column, size_t row, UInt32 timestamp_scale, prometheus::TimeSeries & time_series);

}

#endif
