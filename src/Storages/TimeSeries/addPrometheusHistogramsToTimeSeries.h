#pragma once

#include "config.h"
#if USE_PROMETHEUS_PROTOBUFS

#include <base/types.h>
#include <prompb/types.pb.h>


namespace DB
{
class IColumn;

/// Adds the histogram samples of one time series read by remote read to `time_series` as `prompb.Histogram`.
/// `histograms_column` is `timeSeriesGroupArray(histograms)` over `timeSeriesSelector` grouped by time series (an array of
/// histogram tuples sorted and deduplicated by timestamp, see TimeSeriesHistogramsColumns), `row` is the time series.
/// Its float samples must already be in `time_series`, sorted by timestamp.
///
/// A histogram keeps its representation: an integer one with `count_int`, `zero_count_int` and bucket deltas, a float one with the
/// float fields; the pairs of a side become spans and buckets, adjacent indexes making one span. The reset hint is `GAUGE`, `NO` or
/// `UNKNOWN`, never `YES`, as Prometheus returns it (see `toPrometheusResetHint`); a counter hint is returned as `UNKNOWN` when the
/// predecessor it describes isn't in the response: the first histogram of the series, and the one after a skipped histogram.
/// Prometheus keeps the first of conflicting samples at one timestamp, which isn't known here, so a float sample beats a histogram
/// sample at the same timestamp.
void addPrometheusHistogramsToTimeSeries(
    const IColumn & histograms_column, size_t row, UInt32 timestamp_scale, prometheus::TimeSeries & time_series);

}

#endif
