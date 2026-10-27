#pragma once

#include "config.h"
#if USE_PROMETHEUS_PROTOBUFS

#include <base/types.h>
#include <prompb/types.pb.h>


namespace DB
{
class IColumn;

/// Adds the histogram samples of one time series read by remote read to `time_series` as `prompb.Histogram`.
/// `histograms_column` is `groupArrayIf((timestamp, histogram), notEmpty(histogram))` over `timeSeriesSelector` grouped by
/// time series, `row` is the time series. Its float samples must already be in `time_series`.
///
/// Histograms are returned as stored, the inverse of `makePrometheusHistogramsBlock`: `is_float` decides the flavour,
/// integer bucket counts are encoded as deltas, the rest is copied.
///
/// Prometheus keeps the first of conflicting samples at one timestamp, which isn't known here, so: exact duplicates collapse,
/// a float sample beats a histogram sample (as in Prometheus within one request), and of different histograms the one with
/// the greatest count wins, then the greatest in the order of the stored columns.
void addPrometheusHistogramsToTimeSeries(
    const IColumn & histograms_column, size_t row, UInt32 timestamp_scale, prometheus::TimeSeries & time_series);

}

#endif
