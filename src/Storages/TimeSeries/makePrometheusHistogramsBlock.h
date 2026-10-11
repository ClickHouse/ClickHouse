#pragma once

#include "config.h"
#if USE_PROMETHEUS_PROTOBUFS

#include <Core/Block.h>
#include <prompb/types.pb.h>


namespace DB
{
class StorageTimeSeries;
struct StorageInMemoryMetadata;

/// Builds the `histograms.*` outer columns of a TimeSeries table (see TimeSeriesHistogramsColumns) from the histogram samples
/// of a remote-write request: one row per time series, then `num_metadata_rows` rows without histograms (see
/// PrometheusRemoteWriteProtocol).
///
/// Returns an empty block if the request has no histogram samples, or if the table has no histograms table: then the histogram
/// samples are dropped with a warning and counted in `PrometheusRemoteWriteDroppedHistograms`.
///
/// Converts as Prometheus's receiver does (`ToIntHistogram`, `ToFloatHistogram`): the `count` arm decides the flavour, integer
/// deltas are decoded to absolute counts, the rest is copied. Only what the columns can't represent is rejected here with
/// INCORRECT_DATA; the checks of Prometheus's `Histogram.Validate` are done in the sink (see `validateTimeSeriesHistograms`).
Block makePrometheusHistogramsBlock(
    const google::protobuf::RepeatedPtrField<prometheus::TimeSeries> & time_series,
    size_t num_metadata_rows,
    const StorageTimeSeries & time_series_storage,
    const StorageInMemoryMetadata & metadata);

}

#endif
