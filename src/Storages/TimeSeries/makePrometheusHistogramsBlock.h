#pragma once

#include "config.h"
#if USE_PROMETHEUS_PROTOBUFS

#include <Core/Block.h>
#include <prompb/types.pb.h>

#include <span>


namespace DB
{
class StorageTimeSeries;
struct StorageInMemoryMetadata;

/// Builds the `histograms.*` outer columns of a TimeSeries table (see TimeSeriesHistogramsColumns) from the histogram samples
/// of the time series of a remote-write request selected for writing: one row per time series, then `num_metadata_rows` rows
/// without histograms (see PrometheusRemoteWriteProtocol). The table must store histogram samples, see `dropPrometheusHistograms`
/// for the others. Returns an empty block if the time series have no histogram samples.
///
/// Stores every histogram as sent: the `count` arm decides the flavour (Prometheus's `IsFloatHistogram`), the integer bucket deltas
/// become absolute `Int64` counts, the float counts are copied, and the spans of a side become pairs of the bucket index and the
/// count, empty buckets included. Only what the columns can't represent is rejected here with INCORRECT_DATA: spans not matching
/// their buckets, values out of range. Prometheus's other `Histogram.Validate` checks are in the sink (see `validateTimeSeriesHistograms`).
Block makePrometheusHistogramsBlock(
    std::span<const prometheus::TimeSeries * const> time_series,
    size_t num_metadata_rows,
    const StorageInMemoryMetadata & metadata);

/// Drops the histogram samples of a remote-write request to a table storing no histograms (a table of a version older than
/// `TimeSeriesVersion::MIN_WITH_NATIVE_HISTOGRAMS`): counts them in `PrometheusRemoteWriteDroppedHistograms` and logs
/// a warning. Rejecting the request would make the sender drop its float samples too, so the rest of the request is written.
void dropPrometheusHistograms(
    const google::protobuf::RepeatedPtrField<prometheus::TimeSeries> & time_series,
    const StorageTimeSeries & time_series_storage);

}

#endif
