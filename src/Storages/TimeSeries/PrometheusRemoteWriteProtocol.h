#pragma once

#include "config.h"
#if USE_PROMETHEUS_PROTOBUFS

#include <Common/Logger_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <base/types.h>
#include <Storages/IStorage_fwd.h>
#include <prompb/remote.pb.h>


namespace DB
{
class IColumn;
class StorageTimeSeries;

/// Inserts a Prometheus timestamp (milliseconds since the epoch) into a column of the timestamp type of a TimeSeries table
/// (`DateTime64` with the specified scale, or `DateTime` / `UInt32`).
void insertPrometheusTimestamp(Int64 timestamp_ms, UInt32 scale, IColumn & column);

/// Helper class to support the prometheus remote write protocol.
class PrometheusRemoteWriteProtocol : WithMutableContext
{
public:
    PrometheusRemoteWriteProtocol(StoragePtr time_series_storage_, const ContextMutablePtr & context_);
    ~PrometheusRemoteWriteProtocol();

    void write(
        const google::protobuf::RepeatedPtrField<prometheus::TimeSeries> & time_series,
        const google::protobuf::RepeatedPtrField<prometheus::MetricMetadata> & metrics_metadata);

private:
    std::shared_ptr<StorageTimeSeries> time_series_storage;
    LoggerPtr log;
};

}

#endif
