#pragma once

#include "config.h"
#if USE_PROMETHEUS_PROTOBUFS

#include <Common/Logger_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/IStorage_fwd.h>
#include <prompb/remote.pb.h>

namespace io::prometheus::write::v2
{
class Request;
}

namespace DB
{
class StorageTimeSeries;

/// Helper class to support the prometheus remote write protocol.
class PrometheusRemoteWriteProtocol : WithMutableContext
{
public:
    PrometheusRemoteWriteProtocol(StoragePtr time_series_storage_, const ContextMutablePtr & context_);
    ~PrometheusRemoteWriteProtocol();

    void write(
        const google::protobuf::RepeatedPtrField<prometheus::TimeSeries> & time_series,
        const google::protobuf::RepeatedPtrField<prometheus::MetricMetadata> & metrics_metadata);
    void write(const io::prometheus::write::v2::Request & request);

private:
    std::shared_ptr<StorageTimeSeries> time_series_storage;
    LoggerPtr log;
};

}

#endif
