#pragma once

#include "config.h"
#if USE_PROMETHEUS_PROTOBUFS

#include <Common/Logger_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/IStorage_fwd.h>
#include <prompb/remote.pb.h>


namespace DB
{

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
    StoragePtr time_series_storage;
    LoggerPtr log;
};

}

#endif
