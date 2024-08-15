#pragma once

#include "config.h"
#if USE_PROMETHEUS_PROTOBUFS

#include <Interpreters/Context_fwd.h>
#include <Storages/IStorage_fwd.h>
#include <prompb/remote.pb.h>


namespace DB
{
class StorageTimeSeries;

/// Helper class to support the prometheus remote read protocol.
class PrometheusRemoteReadProtocol : public WithContext
{
public:
    PrometheusRemoteReadProtocol(ConstStoragePtr time_series_storage_, const ContextPtr & context_);
    ~PrometheusRemoteReadProtocol();

    /// Reads time series to send to client by remote read protocol.
    void readTimeSeries(google::protobuf::RepeatedPtrField<prometheus::TimeSeries> & out_time_series,
                        Int64 start_timestamp_ms,
                        Int64 end_timestamp_ms,
                        const google::protobuf::RepeatedPtrField<prometheus::LabelMatcher> & label_matcher,
                        const prometheus::ReadHints & read_hints);

private:
    std::shared_ptr<const StorageTimeSeries> time_series_storage;
    Poco::LoggerPtr log;
};

}

#endif
