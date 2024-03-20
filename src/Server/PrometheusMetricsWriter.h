#pragma once

#include <string>

#include <Common/AsynchronousMetrics.h>
#include <Common/ProfileEvents.h>
#include <IO/WriteBuffer.h>

#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{

/// Write metrics in Prometheus format
class PrometheusMetricsWriter
{
public:
    PrometheusMetricsWriter(
        const Poco::Util::AbstractConfiguration & config, const std::string & config_name,
        const AsynchronousMetrics & async_metrics_);

    virtual void write(WriteBuffer & wb) const;

    virtual ~PrometheusMetricsWriter() = default;

protected:
    const AsynchronousMetrics & async_metrics;
    const bool send_events;
    const bool send_metrics;
    const bool send_asynchronous_metrics;
    const bool send_errors;
};

class KeeperPrometheusMetricsWriter : public PrometheusMetricsWriter
{
    using PrometheusMetricsWriter::PrometheusMetricsWriter;

    void write(WriteBuffer & wb) const override;
};

using PrometheusMetricsWriterPtr = std::shared_ptr<PrometheusMetricsWriter>;

}
