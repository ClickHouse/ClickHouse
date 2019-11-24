#pragma once

#include <string>

#include <IO/WriteBuffer.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

/// Write metrics in Prometheus format
class PrometheusMetricsWriter
{
public:
    PrometheusMetricsWriter(
        const Poco::Util::AbstractConfiguration & config, const std::string & config_name);

    void write(WriteBuffer & wb) const;

private:
    const bool send_events;
    const bool send_metrics;

    static inline constexpr auto profile_events_prefix = "ClickHouse_ProfileEvents_";
    static inline constexpr auto current_metrics_prefix = "ClickHouse_Metrics_";
};

}
