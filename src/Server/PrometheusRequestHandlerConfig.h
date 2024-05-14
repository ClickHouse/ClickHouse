#pragma once

#include <base/types.h>


namespace Poco::Util { class AbstractConfiguration; }

namespace DB
{
class HTTPServerRequest;

/// Configuration of a Prometheus protocol handler after it's parsed from a configuration file.
struct PrometheusRequestHandlerConfig
{
    /// Handler for exposing ClickHouse metrics:
    /// <prometheus>
    ///     <port>9363</port>  <!-- port is not parsed in PrometheusRequestHandlerConfig -->
    ///     <endpoint>/metrics</endpoint>
    ///     <metrics>true</metrics>
    ///     <events>true</events>
    ///     <asynchronous_metrics>true</asynchronous_metrics>
    ///     <errors>true</errors>
    /// </prometheus>
    struct Metrics
    {
        String endpoint;
        bool send_metrics = false;
        bool send_asynchronous_metrics = false;
        bool send_events = false;
        bool send_errors = false;
    };

    std::optional<Metrics> metrics;

    size_t keep_alive_timeout;

    PrometheusRequestHandlerConfig(const Poco::Util::AbstractConfiguration & config, const String & config_prefix);
    bool filterRequest(const HTTPServerRequest & request) const;
};

using PrometheusRequestHandlerConfigPtr = std::shared_ptr<const PrometheusRequestHandlerConfig>;

}
