#pragma once

#include <Core/QualifiedTableName.h>


namespace Poco::Util { class AbstractConfiguration; }

namespace DB
{
class HTTPServerRequest;

/// Configuration of Prometheus protocol handlers after they're parsed from a configuration file.
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

    struct EndpointAndTableName
    {
        String endpoint;
        QualifiedTableName table_name;
    };

    /// Handler for Prometheus remote-write protocol:
    /// <prometheus>
    ///     <port>9363</port>  <!-- port is not parsed in PrometheusRequestHandlerConfig -->
    ///     <remote_write>
    ///         <endpoint>/write</endpoint>
    ///         <table>mydb.prometheus</table>
    ///     </remote_write>
    /// </prometheus>
    std::optional<EndpointAndTableName> remote_write;
    std::optional<EndpointAndTableName> remote_read;

    size_t keep_alive_timeout;
    bool is_stacktrace_enabled = true;

    /// Use endpoints in the config to find out which handler should be used.
    bool detect_handler_by_endpoint = true;

    PrometheusRequestHandlerConfig(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, bool detect_handler_by_endpoint_);
    bool filterRequest(const HTTPServerRequest & request) const;
};

using PrometheusRequestHandlerConfigPtr = std::shared_ptr<const PrometheusRequestHandlerConfig>;

}
