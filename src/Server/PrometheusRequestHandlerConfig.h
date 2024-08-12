#pragma once

#include <Core/QualifiedTableName.h>


namespace DB
{

/// Configuration of a Prometheus protocol handler after it's parsed from a configuration file.
struct PrometheusRequestHandlerConfig
{
    enum class Type
    {
        /// Exposes ClickHouse metrics for scraping by Prometheus.
        ExposeMetrics,

        /// Handles Prometheus remote-write protocol.
        RemoteWrite,

        /// Handles Prometheus remote-read protocol.
        RemoteRead,
    };

    Type type = Type::ExposeMetrics;

    /// Settings for type ExposeMetrics:
    bool expose_metrics = false;
    bool expose_asynchronous_metrics = false;
    bool expose_events = false;
    bool expose_errors = false;

    /// Settings for types RemoteWrite, RemoteRead:
    QualifiedTableName time_series_table_name;

    size_t keep_alive_timeout = 0;
    bool is_stacktrace_enabled = true;
};

}
