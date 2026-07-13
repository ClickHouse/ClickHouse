#pragma once

#include <Core/QualifiedTableName.h>
#include <Server/HTTPHandler.h>


namespace DB
{

/// Configuration of a Prometheus protocol handler after it's parsed from a configuration file.
struct PrometheusRequestHandlerConfig
{
    enum class Type
    {
        /// Exposes ClickHouse metrics for scraping by Prometheus.
        Metrics,

        /// Handles all Prometheus "/api/v1" protocols including Query, Write, Read.
        APIv1,

        /// Handles the read-only query and metadata endpoints of the Prometheus HTTP API
        /// (/api/v1/query, /api/v1/query_range, /api/v1/series, /api/v1/labels, /api/v1/label/<name>/values),
        /// i.e. everything under "/api/v1" except remote write and remote read.
        Query,

        /// Handles Prometheus remote-write protocol.
        Write,

        /// Handles Prometheus remote-read protocol.
        Read,
    };

    Type type = Type::Metrics;

    /// Settings for type Metrics:
    bool expose_info = false;
    bool expose_metrics = false;
    bool expose_asynchronous_metrics = false;
    bool expose_events = false;
    bool expose_errors = false;
    bool expose_histograms = false;
    bool expose_dimensional_metrics = false;

    /// Settings for types APIv1, Query, Write, Read:
    QualifiedTableName time_series_table_name;

    size_t keep_alive_timeout = 0;
    bool is_stacktrace_enabled = true;

    HTTPHandlerConnectionConfig connection_config;
};

}
