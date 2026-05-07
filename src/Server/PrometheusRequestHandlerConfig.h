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
        ExposeMetrics,

        /// Handles Prometheus remote-write protocol.
        RemoteWrite,

        /// Handles Prometheus remote-read protocol.
        RemoteRead,

        /// Handles Prometheus Query API endpoints (/api/v1/query, /api/v1/query_range, etc.)
        QueryAPI,
    };

    Type type = Type::ExposeMetrics;

    /// Settings for type ExposeMetrics:
    bool expose_info = false;
    bool expose_metrics = false;
    bool expose_asynchronous_metrics = false;
    bool expose_events = false;
    bool expose_errors = false;
    bool expose_histograms = false;
    bool expose_dimensional_metrics = false;

    /// Settings for types RemoteWrite, RemoteRead, QueryAPI when @c enable_table_name_url_routing
    /// is false: the target TimeSeries table is fixed at config-load time. When
    /// @c enable_table_name_url_routing is true this field is ignored and the
    /// `(database, table)` pair comes from the `database` / `table` query parameters or the
    /// `X-ClickHouse-Database` / `X-ClickHouse-Table` HTTP headers (after @c http_path_prefix
    /// is stripped from the URL path).
    QualifiedTableName time_series_table_name;

    /// URL prefix that precedes protocol paths such as @c /write, @c /read, @c /api/v1/query
    /// (only meaningful when @c enable_table_name_url_routing is true). Empty means "no prefix",
    /// which is the case on the dedicated @c prometheus.port listener.
    String http_path_prefix;

    /// When true, the handler ignores @c time_series_table_name and resolves the target
    /// `(database, table)` pair from query parameters / headers (see above). Additionally, the
    /// resolved storage must be a TimeSeries with the per-table setting
    /// @c prometheus_url_routing_enabled left at its default value of 1 (i.e. not explicitly
    /// disabled by the table owner); otherwise the handler returns HTTP 403.
    bool enable_table_name_url_routing = false;

    size_t keep_alive_timeout = 0;
    bool is_stacktrace_enabled = true;

    HTTPHandlerConnectionConfig connection_config;
};

}
