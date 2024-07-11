#pragma once


namespace DB
{

/// Configuration of a Prometheus protocol handler after it's parsed from a configuration file.
struct PrometheusRequestHandlerConfig
{
    enum class Type
    {
        /// Exposes ClickHouse metrics for scraping by Prometheus.
        ExposeMetrics,
    };

    Type type = Type::ExposeMetrics;

    /// Settings for type ExposeMetrics:
    bool expose_metrics = false;
    bool expose_asynchronous_metrics = false;
    bool expose_events = false;
    bool expose_errors = false;

    size_t keep_alive_timeout = 0;
    bool is_stacktrace_enabled = true;
};

}
