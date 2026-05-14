#include <Access/Credentials.h>
#include <Server/PrometheusRequestHandlerFactory.h>

#include <Core/Types_fwd.h>
#include <Server/HTTPHandlerFactory.h>
#include <Server/PrometheusMetricsWriter.h>
#include <Server/PrometheusRequestHandler.h>
#include <Server/PrometheusRequestHandlerConfig.h>

#include <string_view>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int INVALID_CONFIG_PARAMETER;
}

namespace
{
    /// Parses common configuration which is attached to any other configuration. The common configuration looks like this:
    /// <prometheus>
    ///     <enable_stacktrace>true</enable_stacktrace>
    /// </prometheus>
    /// <keep_alive_timeout>30</keep_alive_timeout>
    void parseCommonConfig(const Poco::Util::AbstractConfiguration & config, PrometheusRequestHandlerConfig & res)
    {
        res.is_stacktrace_enabled = config.getBool("prometheus.enable_stacktrace", true);
        res.keep_alive_timeout = config.getUInt("keep_alive_timeout", DEFAULT_HTTP_KEEP_ALIVE_TIMEOUT);
    }

    /// Parses a configuration like this:
    /// <!-- <type>expose_metrics</type> (Implied, not actually parsed) -->
    /// <metrics>true</metrics>
    /// <asynchronous_metrics>true</asynchronous_metrics>
    /// <events>true</events>
    /// <errors>true</errors>
    PrometheusRequestHandlerConfig parseExposeMetricsConfig(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
    {
        PrometheusRequestHandlerConfig res;
        res.type = PrometheusRequestHandlerConfig::Type::ExposeMetrics;
        res.expose_info = config.getBool(config_prefix + ".info", true);
        res.expose_metrics = config.getBool(config_prefix + ".metrics", true);
        res.expose_asynchronous_metrics = config.getBool(config_prefix + ".asynchronous_metrics", true);
        res.expose_events = config.getBool(config_prefix + ".events", true);
        res.expose_errors = config.getBool(config_prefix + ".errors", true);
        res.expose_histograms = config.getBool(config_prefix + ".histograms", true);
        res.expose_dimensional_metrics = config.getBool(config_prefix + ".dimensional_metrics", true);
        parseCommonConfig(config, res);
        return res;
    }

    /// Reads the database and table names of the time series table from the configuration.
    /// If either the database name or the table name isn't set in the configuration then we take it from the URL
    /// query parameters 'database' or 'table'.
    void parseTableNameFromConfig(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, PrometheusRequestHandlerConfig & res)
    {
        res.time_series_table_name.database = config.getString(config_prefix + ".database", "");
        res.time_series_table_name.table = config.getString(config_prefix + ".table", "");

        /// When the table is given as a qualified `database.table` name, we resolve it now and set the database name
        /// so it can't be overridden by URL query parameters.
        if (res.time_series_table_name.database.empty() && !res.time_series_table_name.table.empty())
        {
            if (auto parsed = QualifiedTableName::tryParseFromString(res.time_series_table_name.table); parsed && !parsed->database.empty())
                res.time_series_table_name = *parsed;
        }
    }

    /// Parses a configuration like this:
    /// <!-- <type>remote_write</type> (Implied, not actually parsed) -->
    /// <table>db.time_series_table_name</table>
    PrometheusRequestHandlerConfig parseRemoteWriteConfig(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
    {
        PrometheusRequestHandlerConfig res;
        res.type = PrometheusRequestHandlerConfig::Type::RemoteWrite;
        parseTableNameFromConfig(config, config_prefix, res);
        parseCommonConfig(config, res);
        return res;
    }

    /// Parses a configuration like this:
    /// <!-- <type>remote_read</type> (Implied, not actually parsed) -->
    /// <table>db.time_series_table_name</table>
    PrometheusRequestHandlerConfig parseRemoteReadConfig(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
    {
        PrometheusRequestHandlerConfig res;
        res.type = PrometheusRequestHandlerConfig::Type::RemoteRead;
        parseTableNameFromConfig(config, config_prefix, res);
        parseCommonConfig(config, res);
        if (config.has(config_prefix + ".user"))
        {
            AlwaysAllowCredentials credentials(config.getString(config_prefix + ".user"));
            res.connection_config.credentials.emplace(credentials);
        }
        return res;
    }

    /// Parses a configuration like this:
    /// <!-- <type>query_api</type> (Implied, not actually parsed) -->
    /// <table>db.time_series_table_name</table>
    PrometheusRequestHandlerConfig parseQueryAPIConfig(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
    {
        PrometheusRequestHandlerConfig res;
        res.type = PrometheusRequestHandlerConfig::Type::QueryAPI;
        parseTableNameFromConfig(config, config_prefix, res);
        parseCommonConfig(config, res);
        if (config.has(config_prefix + ".user"))
        {
            AlwaysAllowCredentials credentials(config.getString(config_prefix + ".user"));
            res.connection_config.credentials.emplace(credentials);
        }
        return res;
    }

    /// Parses a configuration like this:
    /// <type>expose_metrics</type>
    /// <metrics>true</metrics>
    /// <asynchronous_metrics>true</asynchronous_metrics>
    /// <events>true</events>
    /// <errors>true</errors>
    /// -OR-
    /// <type>remote_write</type>
    /// <table>db.time_series_table_name</table>
    PrometheusRequestHandlerConfig parseHandlerConfig(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
    {
        String type = config.getString(config_prefix + ".type");

        if (type == "expose_metrics")
            return parseExposeMetricsConfig(config, config_prefix);
        if (type == "remote_write")
            return parseRemoteWriteConfig(config, config_prefix);
        if (type == "remote_read")
            return parseRemoteReadConfig(config, config_prefix);
        if (type == "query_api")
            return parseQueryAPIConfig(config, config_prefix);
        throw Exception(
            ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG, "Unknown type {} is specified in the configuration for a prometheus protocol", type);
    }

    /// Returns true if the URL path ends with a Prometheus label values endpoint.
    bool isLabelValuesUrl(const String & url)
    {
        static constexpr std::string_view label_marker = "/api/v1/label/";
        static constexpr std::string_view values_suffix = "/values";

        const auto marker_pos = url.find(label_marker);
        if (marker_pos == String::npos || !url.ends_with(values_suffix))
            return false;

        const size_t label_start = marker_pos + label_marker.size();
        const size_t values_pos = url.size() - values_suffix.size();
        if (label_start >= values_pos)
            return false;

        return url.find('/', label_start) == String::npos;
    }

    /// type=prometheus dispatch picks the protocol from literal <url> path suffixes; regex <url> values are rejected here.
    PrometheusRequestHandlerConfig parsePrometheusHTTPHandlerConfig(
        const Poco::Util::AbstractConfiguration & config,
        const String & handler_config_prefix,
        const String & url)
    {
        if (url.starts_with("regex:"))
        {
            throw Exception(
                ErrorCodes::INVALID_CONFIG_PARAMETER,
                "Prometheus http_handlers rule uses a regex <url> ('{}'); protocol dispatch requires a literal URL whose path "
                "ends with a canonical Prometheus endpoint (e.g. '/prometheus/api/v1/write').",
                url);
        }

        if (url.ends_with("/api/v1/write"))
            return parseRemoteWriteConfig(config, handler_config_prefix);
        if (url.ends_with("/api/v1/read"))
            return parseRemoteReadConfig(config, handler_config_prefix);

        if (url.ends_with("/api/v1/query_range")
            || url.ends_with("/api/v1/query")
            || url.ends_with("/api/v1/series")
            || url.ends_with("/api/v1/labels")
            || isLabelValuesUrl(url))
            return parseQueryAPIConfig(config, handler_config_prefix);

        return parseExposeMetricsConfig(config, handler_config_prefix);
    }

    /// Returns true if the protocol represented by a passed config can be handled.
    bool canBeHandled(const PrometheusRequestHandlerConfig & config, bool for_keeper)
    {
        /// The standalone ClickHouse Keeper can only expose its metrics.
        /// It can't handle other Prometheus protocols.
        return !for_keeper || (config.type == PrometheusRequestHandlerConfig::Type::ExposeMetrics);
    }

    /// Creates a writer which serializes exposing metrics.
    std::shared_ptr<PrometheusMetricsWriter> createPrometheusMetricWriter(bool for_keeper)
    {
        if (for_keeper)
            return std::make_unique<KeeperPrometheusMetricsWriter>();
        return std::make_unique<PrometheusMetricsWriter>();
    }

    /// Base function for making a factory for PrometheusRequestHandler. This function can return nullptr.
    std::shared_ptr<HandlingRuleHTTPHandlerFactory<PrometheusRequestHandler>> createPrometheusHandlerFactoryFromConfig(
        IServer & server,
        const AsynchronousMetrics & async_metrics,
        const PrometheusRequestHandlerConfig & config,
        bool for_keeper,
        std::unordered_map<String, String> headers = {})
    {
        if (!canBeHandled(config, for_keeper))
            return nullptr;
        auto metric_writer = createPrometheusMetricWriter(for_keeper);
        auto creator = [&server, &async_metrics, config, metric_writer, headers_moved = std::move(headers)]() -> std::unique_ptr<PrometheusRequestHandler>
        {
            return std::make_unique<PrometheusRequestHandler>(server, config, async_metrics, metric_writer, headers_moved);
        };
        return std::make_shared<HandlingRuleHTTPHandlerFactory<PrometheusRequestHandler>>(std::move(creator));
    }

    /// Generic function for createPrometheusHandlerFactory() and createKeeperPrometheusHandlerFactory().
    HTTPRequestHandlerFactoryPtr createPrometheusHandlerFactoryImpl(
        IServer & server,
        const Poco::Util::AbstractConfiguration & config,
        const AsynchronousMetrics & asynchronous_metrics,
        const String & name,
        bool for_keeper)
    {
        auto factory = std::make_shared<HTTPRequestHandlerFactoryMain>(name);

        if (config.has("prometheus.handlers"))
        {
            Strings keys;
            config.keys("prometheus.handlers", keys);
            for (const String & key : keys)
            {
                String prefix = "prometheus.handlers." + key;
                auto parsed_config = parseHandlerConfig(config, prefix + ".handler");
                if (auto handler = createPrometheusHandlerFactoryFromConfig(server, asynchronous_metrics, parsed_config, for_keeper))
                {
                    handler->addFiltersFromConfig(config, prefix);
                    factory->addHandler(handler);
                }
            }
        }
        else
        {
            auto parsed_config = parseExposeMetricsConfig(config, "prometheus");
            if (auto handler = createPrometheusHandlerFactoryFromConfig(server, asynchronous_metrics, parsed_config, for_keeper))
            {
                String endpoint = config.getString("prometheus.endpoint", "/metrics");
                handler->attachStrictPath(endpoint);
                handler->allowGetAndHeadRequest();
                factory->addHandler(handler);
            }
        }

        return factory;
    }

}


HTTPRequestHandlerFactoryPtr createPrometheusHandlerFactory(
    IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const AsynchronousMetrics & asynchronous_metrics,
    const String & name)
{
    return createPrometheusHandlerFactoryImpl(server, config, asynchronous_metrics, name, /* for_keeper= */ false);
}


HTTPRequestHandlerFactoryPtr createPrometheusHandlerFactoryForHTTPRule(
    IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    const AsynchronousMetrics & asynchronous_metrics,
    std::unordered_map<String, String> & common_headers)
{
    auto headers = parseHTTPResponseHeadersWithCommons(config, config_prefix, common_headers);

    const String handler_config_prefix = config_prefix + ".handler";
    const String handler_type = config.getString(handler_config_prefix + ".type", "expose_metrics");
    const String url = config.getString(config_prefix + ".url", "");

    /// handler.type=prometheus used to always call parseExposeMetricsConfig and ignore <url>, so write/read/query
    /// (and regex: <url> rules that still matched requests) served exposition instead of failing closed at config load.
    PrometheusRequestHandlerConfig parsed_config = [&]() -> PrometheusRequestHandlerConfig
    {
        if (handler_type != "prometheus")
            return parseExposeMetricsConfig(config, handler_config_prefix);

        return parsePrometheusHTTPHandlerConfig(config, handler_config_prefix, url);
    }();

    auto handler = createPrometheusHandlerFactoryFromConfig(server, asynchronous_metrics, parsed_config, /* for_keeper= */ false, headers);
    chassert(handler);  /// `handler` can't be nullptr here because `for_keeper` is false.
    handler->addFiltersFromConfig(config, config_prefix);
    return handler;
}


HTTPRequestHandlerFactoryPtr createPrometheusHandlerFactoryForHTTPRuleDefaults(
    IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const AsynchronousMetrics & asynchronous_metrics)
{
    /// The "defaults" HTTP handler should serve the prometheus exposing metrics protocol on the http port
    /// only if it isn't already served on its own port <prometheus.port> and if there is no <prometheus.handlers> section.
    if (!config.has("prometheus") || config.getInt("prometheus.port", 0) || config.has("prometheus.handlers"))
        return nullptr;

    auto parsed_config = parseExposeMetricsConfig(config, "prometheus");
    String endpoint = config.getString("prometheus.endpoint", "/metrics");
    auto handler = createPrometheusHandlerFactoryFromConfig(server, asynchronous_metrics, parsed_config, /* for_keeper= */ false);
    chassert(handler);  /// `handler` can't be nullptr here because `for_keeper` is false.
    handler->attachStrictPath(endpoint);
    handler->allowGetAndHeadRequest();
    return handler;
}


HTTPRequestHandlerFactoryPtr createKeeperPrometheusHandlerFactory(
    IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const AsynchronousMetrics & asynchronous_metrics,
    const String & name)
{
    return createPrometheusHandlerFactoryImpl(server, config, asynchronous_metrics, name, /* for_keeper= */ true);
}

}
