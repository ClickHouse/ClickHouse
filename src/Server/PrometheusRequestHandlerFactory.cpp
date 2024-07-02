#include <Server/PrometheusRequestHandlerFactory.h>

#include <Server/HTTPHandlerFactory.h>
#include <Server/PrometheusRequestHandler.h>
#include <Server/PrometheusRequestHandlerConfig.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
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
        res.expose_metrics = config.getBool(config_prefix + ".metrics", true);
        res.expose_asynchronous_metrics = config.getBool(config_prefix + ".asynchronous_metrics", true);
        res.expose_events = config.getBool(config_prefix + ".events", true);
        res.expose_errors = config.getBool(config_prefix + ".errors", true);
        parseCommonConfig(config, res);
        return res;
    }

#ifndef CLICKHOUSE_KEEPER_STANDALONE_BUILD

    /// Extracts a qualified table name from the config. It can be set either as
    ///     <table>mydb.prometheus</table>
    /// or
    ///     <database>mydb</database>
    ///     <table>prometheus</table>
    QualifiedTableName parseTableNameFromConfig(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
    {
        QualifiedTableName res;
        res.table = config.getString(config_prefix + ".table", "prometheus");
        res.database = config.getString(config_prefix + ".database", "");
        if (res.database.empty())
            res = QualifiedTableName::parseFromString(res.table);
        if (res.database.empty())
            res.database = "default";
        return res;
    }

    /// Parses a configuration like this:
    /// <!-- <type>remote_write</type> (Implied, not actually parsed) -->
    /// <table>db.time_series_table_name</table>
    PrometheusRequestHandlerConfig parseRemoteWriteConfig(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
    {
        PrometheusRequestHandlerConfig res;
        res.type = PrometheusRequestHandlerConfig::Type::RemoteWrite;
        res.time_series_table_name = parseTableNameFromConfig(config, config_prefix);
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
        res.time_series_table_name = parseTableNameFromConfig(config, config_prefix);
        parseCommonConfig(config, res);
        return res;
    }

#endif

    /// Parses a configuration like this:
    /// <type>expose_metrics</type>
    /// <metrics>true</metrics>
    /// <asynchronous_metrics>true</asynchronous_metrics>
    /// <events>true</events>
    /// <errors>true</errors>
    /// -OR-
    /// <type>remote_read</type>
    /// <table>db.time_series_table_name</table>
    std::optional<PrometheusRequestHandlerConfig> parseHandlerConfig(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
    {
        String type = config.getString(config_prefix + ".type");

        if (type == "expose_metrics")
            return parseExposeMetricsConfig(config, config_prefix);

        if (type == "remote_write")
        {
#ifndef CLICKHOUSE_KEEPER_STANDALONE_BUILD
            return parseRemoteWriteConfig(config, config_prefix);
#else
            return std::nullopt; /// The standalone Keeper allows the "remote_write" type in its configuration but just skips it.
#endif
        }

        if (type == "remote_read")
        {
#ifndef CLICKHOUSE_KEEPER_STANDALONE_BUILD
            return parseRemoteReadConfig(config, config_prefix);
#else
            return std::nullopt; /// The standalone Keeper allows the "remote_read" type in its configuration but just skips it.
#endif
        }

        throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG, "Unknown type {} is specified in the configuration for a prometheus protocol", type);
    }

    std::shared_ptr<HandlingRuleHTTPHandlerFactory<PrometheusRequestHandler>> createPrometheusHandlerFactoryImpl(
        IServer & server,
        const AsynchronousMetrics & async_metrics,
        const PrometheusRequestHandlerConfig & parsed_config)
    {
        auto creator = [&server, &async_metrics, parsed_config]() -> std::unique_ptr<PrometheusRequestHandler>
        {
            return std::make_unique<PrometheusRequestHandler>(server, parsed_config, async_metrics);
        };
        return std::make_shared<HandlingRuleHTTPHandlerFactory<PrometheusRequestHandler>>(std::move(creator));
    }
}


HTTPRequestHandlerFactoryPtr createPrometheusHandlerFactory(
    IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const AsynchronousMetrics & asynchronous_metrics,
    const String & name)
{
    auto factory = std::make_shared<HTTPRequestHandlerFactoryMain>(name);

    if (config.has("prometheus.handlers"))
    {
        Strings keys;
        config.keys("prometheus.handlers", keys);
        for (const String & key : keys)
        {
            String prefix = "prometheus.handlers." + key;
            if (auto parsed_config = parseHandlerConfig(config, prefix + ".handler"))
            {
                auto handler = createPrometheusHandlerFactoryImpl(server, asynchronous_metrics, *parsed_config);
                handler->addFiltersFromConfig(config, prefix);
                factory->addHandler(handler);
            }
        }
    }
    else
    {
        auto parsed_config = parseExposeMetricsConfig(config, "prometheus");
        String endpoint = config.getString("prometheus.endpoint", "/metrics");
        auto handler = createPrometheusHandlerFactoryImpl(server, asynchronous_metrics, parsed_config);
        handler->attachStrictPath(endpoint);
        handler->allowGetAndHeadRequest();
        factory->addHandler(handler);
    }

    return factory;
}

#ifndef CLICKHOUSE_KEEPER_STANDALONE_BUILD

HTTPRequestHandlerFactoryPtr createPrometheusHandlerFactoryForHTTPRule(
    IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    const AsynchronousMetrics & asynchronous_metrics)
{
    auto parsed_config = parseExposeMetricsConfig(config, config_prefix);
    return createPrometheusHandlerFactoryImpl(server, asynchronous_metrics, parsed_config);
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
    auto handler = createPrometheusHandlerFactoryImpl(server, asynchronous_metrics, parsed_config);
    handler->attachStrictPath(endpoint);
    handler->allowGetAndHeadRequest();
    return handler;
}

#endif

}
