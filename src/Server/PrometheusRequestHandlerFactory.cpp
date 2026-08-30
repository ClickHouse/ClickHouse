#include <Access/Credentials.h>
#include <Server/PrometheusRequestHandlerFactory.h>

#include <Core/Types_fwd.h>
#include <Server/HTTPHandlerFactory.h>
#include <Server/PrometheusMetricsWriter.h>
#include <Server/PrometheusRequestHandler.h>
#include <Server/PrometheusRequestHandlerConfig.h>
#include <Common/StringUtils.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
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

    /// Prometheus label names must match [a-zA-Z_][a-zA-Z0-9_]*.
    bool isValidPrometheusLabelName(const String & name)
    {
        if (name.empty() || isNumericASCII(name[0]))
            return false;
        for (char c : name)
        {
            if (!isWordCharASCII(c))
                return false;
        }
        return true;
    }

    /// Parses a configuration like this:
    /// <labels>
    ///     <shard>1</shard>
    ///     <replica>r1</replica>
    /// </labels>
    void parseConstantLabelsFromConfig(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, PrometheusRequestHandlerConfig & res)
    {
        const String labels_prefix = config_prefix + ".labels";
        if (!config.has(labels_prefix))
            return;

        Strings label_names;
        config.keys(labels_prefix, label_names);
        for (const String & label_name : label_names)
        {
            if (!isValidPrometheusLabelName(label_name))
                throw Exception(
                    ErrorCodes::INVALID_CONFIG_PARAMETER,
                    "Invalid Prometheus label name '{}' in the configuration: label names must match [a-zA-Z_][a-zA-Z0-9_]* and must not be repeated",
                    label_name);
            if (label_name.starts_with("__"))
                throw Exception(
                    ErrorCodes::INVALID_CONFIG_PARAMETER,
                    "Invalid Prometheus label name '{}' in the configuration: names starting with '__' are reserved by Prometheus",
                    label_name);
            /// Collisions with labels this endpoint writes itself (le, ClickHouse_Info labels, exposed
            /// family labels) depend on the active export surface and are validated in
            /// createPrometheusMetricWriter(), where `for_keeper` and the expose_* flags are known.
            res.constant_labels[label_name] = config.getString(labels_prefix + "." + label_name);
        }
    }

    /// Parses a configuration like this:
    /// <!-- <type>metrics</type> (Implied, not actually parsed) -->
    /// <metrics>true</metrics>
    /// <asynchronous_metrics>true</asynchronous_metrics>
    /// <events>true</events>
    /// <errors>true</errors>
    /// <labels><shard>1</shard></labels>
    PrometheusRequestHandlerConfig parseMetricsConfig(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
    {
        PrometheusRequestHandlerConfig res;
        res.type = PrometheusRequestHandlerConfig::Type::Metrics;
        res.expose_info = config.getBool(config_prefix + ".info", true);
        res.expose_metrics = config.getBool(config_prefix + ".metrics", true);
        res.expose_asynchronous_metrics = config.getBool(config_prefix + ".asynchronous_metrics", true);
        res.expose_events = config.getBool(config_prefix + ".events", true);
        res.expose_errors = config.getBool(config_prefix + ".errors", true);
        res.expose_histograms = config.getBool(config_prefix + ".histograms", true);
        res.expose_dimensional_metrics = config.getBool(config_prefix + ".dimensional_metrics", true);
        parseConstantLabelsFromConfig(config, config_prefix, res);
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

    /// Parses the optional <user> element and stores it as credentials in the connection config.
    void parseUserFromConfig(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, PrometheusRequestHandlerConfig & res)
    {
        if (config.has(config_prefix + ".user"))
        {
            AlwaysAllowCredentials credentials(config.getString(config_prefix + ".user"));
            res.connection_config.credentials.emplace(credentials);
        }
    }

    void parseURLRoutingFromConfig(
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        PrometheusRequestHandlerConfig & res)
    {
        if (config.has(config_prefix + ".enable_table_name_url_routing"))
        {
            res.enable_table_name_url_routing = config.getBool(config_prefix + ".enable_table_name_url_routing");

            if (res.enable_table_name_url_routing
                && (!res.time_series_table_name.database.empty() || !res.time_series_table_name.table.empty()))
            {
                throw Exception(
                    ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG,
                    "Setting 'enable_table_name_url_routing' cannot be combined with a configured 'database' or 'table'");
            }
        }
    }

    void validateURLRoutingHandlerType(
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        PrometheusRequestHandlerConfig::Type type,
        std::string_view full_type)
    {
        if (!config.has(config_prefix + ".enable_table_name_url_routing"))
            return;

        if (type == PrometheusRequestHandlerConfig::Type::Write || type == PrometheusRequestHandlerConfig::Type::APIv1)
            return;

        throw Exception(
            ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG,
            "Setting 'enable_table_name_url_routing' is not supported for Prometheus handler type '{}'; "
            "it is supported only for remote-write handlers and 'prometheus_api_v1'",
            full_type);
    }

    /// Parses a configuration like this:
    /// <!-- <type>write</type> (Implied, not actually parsed) -->
    /// <table>db.time_series_table_name</table>
    PrometheusRequestHandlerConfig parseWriteConfig(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
    {
        PrometheusRequestHandlerConfig res;
        res.type = PrometheusRequestHandlerConfig::Type::Write;
        parseTableNameFromConfig(config, config_prefix, res);
        parseURLRoutingFromConfig(config, config_prefix, res);
        parseCommonConfig(config, res);
        parseUserFromConfig(config, config_prefix, res);
        return res;
    }

    /// Parses a configuration like this:
    /// <!-- <type>read</type> (Implied, not actually parsed) -->
    /// <table>db.time_series_table_name</table>
    PrometheusRequestHandlerConfig parseReadConfig(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
    {
        PrometheusRequestHandlerConfig res;
        res.type = PrometheusRequestHandlerConfig::Type::Read;
        parseTableNameFromConfig(config, config_prefix, res);
        parseCommonConfig(config, res);
        parseUserFromConfig(config, config_prefix, res);
        return res;
    }

    /// Parses a configuration like this:
    /// <!-- <type>query</type> (Implied, not actually parsed) -->
    /// <table>db.time_series_table_name</table>
    PrometheusRequestHandlerConfig parseQueryConfig(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
    {
        PrometheusRequestHandlerConfig res;
        res.type = PrometheusRequestHandlerConfig::Type::Query;
        parseTableNameFromConfig(config, config_prefix, res);
        parseCommonConfig(config, res);
        parseUserFromConfig(config, config_prefix, res);
        return res;
    }

    /// Parses a configuration like this:
    /// <!-- <type>api_v1</type> (Implied, not actually parsed) -->
    /// <table>db.time_series_table_name</table>
    PrometheusRequestHandlerConfig parseAPIv1Config(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
    {
        PrometheusRequestHandlerConfig res;
        res.type = PrometheusRequestHandlerConfig::Type::APIv1;
        parseTableNameFromConfig(config, config_prefix, res);
        parseURLRoutingFromConfig(config, config_prefix, res);
        parseCommonConfig(config, res);
        parseUserFromConfig(config, config_prefix, res);
        return res;
    }

    /// Parses the protocol type specified in the <type> element of a handler's configuration.
    PrometheusRequestHandlerConfig::Type parseHandlerType(std::string_view full_type)
    {
        /// Strip a "prometheus_" prefix from the type (e.g. "prometheus_write" -> "write").
        /// "prometheus" alone is an alias for "metrics".
        std::string_view type = full_type;
        if (type == "prometheus")
            type = "metrics";
        else if (type.starts_with("prometheus_"))
            type = type.substr(strlen("prometheus_"));

        /// The "expose_metrics", "remote_write", "remote_read" and "query_api" names are kept
        /// as deprecated aliases for the current "metrics", "write", "read" and "query" names.
        if (type == "metrics" || type == "expose_metrics")
            return PrometheusRequestHandlerConfig::Type::Metrics;
        if (type == "write" || type == "remote_write")
            return PrometheusRequestHandlerConfig::Type::Write;
        if (type == "read" || type == "remote_read")
            return PrometheusRequestHandlerConfig::Type::Read;
        if (type == "query" || type == "query_api")
            return PrometheusRequestHandlerConfig::Type::Query;
        if (type == "api_v1")
            return PrometheusRequestHandlerConfig::Type::APIv1;

        throw Exception(
            ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG, "Unknown type {} is specified in the configuration for a prometheus protocol", full_type);
    }

    /// Parses a configuration like this:
    /// <type>metrics</type>
    /// <metrics>true</metrics>
    /// <asynchronous_metrics>true</asynchronous_metrics>
    /// <events>true</events>
    /// <errors>true</errors>
    /// -OR-
    /// <type>write</type>
    /// <table>db.time_series_table_name</table>
    PrometheusRequestHandlerConfig parseHandlerConfig(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
    {
        const auto full_type = config.getString(config_prefix + ".type");
        auto type = parseHandlerType(full_type);
        validateURLRoutingHandlerType(config, config_prefix, type, full_type);
        switch (type)
        {
            case PrometheusRequestHandlerConfig::Type::Metrics:
                return parseMetricsConfig(config, config_prefix);
            case PrometheusRequestHandlerConfig::Type::Write:
                return parseWriteConfig(config, config_prefix);
            case PrometheusRequestHandlerConfig::Type::Read:
                return parseReadConfig(config, config_prefix);
            case PrometheusRequestHandlerConfig::Type::Query:
                return parseQueryConfig(config, config_prefix);
            case PrometheusRequestHandlerConfig::Type::APIv1:
                return parseAPIv1Config(config, config_prefix);
        }
        UNREACHABLE();
    }

    /// Returns true if the protocol represented by a passed config can be handled.
    bool canBeHandled(const PrometheusRequestHandlerConfig & config, bool for_keeper)
    {
        /// The standalone ClickHouse Keeper can only expose its metrics.
        /// It can't handle other Prometheus protocols.
        return !for_keeper || (config.type == PrometheusRequestHandlerConfig::Type::Metrics);
    }

    /// Creates a writer which serializes exposing metrics.
    std::shared_ptr<PrometheusMetricsWriter> createPrometheusMetricWriter(const PrometheusRequestHandlerConfig & config, bool for_keeper)
    {
        std::shared_ptr<PrometheusMetricsWriter> writer;
        if (for_keeper)
            writer = std::make_unique<KeeperPrometheusMetricsWriter>(config.constant_labels);
        else
            writer = std::make_unique<PrometheusMetricsWriter>(config.constant_labels);

        /// A constant label must not reuse a label name this endpoint writes itself for one of its
        /// enabled sections (the "le" label, the ClickHouse_Info labels, or an exposed histogram/
        /// dimensional family label) - otherwise an exported sample would carry two labels with the same
        /// name. The reserved set is derived from the writer's actual surface (server vs Keeper) and the
        /// enabled expose_* flags, so a name is only rejected when it can really collide.
        if (!config.constant_labels.empty())
        {
            const auto reserved_names = writer->getReservedLabelNames(
                config.expose_info, config.expose_asynchronous_metrics, config.expose_histograms, config.expose_dimensional_metrics);
            for (const auto & label : config.constant_labels)
            {
                if (reserved_names.contains(label.first))
                    throw Exception(
                        ErrorCodes::INVALID_CONFIG_PARAMETER,
                        "Invalid Prometheus label name '{}' in the configuration: this name is reserved by ClickHouse "
                        "for a metric exposed by this endpoint and cannot be used as a constant label",
                        label.first);
            }
        }

        return writer;
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
        auto metric_writer = createPrometheusMetricWriter(config, for_keeper);
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
        bool for_keeper,
        const std::optional<String> & default_session_user = {})
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
                parsed_config.connection_config.default_session_user = default_session_user;
                if (auto handler = createPrometheusHandlerFactoryFromConfig(server, asynchronous_metrics, parsed_config, for_keeper))
                {
                    handler->addFiltersFromConfig(config, prefix);
                    factory->addHandler(handler);
                }
            }
        }
        else
        {
            auto parsed_config = parseMetricsConfig(config, "prometheus");
            parsed_config.connection_config.default_session_user = default_session_user;
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
    const String & name,
    const std::optional<String> & default_session_user)
{
    return createPrometheusHandlerFactoryImpl(server, config, asynchronous_metrics, name, /* for_keeper= */ false, default_session_user);
}


HTTPRequestHandlerFactoryPtr createPrometheusHandlerFactoryForHTTPRule(
    IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    const AsynchronousMetrics & asynchronous_metrics,
    std::unordered_map<String, String> & common_headers,
    const std::optional<String> & default_session_user)
{
    auto headers = parseHTTPResponseHeadersWithCommons(config, config_prefix, common_headers);

    const String handler_config_prefix = config_prefix + ".handler";

    PrometheusRequestHandlerConfig parsed_config = parseHandlerConfig(config, handler_config_prefix);
    parsed_config.connection_config.default_session_user = default_session_user;

    auto handler = createPrometheusHandlerFactoryFromConfig(server, asynchronous_metrics, parsed_config, /* for_keeper= */ false, headers);
    chassert(handler);  /// `handler` can't be nullptr here because `for_keeper` is false.
    handler->addFiltersFromConfig(config, config_prefix);
    return handler;
}


HTTPRequestHandlerFactoryPtr createPrometheusHandlerFactoryForHTTPRuleDefaults(
    IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const AsynchronousMetrics & asynchronous_metrics,
    const std::optional<String> & default_session_user)
{
    /// The "defaults" HTTP handler should serve the prometheus exposing metrics protocol on the http port
    /// only if it isn't already served on its own port <prometheus.port> and if there is no <prometheus.handlers> section.
    if (!config.has("prometheus") || config.getInt("prometheus.port", 0) || config.has("prometheus.handlers"))
        return nullptr;

    auto parsed_config = parseMetricsConfig(config, "prometheus");
    parsed_config.connection_config.default_session_user = default_session_user;
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
    const String & name,
    const std::optional<String> & default_session_user)
{
    return createPrometheusHandlerFactoryImpl(server, config, asynchronous_metrics, name, /* for_keeper= */ true, default_session_user);
}

}
