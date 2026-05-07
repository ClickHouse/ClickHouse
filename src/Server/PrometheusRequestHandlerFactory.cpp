#include <Access/Credentials.h>
#include <Server/PrometheusRequestHandlerFactory.h>

#include <Core/Types_fwd.h>
#include <Server/HTTPHandlerFactory.h>
#include <Server/PrometheusMetricsWriter.h>
#include <Server/PrometheusRequestHandler.h>
#include <Server/PrometheusRequestHandlerConfig.h>
#include <Common/StringUtils.h>
#include <Common/re2.h>

#include <Poco/Net/HTTPRequest.h>


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
        res.time_series_table_name = parseTableNameFromConfig(config, config_prefix);
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


namespace
{
    /// Normalizes a user-supplied `http_path_prefix`:
    ///   - empty stays empty (caller decides what that means);
    ///   - missing leading '/' is added;
    ///   - trailing '/' characters are trimmed;
    ///   - the bare root prefix `/` is collapsed to empty so neither the route regex nor the
    ///     handler-side path strip inserts an extra leading slash (which would require URLs
    ///     like `//write` instead of `/write`).
    /// This is shared between the auto-mount path (`addPrometheusProtocolsToHTTPDefaults`) and
    /// the per-rule `<http_handlers>` path (`parseHTTPRuleHandlerConfig`) so they cannot drift.
    String normalizeHTTPPathPrefix(String prefix)
    {
        if (prefix.empty())
            return prefix;
        while (prefix.size() > 1 && prefix.back() == '/')
            prefix.pop_back();
        if (prefix.front() != '/')
            prefix.insert(prefix.begin(), '/');
        if (prefix == "/")
            prefix.clear();
        return prefix;
    }

    /// Rejects fixed-table config fields (@c table / @c database) that have no meaning when the
    /// handler is mounted under @c http_handlers -- that mount is dynamic-routing-only.
    void rejectFixedTableKeysForHTTPRule(const Poco::Util::AbstractConfiguration & config, const String & handler_prefix)
    {
        for (const auto & key : {"table", "database"})
        {
            if (config.has(handler_prefix + "." + key))
                throw Exception(
                    ErrorCodes::INVALID_CONFIG_PARAMETER,
                    "Prometheus handler under <http_handlers> cannot specify <{}>: dynamic routing is the only supported mode here. "
                    "The target table is resolved from `database` / `table` query parameters (or "
                    "`X-ClickHouse-Database` / `X-ClickHouse-Table` headers). Remove <{}> from {}.",
                    key, key, handler_prefix);
        }
    }

    /// Parses a `<prometheus>`-style handler configuration block intended for use under
    /// `<http_handlers>`.
    ///
    /// Two shapes are accepted:
    ///   - Legacy `<type>prometheus</type>` (the historical handler type wired up by
    ///     @c HTTPHandlerFactory) is kept as-is and behaves exactly like the old code: the handler
    ///     exposes metrics, with the @c metrics / @c events / @c asynchronous_metrics / @c errors
    ///     toggles read from the same handler block. @c expose_metrics is accepted as an
    ///     equivalent spelling (matches the dedicated-port @c <prometheus.handlers> grammar).
    ///   - New explicit handler types @c prometheus_remote_write / @c prometheus_remote_read /
    ///     @c prometheus_query_api enable the dynamic-routing-only mode: the parsed config has
    ///     @c enable_table_name_url_routing set to true, @c time_series_table_name left empty,
    ///     and the @c (database, table) pair is later resolved from query parameters / headers.
    PrometheusRequestHandlerConfig parseHTTPRuleHandlerConfig(
        const Poco::Util::AbstractConfiguration & config, const String & handler_prefix)
    {
        const String type = config.getString(handler_prefix + ".type");

        /// Backward-compatible expose-metrics shape under @c <http_handlers>. Historically the
        /// inner @c <type> value here was just the dispatcher's marker and the handler block was
        /// always parsed as expose-metrics; preserve that behavior so existing deployments keep
        /// working.
        if (type == "prometheus" || type == "expose_metrics")
        {
            auto res = parseExposeMetricsConfig(config, handler_prefix);
            return res;
        }

        PrometheusRequestHandlerConfig res;
        if (type == "prometheus_remote_write")
            res.type = PrometheusRequestHandlerConfig::Type::RemoteWrite;
        else if (type == "prometheus_remote_read")
            res.type = PrometheusRequestHandlerConfig::Type::RemoteRead;
        else if (type == "prometheus_query_api")
            res.type = PrometheusRequestHandlerConfig::Type::QueryAPI;
        else
            throw Exception(
                ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG,
                "Unknown type {} is specified in the configuration for a prometheus protocol", type);

        rejectFixedTableKeysForHTTPRule(config, handler_prefix);

        res.enable_table_name_url_routing = true;
        /// Per-rule prefixes go through the same normalization as the auto-mount so that an
        /// `<http_path_prefix>explicit</http_path_prefix>` (no leading slash) or a stray
        /// trailing slash both line up with the request URI in `computeDispatchInfo`.
        res.http_path_prefix = normalizeHTTPPathPrefix(
            config.getString(handler_prefix + ".http_path_prefix", ""));
        parseCommonConfig(config, res);

        if ((res.type == PrometheusRequestHandlerConfig::Type::RemoteRead
                || res.type == PrometheusRequestHandlerConfig::Type::QueryAPI)
            && config.has(handler_prefix + ".user"))
        {
            AlwaysAllowCredentials credentials(config.getString(handler_prefix + ".user"));
            res.connection_config.credentials.emplace(credentials);
        }

        return res;
    }

    /// Compiles `regex_pattern` into a filter that matches the request URL path (query string
    /// stripped) anchored at both ends.
    auto regexPathFilter(const String & regex_pattern)
    {
        auto compiled = std::make_shared<const re2::RE2>(regex_pattern);
        if (!compiled->ok())
            throw Exception(
                ErrorCodes::INVALID_CONFIG_PARAMETER,
                "Cannot compile regex {} for prometheus URL filter: {}",
                regex_pattern, compiled->error());

        return [compiled](const HTTPServerRequest & request)
        {
            const auto & uri = request.getURI();
            const auto question = uri.find('?');
            std::string_view path = (question == String::npos) ? std::string_view{uri} : std::string_view{uri.data(), question};
            return compiled->Match({path.data(), path.size()}, 0, path.size(), re2::RE2::ANCHOR_BOTH, nullptr, 0);
        };
    }

    /// Returns a method-list filter equivalent to @c methods in @c http_handlers.
    auto methodsListFilter(std::vector<String> methods_in)
    {
        return [methods = std::move(methods_in)](const HTTPServerRequest & request)
        {
            return std::find(methods.begin(), methods.end(), request.getMethod()) != methods.end();
        };
    }

    /// Adds the three dynamic-routing rules (RemoteWrite, RemoteRead, QueryAPI) to `factory`,
    /// matching `^<prefix>/<protocol-suffix>$` (database/table are query params, not path
    /// segments). @c prefix is treated as a literal URL prefix and is regex-escaped before
    /// being interpolated into the route filter.
    void addDynamicRoutingRules(
        HTTPRequestHandlerFactoryMain & factory,
        IServer & server,
        const Poco::Util::AbstractConfiguration & config,
        const AsynchronousMetrics & async_metrics,
        const String & prefix)
    {
        const std::string escaped_prefix = re2::RE2::QuoteMeta(std::string_view{prefix});

        auto add_rule = [&](PrometheusRequestHandlerConfig::Type type,
                            const String & path_suffix_regex,
                            std::vector<String> allowed_methods)
        {
            PrometheusRequestHandlerConfig parsed_config;
            parsed_config.type = type;
            parsed_config.enable_table_name_url_routing = true;
            parsed_config.http_path_prefix = prefix;
            /// Pull the shared `<prometheus>` toggles (currently @c enable_stacktrace, with the
            /// historical default of @c true) so the auto-mounted handlers behave the same as the
            /// dedicated-port and explicit-`<http_handlers>` paths instead of always force-enabling
            /// stack traces in HTTP error bodies.
            parseCommonConfig(config, parsed_config);

            auto handler = createPrometheusHandlerFactoryFromConfig(server, async_metrics, parsed_config, /* for_keeper= */ false);
            chassert(handler);
            handler->addFilter(regexPathFilter("^" + escaped_prefix + path_suffix_regex + "$"));
            handler->addFilter(methodsListFilter(std::move(allowed_methods)));
            factory.addHandler(std::move(handler));
        };

        add_rule(PrometheusRequestHandlerConfig::Type::RemoteWrite, "/write",
                 {Poco::Net::HTTPRequest::HTTP_POST});
        /// The Prometheus remote-read spec uses POST, but the original ClickHouse server has
        /// historically also accepted GET on the dedicated port (see
        /// @c get_response_to_remote_read in the integration tests). Accept both for back-compat.
        add_rule(PrometheusRequestHandlerConfig::Type::RemoteRead, "/read",
                 {Poco::Net::HTTPRequest::HTTP_POST, Poco::Net::HTTPRequest::HTTP_GET});
        add_rule(PrometheusRequestHandlerConfig::Type::QueryAPI,
                 "/api/v1/(query|query_range|series|labels|label/[^/]+/values|metadata)",
                 {Poco::Net::HTTPRequest::HTTP_GET, Poco::Net::HTTPRequest::HTTP_POST,
                  Poco::Net::HTTPRequest::HTTP_OPTIONS, Poco::Net::HTTPRequest::HTTP_HEAD});
    }
}


HTTPRequestHandlerFactoryPtr createPrometheusHandlerFactoryForHTTPRule(
    IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    const AsynchronousMetrics & asynchronous_metrics,
    std::unordered_map<String, String> & common_headers)
{
    auto headers = parseHTTPResponseHeadersWithCommons(config, config_prefix, common_headers);

    auto parsed_config = parseHTTPRuleHandlerConfig(config, config_prefix + ".handler");
    auto handler = createPrometheusHandlerFactoryFromConfig(server, asynchronous_metrics, parsed_config, /* for_keeper= */ false, headers);
    chassert(handler);  /// `handler` can't be nullptr here because `for_keeper` is false.
    handler->addFiltersFromConfig(config, config_prefix);
    return handler;
}


void addPrometheusProtocolsToHTTPDefaults(
    HTTPRequestHandlerFactoryMain & factory,
    IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const AsynchronousMetrics & async_metrics)
{
    /// Auto-mounting only makes sense when the user has a `<prometheus>` block; otherwise we
    /// don't want to reserve any URL space at all.
    if (!config.has("prometheus"))
        return;

    /// When `<prometheus><keeper_metrics_only>true</keeper_metrics_only>` is set, the
    /// dedicated Prometheus listener is intentionally narrowed to keeper-related scrape
    /// metrics (`KeeperPrometheusHandler`). Do not expand the request surface by also
    /// auto-mounting `remote_write`/`remote_read`/Query API on the main `<http_port>`.
    if (config.getBool("prometheus.keeper_metrics_only", false))
        return;

    /// An empty value for `<http_path_prefix>` is the explicit opt-out for the auto-mount.
    /// The normalization step below would also collapse `/` to empty (root mount), but root
    /// mount is a valid configuration we want to honor, so we have to distinguish "user did
    /// not set the key" (use the default) from "user set the key to empty" (opt out) BEFORE
    /// normalizing.
    if (config.has("prometheus.http_path_prefix") && config.getString("prometheus.http_path_prefix").empty())
        return;

    String prefix = normalizeHTTPPathPrefix(
        config.getString("prometheus.http_path_prefix", "/prometheus"));

    addDynamicRoutingRules(factory, server, config, async_metrics, prefix);
}


HTTPRequestHandlerFactoryPtr createPrometheusHandlerFactoryForHTTPRuleDefaults(
    IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const AsynchronousMetrics & asynchronous_metrics)
{
    /// Optional scrape on http_port: only when scrape is not already on <prometheus.port> and
    /// <prometheus.handlers> does not take over handler wiring (see header comment).
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
