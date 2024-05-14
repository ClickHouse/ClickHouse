#include <Server/PrometheusRequestHandlerFactory.h>

#include <Server/HTTPHandlerFactory.h>
#include "Server/PrometheusMetricsWriter.h"
#include "Server/PrometheusRequestHandler.h"
#include "Server/PrometheusRequestHandlerConfig.h"


namespace DB
{

namespace
{
    std::shared_ptr<HandlingRuleHTTPHandlerFactory<PrometheusRequestHandler>> createPrometheusHandlerFactoryImpl(
        IServer & server,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const AsynchronousMetrics & asynchronous_metrics,
        const PrometheusMetricsWriterPtr & metrics_writer,
        bool use_default_filter_by_endpoint_and_http_method = true)
    {
        auto parsed_config = std::make_shared<PrometheusRequestHandlerConfig>(config, config_prefix);

        auto creator = [&server, parsed_config, &asynchronous_metrics, metrics_writer]() -> std::unique_ptr<PrometheusRequestHandler>
        {
            return std::make_unique<PrometheusRequestHandler>(server, parsed_config, asynchronous_metrics, metrics_writer);
        };

        auto factory = std::make_shared<HandlingRuleHTTPHandlerFactory<PrometheusRequestHandler>>(std::move(creator));

        if (use_default_filter_by_endpoint_and_http_method)
        {
            auto filter = [parsed_config](const HTTPServerRequest & request) -> bool { return parsed_config->filterRequest(request); };
            factory->addFilter(filter);
        }

        return factory;
    }
}

HTTPRequestHandlerFactoryPtr createPrometheusHandlerFactoryMain(
    IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const String & name,
    const AsynchronousMetrics & asynchronous_metrics)
{
    auto factory = std::make_shared<HTTPRequestHandlerFactoryMain>(name);
    factory->addHandler(createPrometheusHandlerFactoryDefault(server, config, asynchronous_metrics));
    return factory;
}

HTTPRequestHandlerFactoryPtr createPrometheusHandlerFactoryDefault(
    IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const AsynchronousMetrics & asynchronous_metrics)
{
    auto metrics_writer = std::make_shared<PrometheusMetricsWriter>();
    return createPrometheusHandlerFactoryImpl(server, config, "prometheus", asynchronous_metrics, metrics_writer);
}

HTTPRequestHandlerFactoryPtr createPrometheusHandlerFactoryForRule(
    IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    const AsynchronousMetrics & asynchronous_metrics)
{
    auto metrics_writer = std::make_shared<PrometheusMetricsWriter>();

    /// Here `use_default_filter_by_endpoint_and_http_method` is set to `false`
    /// because for http rules inside section <http_handlers>:
    /// <http_handlers>
    ///     <rule_my_handler>
    ///         <handler>
    ///             <type>prometheus</type>
    ///             <metrics>true</metrics>
    ///             <asynchronous_metrics>true</asynchronous_metrics>
    ///             <events>true</events>
    ///             <errors>true</errors>
    ///         </handler>
    ///         <methods>HEAD,GET</methods>
    ///         <url>regex:.*/metrics</url>
    ///     </rule_my_handler>
    /// </http_handlers>
    /// we don't need the default URL filter by the default endpoint `/metrics` because a regular expression in the <url> tag must be used instead.
    bool use_default_filter_by_endpoint_and_http_method = false;

    auto factory = createPrometheusHandlerFactoryImpl(
        server, config, config_prefix + ".handler", asynchronous_metrics, metrics_writer, use_default_filter_by_endpoint_and_http_method);

    factory->addFiltersFromConfig(config, config_prefix);
    return factory;
}

HTTPRequestHandlerFactoryPtr createPrometheusHandlerFactoryForKeeper(
    IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const String & name,
    const AsynchronousMetrics & asynchronous_metrics)
{
    auto metrics_writer = std::make_shared<KeeperPrometheusMetricsWriter>();
    auto factory = std::make_shared<HTTPRequestHandlerFactoryMain>(name);
    factory->addHandler(createPrometheusHandlerFactoryImpl(server, config, "prometheus", asynchronous_metrics, metrics_writer));
    return factory;
}

}
