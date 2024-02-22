#include <Server/HTTPHandlerFactory.h>

#include <Server/HTTP/HTTPRequestHandler.h>
#include <Server/IServer.h>
#include <Access/Credentials.h>

#include <Poco/Util/AbstractConfiguration.h>

#include "HTTPHandler.h"
#include "NotFoundHandler.h"
#include "StaticRequestHandler.h"
#include "ReplicasStatusHandler.h"
#include "InterserverIOHTTPHandler.h"
#include "PrometheusRequestHandler.h"
#include "WebUIRequestHandler.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int INVALID_CONFIG_PARAMETER;
}

static void addCommonDefaultHandlersFactory(HTTPRequestHandlerFactoryMain & factory, IServer & server);
static void addDefaultHandlersFactory(
    HTTPRequestHandlerFactoryMain & factory,
    IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    AsynchronousMetrics & async_metrics);

static inline auto createHandlersFactoryFromConfig(
    IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & name,
    const String & prefix,
    AsynchronousMetrics & async_metrics)
{
    auto main_handler_factory = std::make_shared<HTTPRequestHandlerFactoryMain>(name);

    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(prefix, keys);

    for (const auto & key : keys)
    {
        if (key == "defaults")
        {
            addDefaultHandlersFactory(*main_handler_factory, server, config, async_metrics);
        }
        else if (startsWith(key, "rule"))
        {
            const auto & handler_type = config.getString(prefix + "." + key + ".handler.type", "");

            if (handler_type.empty())
                throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Handler type in config is not specified here: "
                    "{}.{}.handler.type", prefix, key);

            if (handler_type == "static")
                main_handler_factory->addHandler(createStaticHandlerFactory(server, config, prefix + "." + key));
            else if (handler_type == "dynamic_query_handler")
                main_handler_factory->addHandler(createDynamicHandlerFactory(server, config, prefix + "." + key));
            else if (handler_type == "predefined_query_handler")
                main_handler_factory->addHandler(createPredefinedHandlerFactory(server, config, prefix + "." + key));
            else if (handler_type == "prometheus")
                main_handler_factory->addHandler(createPrometheusHandlerFactory(server, config, async_metrics, prefix + "." + key));
            else if (handler_type == "replicas_status")
                main_handler_factory->addHandler(createReplicasStatusHandlerFactory(server, config, prefix + "." + key));
            else
                throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Unknown handler type '{}' in config here: {}.{}.handler.type",
                    handler_type, prefix, key);
        }
        else
            throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG, "Unknown element in config: "
                "{}.{}, must be 'rule' or 'defaults'", prefix, key);
    }

    return main_handler_factory;
}

static inline HTTPRequestHandlerFactoryPtr
createHTTPHandlerFactory(IServer & server, const Poco::Util::AbstractConfiguration & config, const std::string & name, AsynchronousMetrics & async_metrics)
{
    if (config.has("http_handlers"))
    {
        return createHandlersFactoryFromConfig(server, config, name, "http_handlers", async_metrics);
    }
    else
    {
        auto factory = std::make_shared<HTTPRequestHandlerFactoryMain>(name);
        addDefaultHandlersFactory(*factory, server, config, async_metrics);
        return factory;
    }
}

static inline HTTPRequestHandlerFactoryPtr createInterserverHTTPHandlerFactory(IServer & server, const std::string & name)
{
    auto factory = std::make_shared<HTTPRequestHandlerFactoryMain>(name);
    addCommonDefaultHandlersFactory(*factory, server);

    auto main_handler = std::make_shared<HandlingRuleHTTPHandlerFactory<InterserverIOHTTPHandler>>(server);
    main_handler->allowPostAndGetParamsAndOptionsRequest();
    factory->addHandler(main_handler);

    return factory;
}

HTTPRequestHandlerFactoryPtr createHandlerFactory(IServer & server, const Poco::Util::AbstractConfiguration & config, AsynchronousMetrics & async_metrics, const std::string & name)
{
    if (name == "HTTPHandler-factory" || name == "HTTPSHandler-factory")
        return createHTTPHandlerFactory(server, config, name, async_metrics);
    else if (name == "InterserverIOHTTPHandler-factory" || name == "InterserverIOHTTPSHandler-factory")
        return createInterserverHTTPHandlerFactory(server, name);
    else if (name == "PrometheusHandler-factory")
        return createPrometheusMainHandlerFactory(server, config, async_metrics, name);

    throw Exception(ErrorCodes::LOGICAL_ERROR, "LOGICAL ERROR: Unknown HTTP handler factory name.");
}

static const auto ping_response_expression = "Ok.\n";
static const auto root_response_expression = "config://http_server_default_response";

void addCommonDefaultHandlersFactory(HTTPRequestHandlerFactoryMain & factory, IServer & server)
{
    auto root_handler = std::make_shared<HandlingRuleHTTPHandlerFactory<StaticRequestHandler>>(server, root_response_expression);
    root_handler->attachStrictPath("/");
    root_handler->allowGetAndHeadRequest();
    factory.addHandler(root_handler);

    auto ping_handler = std::make_shared<HandlingRuleHTTPHandlerFactory<StaticRequestHandler>>(server, ping_response_expression);
    ping_handler->attachStrictPath("/ping");
    ping_handler->allowGetAndHeadRequest();
    factory.addPathToHints("/ping");
    factory.addHandler(ping_handler);

    auto replicas_status_handler = std::make_shared<HandlingRuleHTTPHandlerFactory<ReplicasStatusHandler>>(server);
    replicas_status_handler->attachNonStrictPath("/replicas_status");
    replicas_status_handler->allowGetAndHeadRequest();
    factory.addPathToHints("/replicas_status");
    factory.addHandler(replicas_status_handler);

    auto play_handler = std::make_shared<HandlingRuleHTTPHandlerFactory<WebUIRequestHandler>>(server);
    play_handler->attachNonStrictPath("/play");
    play_handler->allowGetAndHeadRequest();
    factory.addPathToHints("/play");
    factory.addHandler(play_handler);

    auto dashboard_handler = std::make_shared<HandlingRuleHTTPHandlerFactory<WebUIRequestHandler>>(server);
    dashboard_handler->attachNonStrictPath("/dashboard");
    dashboard_handler->allowGetAndHeadRequest();
    factory.addPathToHints("/dashboard");
    factory.addHandler(dashboard_handler);

    auto js_handler = std::make_shared<HandlingRuleHTTPHandlerFactory<WebUIRequestHandler>>(server);
    js_handler->attachNonStrictPath("/js/");
    js_handler->allowGetAndHeadRequest();
    factory.addHandler(js_handler);
}

void addDefaultHandlersFactory(
    HTTPRequestHandlerFactoryMain & factory,
    IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    AsynchronousMetrics & async_metrics)
{
    addCommonDefaultHandlersFactory(factory, server);

    auto query_handler = std::make_shared<HandlingRuleHTTPHandlerFactory<DynamicQueryHandler>>(server, "query");
    query_handler->allowPostAndGetParamsAndOptionsRequest();
    factory.addHandler(query_handler);

    /// We check that prometheus handler will be served on current (default) port.
    /// Otherwise it will be created separately, see createHandlerFactory(...).
    if (config.has("prometheus") && config.getInt("prometheus.port", 0) == 0)
    {
        auto prometheus_handler = std::make_shared<HandlingRuleHTTPHandlerFactory<PrometheusRequestHandler>>(
            server, PrometheusMetricsWriter(config, "prometheus", async_metrics));
        prometheus_handler->attachStrictPath(config.getString("prometheus.endpoint", "/metrics"));
        prometheus_handler->allowGetAndHeadRequest();
        factory.addHandler(prometheus_handler);
    }
}

}
