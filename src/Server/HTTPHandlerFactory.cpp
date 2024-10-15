#include <Server/HTTPHandlerFactory.h>

#include <Server/HTTP/HTTPRequestHandler.h>
#include <Server/PrometheusMetricsWriter.h>
#include <Server/PrometheusRequestHandlerFactory.h>
#include <Server/IServer.h>

#include <Poco/Util/AbstractConfiguration.h>

#include "HTTPHandler.h"
#include "StaticRequestHandler.h"
#include "ReplicasStatusHandler.h"
#include "InterserverIOHTTPHandler.h"
#include "WebUIRequestHandler.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int INVALID_CONFIG_PARAMETER;
}

namespace
{

class RedirectRequestHandler : public HTTPRequestHandler
{
private:
    std::string url;

public:
    explicit RedirectRequestHandler(std::string url_)
        : url(std::move(url_))
    {
    }

    void handleRequest(HTTPServerRequest &, HTTPServerResponse & response, const ProfileEvents::Event &) override
    {
        response.redirect(url);
    }
};

HTTPRequestHandlerFactoryPtr createRedirectHandlerFactory(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix)
{
    std::string url = config.getString(config_prefix + ".handler.location");

    auto factory = std::make_shared<HandlingRuleHTTPHandlerFactory<RedirectRequestHandler>>(
        [my_url = std::move(url)]() { return std::make_unique<RedirectRequestHandler>(my_url); });

    factory->addFiltersFromConfig(config, config_prefix);
    return factory;
}

}


static void addCommonDefaultHandlersFactory(HTTPRequestHandlerFactoryMain & factory, IServer & server);
static void addDefaultHandlersFactory(
    HTTPRequestHandlerFactoryMain & factory,
    IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    AsynchronousMetrics & async_metrics);

static auto createPingHandlerFactory(IServer & server)
{
    auto creator = [&server]() -> std::unique_ptr<StaticRequestHandler>
    {
        constexpr auto ping_response_expression = "Ok.\n";
        return std::make_unique<StaticRequestHandler>(
            server, ping_response_expression, parseHTTPResponseHeaders("text/html; charset=UTF-8"));
    };
    return std::make_shared<HandlingRuleHTTPHandlerFactory<StaticRequestHandler>>(std::move(creator));
}

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
            {
                main_handler_factory->addHandler(createStaticHandlerFactory(server, config, prefix + "." + key));
            }
            else if (handler_type == "redirect")
            {
                main_handler_factory->addHandler(createRedirectHandlerFactory(config, prefix + "." + key));
            }
            else if (handler_type == "dynamic_query_handler")
            {
                main_handler_factory->addHandler(createDynamicHandlerFactory(server, config, prefix + "." + key));
            }
            else if (handler_type == "predefined_query_handler")
            {
                main_handler_factory->addHandler(createPredefinedHandlerFactory(server, config, prefix + "." + key));
            }
            else if (handler_type == "prometheus")
            {
                main_handler_factory->addHandler(
                    createPrometheusHandlerFactoryForHTTPRule(server, config, prefix + "." + key, async_metrics));
            }
            else if (handler_type == "replicas_status")
            {
                main_handler_factory->addHandler(createReplicasStatusHandlerFactory(server, config, prefix + "." + key));
            }
            else if (handler_type == "ping")
            {
                auto handler = createPingHandlerFactory(server);
                handler->addFiltersFromConfig(config, prefix + "." + key);
                main_handler_factory->addHandler(std::move(handler));
            }
            else if (handler_type == "play")
            {
                auto handler = std::make_shared<HandlingRuleHTTPHandlerFactory<PlayWebUIRequestHandler>>(server);
                handler->addFiltersFromConfig(config, prefix + "." + key);
                main_handler_factory->addHandler(std::move(handler));
            }
            else if (handler_type == "dashboard")
            {
                auto handler = std::make_shared<HandlingRuleHTTPHandlerFactory<DashboardWebUIRequestHandler>>(server);
                handler->addFiltersFromConfig(config, prefix + "." + key);
                main_handler_factory->addHandler(std::move(handler));
            }
            else if (handler_type == "binary")
            {
                auto handler = std::make_shared<HandlingRuleHTTPHandlerFactory<BinaryWebUIRequestHandler>>(server);
                handler->addFiltersFromConfig(config, prefix + "." + key);
                main_handler_factory->addHandler(std::move(handler));
            }
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

    auto factory = std::make_shared<HTTPRequestHandlerFactoryMain>(name);
    addDefaultHandlersFactory(*factory, server, config, async_metrics);
    return factory;
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
    if (name == "InterserverIOHTTPHandler-factory" || name == "InterserverIOHTTPSHandler-factory")
        return createInterserverHTTPHandlerFactory(server, name);
    if (name == "PrometheusHandler-factory")
        return createPrometheusHandlerFactory(server, config, async_metrics, name);

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown HTTP handler factory name.");
}


void addCommonDefaultHandlersFactory(HTTPRequestHandlerFactoryMain & factory, IServer & server)
{
    auto root_creator = [&server]() -> std::unique_ptr<StaticRequestHandler>
    {
        constexpr auto root_response_expression = "config://http_server_default_response";
        return std::make_unique<StaticRequestHandler>(
            server, root_response_expression, parseHTTPResponseHeaders("text/html; charset=UTF-8"));
    };
    auto root_handler = std::make_shared<HandlingRuleHTTPHandlerFactory<StaticRequestHandler>>(std::move(root_creator));
    root_handler->attachStrictPath("/");
    root_handler->allowGetAndHeadRequest();
    factory.addHandler(root_handler);

    auto ping_handler = createPingHandlerFactory(server);
    ping_handler->attachStrictPath("/ping");
    ping_handler->allowGetAndHeadRequest();
    factory.addPathToHints("/ping");
    factory.addHandler(ping_handler);

    auto replicas_status_handler = std::make_shared<HandlingRuleHTTPHandlerFactory<ReplicasStatusHandler>>(server);
    replicas_status_handler->attachNonStrictPath("/replicas_status");
    replicas_status_handler->allowGetAndHeadRequest();
    factory.addPathToHints("/replicas_status");
    factory.addHandler(replicas_status_handler);

    auto play_handler = std::make_shared<HandlingRuleHTTPHandlerFactory<PlayWebUIRequestHandler>>(server);
    play_handler->attachNonStrictPath("/play");
    play_handler->allowGetAndHeadRequest();
    factory.addPathToHints("/play");
    factory.addHandler(play_handler);

    auto dashboard_handler = std::make_shared<HandlingRuleHTTPHandlerFactory<DashboardWebUIRequestHandler>>(server);
    dashboard_handler->attachNonStrictPath("/dashboard");
    dashboard_handler->allowGetAndHeadRequest();
    factory.addPathToHints("/dashboard");
    factory.addHandler(dashboard_handler);

    auto binary_handler = std::make_shared<HandlingRuleHTTPHandlerFactory<BinaryWebUIRequestHandler>>(server);
    binary_handler->attachNonStrictPath("/binary");
    binary_handler->allowGetAndHeadRequest();
    factory.addPathToHints("/binary");
    factory.addHandler(binary_handler);

    auto js_handler = std::make_shared<HandlingRuleHTTPHandlerFactory<JavaScriptWebUIRequestHandler>>(server);
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

    auto dynamic_creator = [&server] () -> std::unique_ptr<DynamicQueryHandler>
    {
        return std::make_unique<DynamicQueryHandler>(server, "query");
    };
    auto query_handler = std::make_shared<HandlingRuleHTTPHandlerFactory<DynamicQueryHandler>>(std::move(dynamic_creator));
    query_handler->addFilter([](const auto & request)
        {
            bool path_matches_get_or_head = startsWith(request.getURI(), "?")
                            || startsWith(request.getURI(), "/?")
                            || startsWith(request.getURI(), "/query?");
            bool is_get_or_head_request = request.getMethod() == Poco::Net::HTTPRequest::HTTP_GET
                            || request.getMethod() == Poco::Net::HTTPRequest::HTTP_HEAD;

            bool path_matches_post_or_options = path_matches_get_or_head
                             || request.getURI() == "/"
                             || request.getURI().empty();
            bool is_post_or_options_request = request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST
                                    || request.getMethod() == Poco::Net::HTTPRequest::HTTP_OPTIONS;

            return (path_matches_get_or_head && is_get_or_head_request) || (path_matches_post_or_options && is_post_or_options_request);
        }
    );
    factory.addHandler(query_handler);

    /// createPrometheusHandlerFactoryForHTTPRuleDefaults() can return nullptr if prometheus protocols must not be served on http port.
    if (auto prometheus_handler = createPrometheusHandlerFactoryForHTTPRuleDefaults(server, config, async_metrics))
        factory.addHandler(prometheus_handler);
}

}
