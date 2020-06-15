#include "HTTPHandlerFactory.h"

#include <Poco/Util/LayeredConfiguration.h>

#include "HTTPHandler.h"
#include "NotFoundHandler.h"
#include "StaticRequestHandler.h"
#include "ReplicasStatusHandler.h"
#include "InterserverIOHTTPHandler.h"
#include "PrometheusRequestHandler.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int INVALID_CONFIG_PARAMETER;
}

static void addCommonDefaultHandlersFactory(HTTPRequestHandlerFactoryMain & factory, IServer & server);
static void addDefaultHandlersFactory(HTTPRequestHandlerFactoryMain & factory, IServer & server, AsynchronousMetrics & async_metrics);

HTTPRequestHandlerFactoryMain::HTTPRequestHandlerFactoryMain(const std::string & name_)
    : log(&Poco::Logger::get(name_)), name(name_)
{
}

Poco::Net::HTTPRequestHandler * HTTPRequestHandlerFactoryMain::createRequestHandler(const Poco::Net::HTTPServerRequest & request)
{
    LOG_TRACE(log, "HTTP Request for {}. Method: {}, Address: {}, User-Agent: {}{}, Content Type: {}, Transfer Encoding: {}",
        name, request.getMethod(), request.clientAddress().toString(), request.has("User-Agent") ? request.get("User-Agent") : "none",
        (request.hasContentLength() ? (", Length: " + std::to_string(request.getContentLength())) : ("")),
        request.getContentType(), request.getTransferEncoding());

    for (auto & handler_factory : child_factories)
    {
        auto * handler = handler_factory->createRequestHandler(request);
        if (handler != nullptr)
            return handler;
    }

    if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_GET
        || request.getMethod() == Poco::Net::HTTPRequest::HTTP_HEAD
        || request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST)
    {
        return new NotFoundHandler;
    }

    return nullptr;
}

HTTPRequestHandlerFactoryMain::~HTTPRequestHandlerFactoryMain()
{
    while (!child_factories.empty())
    {
        delete child_factories.back();
        child_factories.pop_back();
    }
}

HTTPRequestHandlerFactoryMain::TThis * HTTPRequestHandlerFactoryMain::addHandler(Poco::Net::HTTPRequestHandlerFactory * child_factory)
{
    child_factories.emplace_back(child_factory);
    return this;
}

static inline auto createHandlersFactoryFromConfig(
    IServer & server, const std::string & name, const String & prefix, AsynchronousMetrics & async_metrics)
{
    auto main_handler_factory = std::make_unique<HTTPRequestHandlerFactoryMain>(name);

    Poco::Util::AbstractConfiguration::Keys keys;
    server.config().keys(prefix, keys);

    for (const auto & key : keys)
    {
        if (key == "defaults")
            addDefaultHandlersFactory(*main_handler_factory, server, async_metrics);
        else if (startsWith(key, "rule"))
        {
            const auto & handler_type = server.config().getString(prefix + "." + key + ".handler.type", "");

            if (handler_type.empty())
                throw Exception("Handler type in config is not specified here: " + prefix + "." + key + ".handler.type",
                    ErrorCodes::INVALID_CONFIG_PARAMETER);

            if (handler_type == "static")
                main_handler_factory->addHandler(createStaticHandlerFactory(server, prefix + "." + key));
            else if (handler_type == "dynamic_query_handler")
                main_handler_factory->addHandler(createDynamicHandlerFactory(server, prefix + "." + key));
            else if (handler_type == "predefined_query_handler")
                main_handler_factory->addHandler(createPredefinedHandlerFactory(server, prefix + "." + key));
            else if (handler_type == "prometheus")
                main_handler_factory->addHandler(createPrometheusHandlerFactory(server, async_metrics, prefix + "." + key));
            else if (handler_type == "replicas_status")
                main_handler_factory->addHandler(createReplicasStatusHandlerFactory(server, prefix + "." + key));
            else
                throw Exception("Unknown handler type '" + handler_type + "' in config here: " + prefix + "." + key + ".handler.type",
                    ErrorCodes::INVALID_CONFIG_PARAMETER);
        }
        else
            throw Exception("Unknown element in config: " + prefix + "." + key + ", must be 'rule' or 'defaults'",
                ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);
    }

    return main_handler_factory.release();
}

static inline Poco::Net::HTTPRequestHandlerFactory * createHTTPHandlerFactory(IServer & server, const std::string & name, AsynchronousMetrics & async_metrics)
{
    if (server.config().has("http_handlers"))
        return createHandlersFactoryFromConfig(server, name, "http_handlers", async_metrics);
    else
    {
        auto factory = std::make_unique<HTTPRequestHandlerFactoryMain>(name);
        addDefaultHandlersFactory(*factory, server, async_metrics);
        return factory.release();
    }
}

static inline Poco::Net::HTTPRequestHandlerFactory * createInterserverHTTPHandlerFactory(IServer & server, const std::string & name)
{
    auto factory = std::make_unique<HTTPRequestHandlerFactoryMain>(name);
    addCommonDefaultHandlersFactory(*factory, server);

    auto main_handler = std::make_unique<HandlingRuleHTTPHandlerFactory<InterserverIOHTTPHandler>>(server);
    main_handler->allowPostAndGetParamsRequest();
    factory->addHandler(main_handler.release());

    return factory.release();
}

Poco::Net::HTTPRequestHandlerFactory * createHandlerFactory(IServer & server, AsynchronousMetrics & async_metrics, const std::string & name)
{
    if (name == "HTTPHandler-factory" || name == "HTTPSHandler-factory")
        return createHTTPHandlerFactory(server, name, async_metrics);
    else if (name == "InterserverIOHTTPHandler-factory" || name == "InterserverIOHTTPSHandler-factory")
        return createInterserverHTTPHandlerFactory(server, name);
    else if (name == "PrometheusHandler-factory")
    {
        auto factory = std::make_unique<HTTPRequestHandlerFactoryMain>(name);
        auto handler = std::make_unique<HandlingRuleHTTPHandlerFactory<PrometheusRequestHandler>>(
            server, PrometheusMetricsWriter(server.config(), "prometheus", async_metrics));
        handler->attachStrictPath(server.config().getString("prometheus.endpoint", "/metrics"))->allowGetAndHeadRequest();
        factory->addHandler(handler.release());
        return factory.release();
    }

    throw Exception("LOGICAL ERROR: Unknown HTTP handler factory name.", ErrorCodes::LOGICAL_ERROR);
}

static const auto ping_response_expression = "Ok.\n";
static const auto root_response_expression = "config://http_server_default_response";

void addCommonDefaultHandlersFactory(HTTPRequestHandlerFactoryMain & factory, IServer & server)
{
    auto root_handler = std::make_unique<HandlingRuleHTTPHandlerFactory<StaticRequestHandler>>(server, root_response_expression);
    root_handler->attachStrictPath("/")->allowGetAndHeadRequest();
    factory.addHandler(root_handler.release());

    auto ping_handler = std::make_unique<HandlingRuleHTTPHandlerFactory<StaticRequestHandler>>(server, ping_response_expression);
    ping_handler->attachStrictPath("/ping")->allowGetAndHeadRequest();
    factory.addHandler(ping_handler.release());

    auto replicas_status_handler = std::make_unique<HandlingRuleHTTPHandlerFactory<ReplicasStatusHandler>>(server);
    replicas_status_handler->attachNonStrictPath("/replicas_status")->allowGetAndHeadRequest();
    factory.addHandler(replicas_status_handler.release());
}

void addDefaultHandlersFactory(HTTPRequestHandlerFactoryMain & factory, IServer & server, AsynchronousMetrics & async_metrics)
{
    addCommonDefaultHandlersFactory(factory, server);

    auto query_handler = std::make_unique<HandlingRuleHTTPHandlerFactory<DynamicQueryHandler>>(server, "query");
    query_handler->allowPostAndGetParamsRequest();
    factory.addHandler(query_handler.release());

    /// We check that prometheus handler will be served on current (default) port.
    /// Otherwise it will be created separately, see createHandlerFactory(...).
    if (server.config().has("prometheus") && server.config().getInt("prometheus.port", 0) == 0)
    {
        auto prometheus_handler = std::make_unique<HandlingRuleHTTPHandlerFactory<PrometheusRequestHandler>>(
            server, PrometheusMetricsWriter(server.config(), "prometheus", async_metrics));
        prometheus_handler->attachStrictPath(server.config().getString("prometheus.endpoint", "/metrics"))->allowGetAndHeadRequest();
        factory.addHandler(prometheus_handler.release());
    }
}

}
