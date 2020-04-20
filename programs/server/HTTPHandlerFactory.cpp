#include "HTTPHandlerFactory.h"

#include <re2/re2.h>
#include <re2/stringpiece.h>
#include <common/find_symbols.h>
#include <Poco/StringTokenizer.h>

#include "HTTPHandler.h"
#include "NotFoundHandler.h"
#include "StaticRequestHandler.h"
#include "ReplicasStatusHandler.h"
#include "InterserverIOHTTPHandler.h"

#if USE_RE2_ST
#include <re2_st/re2.h>
#else
#define re2_st re2
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
}

HTTPRequestHandlerFactoryMain::HTTPRequestHandlerFactoryMain(const std::string & name_)
    : log(&Logger::get(name_)), name(name_)
{
}

Poco::Net::HTTPRequestHandler * HTTPRequestHandlerFactoryMain::createRequestHandler(const Poco::Net::HTTPServerRequest & request) // override
{
    LOG_TRACE(log, "HTTP Request for " << name << ". "
        << "Method: "
        << request.getMethod()
        << ", Address: "
        << request.clientAddress().toString()
        << ", User-Agent: "
        << (request.has("User-Agent") ? request.get("User-Agent") : "none")
        << (request.hasContentLength() ? (", Length: " + std::to_string(request.getContentLength())) : (""))
        << ", Content Type: " << request.getContentType()
        << ", Transfer Encoding: " << request.getTransferEncoding());

    for (auto & handler_factory : child_handler_factories)
    {
        auto handler = handler_factory->createRequestHandler(request);
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
    while (!child_handler_factories.empty())
        delete child_handler_factories.back(), child_handler_factories.pop_back();
}

HTTPRequestHandlerFactoryMain::TThis * HTTPRequestHandlerFactoryMain::addHandler(Poco::Net::HTTPRequestHandlerFactory * child_factory)
{
    child_handler_factories.emplace_back(child_factory);
    return this;
}

static inline auto createHandlersFactoryFromConfig(IServer & server, const std::string & name, const String & prefix)
{
    auto main_handler_factory = new HTTPRequestHandlerFactoryMain(name);

    try
    {
        Poco::Util::AbstractConfiguration::Keys keys;
        server.config().keys(prefix, keys);

        for (const auto & key : keys)
        {
            if (!startsWith(key, "routing_rule"))
                throw Exception("Unknown element in config: " + prefix + "." + key + ", must be 'routing_rule'", ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);

            const auto & handler_type = server.config().getString(prefix + "." + key + ".handler.type", "");

            if (handler_type == "static")
                main_handler_factory->addHandler(createStaticHandlerFactory(server, prefix));
            else if (handler_type == "dynamic_query_handler")
                main_handler_factory->addHandler(createDynamicHandlerFactory(server, prefix));
            else if (handler_type == "predefine_query_handler")
                main_handler_factory->addHandler(createPredefineHandlerFactory(server, prefix));
            else
                throw Exception("Unknown element in config: " + prefix + "." + key + ", must be 'routing_rule'", ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);
        }

        return main_handler_factory;
    }
    catch (...)
    {
        delete main_handler_factory;
        throw;
    }
}

static const auto ping_response_expression = "Ok.\n";
static const auto root_response_expression = "config://http_server_default_response";

static inline Poco::Net::HTTPRequestHandlerFactory * createHTTPHandlerFactory(IServer & server, const std::string & name)
{
    if (server.config().has("routing_rules"))
        return createHandlersFactoryFromConfig(server, name, "routing_rules");
    else
    {
        return (new HTTPRequestHandlerFactoryMain(name))
            ->addHandler((new RoutingRuleHTTPHandlerFactory<StaticRequestHandler>(server, root_response_expression))
                ->attachStrictPath("/")->allowGetAndHeadRequest())
            ->addHandler((new RoutingRuleHTTPHandlerFactory<StaticRequestHandler>(server, ping_response_expression))
                ->attachStrictPath("/ping")->allowGetAndHeadRequest())
            ->addHandler((new RoutingRuleHTTPHandlerFactory<ReplicasStatusHandler>(server))
                ->attachNonStrictPath("/replicas_status")->allowGetAndHeadRequest())
            ->addHandler((new RoutingRuleHTTPHandlerFactory<DynamicQueryHandler>(server, "query"))->allowPostAndGetParamsRequest());
        /// TODO:
//    if (configuration.has("prometheus") && configuration.getInt("prometheus.port", 0) == 0)
//        handler_factory->addHandler<PrometheusHandlerFactory>(async_metrics);
    }
}

static inline Poco::Net::HTTPRequestHandlerFactory * createInterserverHTTPHandlerFactory(IServer & server, const std::string & name)
{
    return (new HTTPRequestHandlerFactoryMain(name))
        ->addHandler((new RoutingRuleHTTPHandlerFactory<StaticRequestHandler>(server, root_response_expression))
            ->attachStrictPath("/")->allowGetAndHeadRequest())
        ->addHandler((new RoutingRuleHTTPHandlerFactory<StaticRequestHandler>(server, ping_response_expression))
            ->attachStrictPath("/ping")->allowGetAndHeadRequest())
        ->addHandler((new RoutingRuleHTTPHandlerFactory<ReplicasStatusHandler>(server))
            ->attachNonStrictPath("/replicas_status")->allowGetAndHeadRequest())
        ->addHandler((new RoutingRuleHTTPHandlerFactory<InterserverIOHTTPHandler>(server))->allowPostAndGetParamsRequest());
}

Poco::Net::HTTPRequestHandlerFactory * createHandlerFactory(IServer & server, const std::string & name)
{
    if (name == "HTTPHandler-factory" || name == "HTTPSHandler-factory")
        return createHTTPHandlerFactory(server, name);
    else if (name == "InterserverIOHTTPHandler-factory" || name == "InterserverIOHTTPSHandler-factory")
        return createInterserverHTTPHandlerFactory(server, name);

    throw Exception("LOGICAL ERROR: Unknown HTTP handler factory name.", ErrorCodes::LOGICAL_ERROR);
}

}