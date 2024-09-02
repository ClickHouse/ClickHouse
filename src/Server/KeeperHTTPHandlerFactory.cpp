#include <Server/KeeperHTTPHandlerFactory.h>

#if USE_NURAFT

#include <memory>

#include <Coordination/FourLetterCommand.h>
#include <Coordination/KeeperDispatcher.h>
#include <IO/HTTPCommon.h>
#include <IO/Operators.h>
#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>
#include <Server/HTTPHandlerFactory.h>
#include <Server/HTTPHandlerRequestFilter.h>
#include <Server/IServer.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>

#include <Server/KeeperDashboardRequestHandler.h>
#include <Server/KeeperHTTPStorageHandler.h>
#include <Server/KeeperNotFoundHandler.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
}

KeeperHTTPRequestHandlerFactory::KeeperHTTPRequestHandlerFactory(const std::string & name_) : log(getLogger(name_)), name(name_)
{
}

std::unique_ptr<HTTPRequestHandler> KeeperHTTPRequestHandlerFactory::createRequestHandler(const HTTPServerRequest & request)
{
    LOG_TRACE(log, "HTTP Request for {}. {}", name, request.toStringForLogging());

    for (auto & handler_factory : child_factories)
    {
        auto handler = handler_factory->createRequestHandler(request);
        if (handler)
            return handler;
    }

    if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_GET || request.getMethod() == Poco::Net::HTTPRequest::HTTP_HEAD
        || request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST)
    {
        return std::unique_ptr<HTTPRequestHandler>(new KeeperNotFoundHandler(hints.getHints(request.getURI())));
    }

    return nullptr;
}

void addDashboardHandlersToFactory(
    KeeperHTTPRequestHandlerFactory & factory, std::shared_ptr<KeeperDispatcher> keeper_dispatcher)
{
    auto dashboard_ui_creator = []() -> std::unique_ptr<KeeperDashboardWebUIRequestHandler>
    { return std::make_unique<KeeperDashboardWebUIRequestHandler>(); };

    auto dashboard_handler = std::make_shared<HandlingRuleHTTPHandlerFactory<KeeperDashboardWebUIRequestHandler>>(dashboard_ui_creator);
    dashboard_handler->attachStrictPath("/dashboard");
    dashboard_handler->allowGetAndHeadRequest();
    factory.addPathToHints("/dashboard");
    factory.addHandler(dashboard_handler);

    auto dashboard_content_creator = [keeper_dispatcher]() -> std::unique_ptr<KeeperDashboardContentRequestHandler>
    { return std::make_unique<KeeperDashboardContentRequestHandler>(keeper_dispatcher); };

    auto dashboard_content_handler
        = std::make_shared<HandlingRuleHTTPHandlerFactory<KeeperDashboardContentRequestHandler>>(dashboard_content_creator);
    dashboard_content_handler->attachStrictPath("/dashboard/content");
    dashboard_content_handler->allowGetAndHeadRequest();
    factory.addHandler(dashboard_content_handler);
}

void addReadinessHandlerToFactory(
    KeeperHTTPRequestHandlerFactory & factory,
    std::shared_ptr<KeeperDispatcher> keeper_dispatcher,
    const Poco::Util::AbstractConfiguration & config)
{
    auto creator = [keeper_dispatcher]() -> std::unique_ptr<KeeperHTTPReadinessHandler>
    { return std::make_unique<KeeperHTTPReadinessHandler>(keeper_dispatcher); };
    auto readiness_handler = std::make_shared<HandlingRuleHTTPHandlerFactory<KeeperHTTPReadinessHandler>>(std::move(creator));
    readiness_handler->attachStrictPath(config.getString("keeper_server.http_control.readiness.endpoint", "/ready"));
    readiness_handler->allowGetAndHeadRequest();
    factory.addPathToHints("/ready");
    factory.addHandler(readiness_handler);
}

void addCommandsHandlersToFactory(KeeperHTTPRequestHandlerFactory & factory, std::shared_ptr<KeeperDispatcher> keeper_dispatcher)
{
    auto creator = [keeper_dispatcher]() -> std::unique_ptr<KeeperHTTPCommandsHandler>
    { return std::make_unique<KeeperHTTPCommandsHandler>(keeper_dispatcher); };

    auto commads_handler = std::make_shared<HandlingRuleHTTPHandlerFactory<KeeperHTTPCommandsHandler>>(std::move(creator));
    commads_handler->attachNonStrictPath("/api/v1/commands");
    commads_handler->allowGetHeadAndPostRequest();

    factory.addPathToHints("/api/v1/commands");
    factory.addHandler(commads_handler);
}

void addStorageHandlersToFactory(
    KeeperHTTPRequestHandlerFactory & factory, const IServer & server, std::shared_ptr<KeeperDispatcher> keeper_dispatcher)
{
    auto creator = [&server, keeper_dispatcher]() -> std::unique_ptr<KeeperHTTPStorageHandler>
    { return std::make_unique<KeeperHTTPStorageHandler>(server, keeper_dispatcher); };

    auto storage_handler = std::make_shared<HandlingRuleHTTPHandlerFactory<KeeperHTTPStorageHandler>>(std::move(creator));
    storage_handler->attachNonStrictPath("/api/v1/storage");
    storage_handler->allowGetHeadAndPostRequest();

    factory.addPathToHints("/api/v1/storage");
    factory.addHandler(storage_handler);
}

void addDefaultHandlersToFactory(
    KeeperHTTPRequestHandlerFactory & factory,
    const IServer & server,
    std::shared_ptr<KeeperDispatcher> keeper_dispatcher,
    const Poco::Util::AbstractConfiguration & config)
{
    addReadinessHandlerToFactory(factory, keeper_dispatcher, config);
    addDashboardHandlersToFactory(factory, keeper_dispatcher);
    addCommandsHandlersToFactory(factory, keeper_dispatcher);
    addStorageHandlersToFactory(factory, server, keeper_dispatcher);
}

static inline auto createHandlersFactoryFromConfig(
    const IServer & server,
    std::shared_ptr<KeeperDispatcher> keeper_dispatcher,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & name,
    const String & prefix)
{
    auto main_handler_factory = std::make_shared<KeeperHTTPRequestHandlerFactory>(name);

    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(prefix, keys);

    for (const auto & key : keys)
    {
        if (key == "defaults")
        {
            addDefaultHandlersToFactory(*main_handler_factory, server, keeper_dispatcher, config);
        }
        else if (startsWith(key, "rule"))
        {
            const auto & handler_type = config.getString(prefix + "." + key + ".handler.type", "");

            if (handler_type.empty())
                throw Exception(
                    ErrorCodes::INVALID_CONFIG_PARAMETER,
                    "Handler type in config is not specified here: "
                    "{}.{}.handler.type",
                    prefix,
                    key);
            else if (handler_type == "ready")
                addReadinessHandlerToFactory(*main_handler_factory, keeper_dispatcher, config);
            else if (handler_type == "dashboard")
                addDashboardHandlersToFactory(*main_handler_factory, keeper_dispatcher);
            else if (handler_type == "commands")
                addCommandsHandlersToFactory(*main_handler_factory, keeper_dispatcher);
            else if (handler_type == "storage")
                addStorageHandlersToFactory(*main_handler_factory, server, keeper_dispatcher);
            else
                throw Exception(
                    ErrorCodes::INVALID_CONFIG_PARAMETER,
                    "Unknown handler type '{}' in config here: {}.{}.handler.type",
                    handler_type,
                    prefix,
                    key);
        }
        else
            throw Exception(
                ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG,
                "Unknown element in config: "
                "{}.{}, must be 'rule' or 'defaults'",
                prefix,
                key);
    }

    return main_handler_factory;
}

KeeperHTTPReadinessHandler::KeeperHTTPReadinessHandler(std::shared_ptr<KeeperDispatcher> keeper_dispatcher_)
    : log(getLogger("KeeperHTTPReadinessHandler")), keeper_dispatcher(keeper_dispatcher_)
{
}

void KeeperHTTPReadinessHandler::handleRequest(
    HTTPServerRequest & /*request*/, HTTPServerResponse & response, const ProfileEvents::Event & /*write_event*/)
{
    try
    {
        auto is_leader = keeper_dispatcher->isLeader();
        auto is_follower = keeper_dispatcher->isFollower() && keeper_dispatcher->hasLeader();
        auto is_observer = keeper_dispatcher->isObserver() && keeper_dispatcher->hasLeader();

        auto data = keeper_dispatcher->getKeeper4LWInfo();

        auto status = is_leader || is_follower || is_observer;

        Poco::JSON::Object json, details;

        details.set("role", data.getRole());
        details.set("hasLeader", keeper_dispatcher->hasLeader());
        json.set("details", details);
        json.set("status", status ? "ok" : "fail");

        std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        oss.exceptions(std::ios::failbit);
        Poco::JSON::Stringifier::stringify(json, oss);

        if (!status)
            response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_SERVICE_UNAVAILABLE);

        *response.send() << oss.str();
    }
    catch (...)
    {
        tryLogCurrentException(log);

        try
        {
            response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);

            if (!response.sent())
            {
                /// We have not sent anything yet and we don't even know if we need to compress response.
                *response.send() << getCurrentExceptionMessage(false) << '\n';
            }
        }
        catch (...)
        {
            LOG_ERROR(log, "Cannot send exception to client");
        }
    }
}

KeeperHTTPCommandsHandler::KeeperHTTPCommandsHandler(std::shared_ptr<KeeperDispatcher> keeper_dispatcher_)
    : log(getLogger("KeeperHTTPCommandsHandler")), keeper_dispatcher(keeper_dispatcher_)
{
}

void KeeperHTTPCommandsHandler::handleRequest(
    HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & /*write_event*/)
try
{
    std::vector<std::string> uri_segments;
    try
    {
        Poco::URI uri(request.getURI());
        uri.getPathSegments(uri_segments);
    }
    catch (...)
    {
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_BAD_REQUEST, "Could not parse request path.");
        *response.send() << "Could not parse request path.\n";
        return;
    }

    /// non-strict path "/api/v1/commands" filter is already attached
    if (uri_segments.size() != 4)
    {
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_BAD_REQUEST, "Invalid command path");
        *response.send() << "Invalid command path\n";
        return;
    }
    const auto command = uri_segments[3];

    setResponseDefaultHeaders(response);

    Poco::JSON::Object response_json;
    response.setContentType("application/json");

    if (!FourLetterCommandFactory::instance().isKnown(DB::IFourLetterCommand::toCode(command)))
    {
        LOG_INFO(log, "Invalid four letter command: {}", command);
        response_json.set("message", "Invalid four letter command.");
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_BAD_REQUEST);
    }
    else if (!FourLetterCommandFactory::instance().isEnabled(DB::IFourLetterCommand::toCode(command)))
    {
        LOG_INFO(log, "Not enabled four letter command: {}", command);
        response_json.set("message", "Command is disabled. Check server settings.");
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_FORBIDDEN);
    }
    else
    {
        auto command_ptr = FourLetterCommandFactory::instance().get(DB::IFourLetterCommand::toCode(command));
        LOG_DEBUG(log, "Received four letter command {}", command_ptr->name());

        try
        {
            String res = command_ptr->run();
            response_json.set("result", res);
            response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
        }
        catch (...)
        {
            tryLogCurrentException(log, "Error when executing four letter command " + command_ptr->name());
            response_json.set("message", "Internal server error.");
            response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
        }
    }

    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(response_json, oss);

    *response.send() << oss.str();
}
catch (...)
{
    tryLogCurrentException(log);

    try
    {
        response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);

        if (!response.sent())
        {
            /// We have not sent anything yet and we don't even know if we need to compress response.
            *response.send() << getCurrentExceptionMessage(false) << '\n';
        }
    }
    catch (...)
    {
        LOG_ERROR(log, "Cannot send exception to client");
    }
}

HTTPRequestHandlerFactoryPtr createKeeperHTTPHandlerFactory(
    const IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    std::shared_ptr<KeeperDispatcher> keeper_dispatcher,
    const std::string & name)
{
    if (config.has("keeper_server.http_control.handlers"))
        return createHandlersFactoryFromConfig(server, keeper_dispatcher, config, name, "keeper_server.http_control.handlers");

    auto factory = std::make_shared<KeeperHTTPRequestHandlerFactory>(name);
    addDefaultHandlersToFactory(*factory, server, keeper_dispatcher, config);
    return factory;
}

}
#endif
