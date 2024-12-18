#include <Server/KeeperReadinessHandler.h>

#if USE_NURAFT

#include <memory>

#include <IO/HTTPCommon.h>
#include <IO/Operators.h>
#include <Coordination/KeeperDispatcher.h>
#include <Server/HTTPHandlerFactory.h>
#include <Server/HTTPHandlerRequestFilter.h>
#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>

namespace DB
{

void KeeperReadinessHandler::handleRequest(HTTPServerRequest & /*request*/, HTTPServerResponse & response, const ProfileEvents::Event & /*write_event*/)
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

        std::ostringstream oss;     // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        oss.exceptions(std::ios::failbit);
        Poco::JSON::Stringifier::stringify(json, oss);

        if (!status)
            response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_SERVICE_UNAVAILABLE);

        *response.send() << oss.str();
    }
    catch (...)
    {
        tryLogCurrentException("KeeperReadinessHandler");

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
            LOG_ERROR((getLogger("KeeperReadinessHandler")), "Cannot send exception to client");
        }
    }
}

HTTPRequestHandlerFactoryPtr createKeeperHTTPControlMainHandlerFactory(
    const Poco::Util::AbstractConfiguration & config,
    std::shared_ptr<KeeperDispatcher> keeper_dispatcher,
    const std::string & name)
{
    auto factory = std::make_shared<HTTPRequestHandlerFactoryMain>(name);
    using Factory = HandlingRuleHTTPHandlerFactory<KeeperReadinessHandler>;
    Factory::Creator creator = [keeper_dispatcher]() -> std::unique_ptr<KeeperReadinessHandler>
    {
        return std::make_unique<KeeperReadinessHandler>(keeper_dispatcher);
    };

    auto readiness_handler = std::make_shared<Factory>(std::move(creator));

    readiness_handler->attachStrictPath(config.getString("keeper_server.http_control.readiness.endpoint", "/ready"));
    readiness_handler->allowGetAndHeadRequest();
    factory->addHandler(readiness_handler);

    return factory;
}

}

#endif
