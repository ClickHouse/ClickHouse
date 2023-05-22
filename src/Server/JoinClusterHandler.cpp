#include <string>
#include <Server/JoinClusterHandler.h>

#include <IO/HTTPCommon.h>
#include <Server/HTTPHandlerFactory.h>
#include <Server/IServer.h>

#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/JSON/Parser.h>

namespace DB
{

void JoinClusterHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response)
{
    try
    {
        const auto keep_alive_timeout = server.config().getUInt("keep_alive_timeout", 10);
        setResponseDefaultHeaders(response, keep_alive_timeout);

        auto & stream = request.getStream();

        char buf[1024];
        auto bytes_read = stream.read(buf, 1024);

        std::string json(buf, bytes_read);

        Poco::JSON::Parser parser;
        auto ret = parser.parse(json).extract<Poco::JSON::Object>();
        auto server_id = ret.get("server_id").extract<Int32>();
        auto keeper_endpoint = ret.get("server_keeper_endpoint").extract<std::string>();

        AddToClusterAction add_server_action{.server = std::make_shared<nuraft::srv_config>(server_id, keeper_endpoint)};

        keeper_dispatcher->addServerToCluster(add_server_action);

        const char * data = "Ok.\n";
        response.sendBuffer(data, strlen(data));
    }
    catch (...)
    {
        tryLogCurrentException("JoinClusterHandler");

        try
        {
            response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);

            if (!response.sent())
            {
                /// We have not sent anything yet and we don't even know if we need to compress response.
                *response.send() << getCurrentExceptionMessage(false) << std::endl;
            }
        }
        catch (...)
        {
            LOG_ERROR((&Poco::Logger::get("JoinClusterHandler")), "Cannot send exception to client");
        }
    }
}

HTTPRequestHandlerFactoryPtr
createJoinClusterMainHandlerFactory(IServer & server, std::shared_ptr<KeeperDispatcher> & keeper_dispatcher, const std::string & name)
{
    auto factory = std::make_shared<HTTPRequestHandlerFactoryMain>(name);
    auto handler = std::make_shared<HandlingRuleHTTPHandlerFactory<JoinClusterHandler>>(
        server, keeper_dispatcher);
    handler->attachNonStrictPath("/join_cluster");
    handler->allowPostAndGetParamsAndOptionsRequest();
    factory->addHandler(handler);
    return factory;
}

}
