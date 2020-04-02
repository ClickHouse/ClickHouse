#pragma once

#include "IServer.h"

#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Common/StringUtils/StringUtils.h>

namespace DB
{

/// Handler for Web Terminal HTTP request.
class HTTPWebTerminalHandler : public Poco::Net::HTTPRequestHandler
{
public:
    HTTPWebTerminalHandler(IServer & server_);

    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override;

private:
    Context context;
    std::chrono::steady_clock::duration session_timeout;
};

class HTTPWebConsoleRequestHandlerFactory : public Poco::Net::HTTPRequestHandlerFactory
{
public:
    HTTPWebConsoleRequestHandlerFactory(IServer & server_) : server(server_) {}

    Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest & request) override
    {
        const auto & method = request.getMethod();

        if ((method == Poco::Net::HTTPRequest::HTTP_GET || method == Poco::Net::HTTPRequest::HTTP_POST) &&
            startsWith(request.getURI(), "/console"))
            return new HTTPWebTerminalHandler(server);

        return nullptr;
    }

private:
    IServer & server;
};

}
