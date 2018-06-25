#pragma once

#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <common/logger_useful.h>
#include "IServer.h"
#include "HTTPHandler.h"
#include "InterserverIOHTTPHandler.h"
#include "NotFoundHandler.h"
#include "PingRequestHandler.h"
#include "ReplicasStatusHandler.h"
#include "RootRequestHandler.h"


namespace DB
{

template <typename HandlerType>
class HTTPRequestHandlerFactory : public Poco::Net::HTTPRequestHandlerFactory
{
private:
    IServer & server;
    Logger * log;
    std::string name;

public:
    HTTPRequestHandlerFactory(IServer & server_, const std::string & name_) : server(server_), log(&Logger::get(name_)), name(name_)
    {
    }

    Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest & request) override
    {
        LOG_TRACE(log,
            "HTTP Request for " << name << ". "
                                << "Method: "
                                << request.getMethod()
                                << ", Address: "
                                << request.clientAddress().toString()
                                << ", User-Agent: "
                                << (request.has("User-Agent") ? request.get("User-Agent") : "none"));

        const auto & uri = request.getURI();

        if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_GET || request.getMethod() == Poco::Net::HTTPRequest::HTTP_HEAD)
        {
            if (uri == "/")
                return new RootRequestHandler(server);
            if (uri == "/ping")
                return new PingRequestHandler(server);
            else if (startsWith(uri, "/replicas_status"))
                return new ReplicasStatusHandler(server.context());
        }

        if (uri.find('?') != std::string::npos || request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST)
        {
            return new HandlerType(server);
        }

        if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_GET || request.getMethod() == Poco::Net::HTTPRequest::HTTP_HEAD
            || request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST)
        {
            return new NotFoundHandler;
        }

        return nullptr;
    }
};

using HTTPHandlerFactory = HTTPRequestHandlerFactory<HTTPHandler>;
using InterserverIOHTTPHandlerFactory = HTTPRequestHandlerFactory<InterserverIOHTTPHandler>;

}
