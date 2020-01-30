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
#include "PrometheusRequestHandler.h"
#include "ReplicasStatusHandler.h"
#include "RootRequestHandler.h"


namespace DB
{

/// Handle request using child handlers
class HTTPRequestHandlerFactoryMain : public Poco::Net::HTTPRequestHandlerFactory
{
private:
    using TThis = HTTPRequestHandlerFactoryMain;

    IServer & server;
    Logger * log;
    std::string name;

    std::vector<std::unique_ptr<Poco::Net::HTTPRequestHandlerFactory>> child_handler_factories;

public:
    HTTPRequestHandlerFactoryMain(IServer & server_, const std::string & name_);

    Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest & request) override;

    template <typename T, typename... TArgs>
    TThis * addHandler(TArgs &&... args)
    {
        child_handler_factories.emplace_back(std::make_unique<T>(server, std::forward<TArgs>(args)...));
        return this;
    }
};


/// Handle POST or GET with params
template <typename HandleType>
class HTTPQueryRequestHandlerFactory : public Poco::Net::HTTPRequestHandlerFactory
{
private:
    IServer & server;

public:
    HTTPQueryRequestHandlerFactory(IServer & server_) : server(server_) {}

    Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest & request) override
    {
        if (request.getURI().find('?') != std::string::npos || request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST)
            return new HandleType(server);
        return nullptr;
    }
};


/// Handle GET or HEAD endpoint on specified path
template <typename TGetEndpoint>
class HTTPGetRequestHandlerFactory : public Poco::Net::HTTPRequestHandlerFactory
{
private:
    IServer & server;
public:
    HTTPGetRequestHandlerFactory(IServer & server_) : server(server_) {}

    Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest & request) override
    {
        auto & method = request.getMethod();
        if (!(method == Poco::Net::HTTPRequest::HTTP_GET || method == Poco::Net::HTTPRequest::HTTP_HEAD))
            return nullptr;

        auto & uri = request.getURI();
        bool uri_match = TGetEndpoint::strict_path ? uri == TGetEndpoint::path : startsWith(uri, TGetEndpoint::path);
        if (uri_match)
            return new typename TGetEndpoint::HandleType(server);

        return nullptr;
    }
};


struct RootEndpoint
{
    static constexpr auto path = "/";
    static constexpr auto strict_path = true;
    using HandleType = RootRequestHandler;
};

struct PingEndpoint
{
    static constexpr auto path = "/ping";
    static constexpr auto strict_path = true;
    using HandleType = PingRequestHandler;
};

struct ReplicasStatusEndpoint
{
    static constexpr auto path = "/replicas_status";
    static constexpr auto strict_path = false;
    using HandleType = ReplicasStatusHandler;
};

using HTTPRootRequestHandlerFactory = HTTPGetRequestHandlerFactory<RootEndpoint>;
using HTTPPingRequestHandlerFactory = HTTPGetRequestHandlerFactory<PingEndpoint>;
using HTTPReplicasStatusRequestHandlerFactory = HTTPGetRequestHandlerFactory<ReplicasStatusEndpoint>;

template <typename HandleType>
HTTPRequestHandlerFactoryMain * createDefaultHandlerFatory(IServer & server, const std::string & name)
{
    auto handlerFactory = new HTTPRequestHandlerFactoryMain(server, name);
    handlerFactory->addHandler<HTTPRootRequestHandlerFactory>()
                  ->addHandler<HTTPPingRequestHandlerFactory>()
                  ->addHandler<HTTPReplicasStatusRequestHandlerFactory>()
                  ->addHandler<HTTPQueryRequestHandlerFactory<HandleType>>();
    return handlerFactory;
}


}
