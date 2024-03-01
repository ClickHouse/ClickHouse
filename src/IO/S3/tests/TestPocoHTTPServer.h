#pragma once

#include <cstddef>
#include <memory>
#include <string>

#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/HTTPServerParams.h>
#include <Poco/Net/NetException.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/AutoPtr.h>
#include <Poco/SharedPtr.h>


template <typename RequestHandler>
class HTTPRequestHandlerFactory : public Poco::Net::HTTPRequestHandlerFactory
{
    virtual Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest &) override
    {
        return new RequestHandler();
    }

public:
    virtual ~HTTPRequestHandlerFactory() override
    {
    }
};

template <typename RequestHandler>
class TestPocoHTTPServer
{
    std::unique_ptr<Poco::Net::ServerSocket> server_socket;
    Poco::SharedPtr<HTTPRequestHandlerFactory<RequestHandler>> handler_factory;
    Poco::AutoPtr<Poco::Net::HTTPServerParams> server_params;
    std::unique_ptr<Poco::Net::HTTPServer> server;

public:
    TestPocoHTTPServer():
        server_socket(std::make_unique<Poco::Net::ServerSocket>(0)),
        handler_factory(new HTTPRequestHandlerFactory<RequestHandler>()),
        server_params(new Poco::Net::HTTPServerParams()),
        server(std::make_unique<Poco::Net::HTTPServer>(handler_factory, *server_socket, server_params))
    {
        server->start();
    }

    std::string getUrl()
    {
        return "http://" + server_socket->address().toString();
    }
};
