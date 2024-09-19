#pragma once

#include <cstddef>
#include <memory>
#include <string>

#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/HTTPServerParams.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/MessageHeader.h>
#include <Poco/Net/NetException.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/AutoPtr.h>
#include <Poco/SharedPtr.h>

class MockRequestHandler : public Poco::Net::HTTPRequestHandler
{
    Poco::Net::MessageHeader & last_request_header;

public:
    explicit MockRequestHandler(Poco::Net::MessageHeader & last_request_header_)
    : last_request_header(last_request_header_)
    {
    }

    void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override
    {
        response.setStatus(Poco::Net::HTTPResponse::HTTP_OK);
        last_request_header = request;
        response.send();
    }
};

class HTTPRequestHandlerFactory : public Poco::Net::HTTPRequestHandlerFactory
{
    Poco::Net::MessageHeader & last_request_header;

    Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest &) override
    {
        return new MockRequestHandler(last_request_header);
    }

public:
    explicit HTTPRequestHandlerFactory(Poco::Net::MessageHeader & last_request_header_)
    : last_request_header(last_request_header_)
    {
    }

    ~HTTPRequestHandlerFactory() override = default;
};

class TestPocoHTTPServer
{
    std::unique_ptr<Poco::Net::ServerSocket> server_socket;
    Poco::SharedPtr<HTTPRequestHandlerFactory> handler_factory;
    Poco::AutoPtr<Poco::Net::HTTPServerParams> server_params;
    std::unique_ptr<Poco::Net::HTTPServer> server;
    // Stores the last request header handled. It's obviously not thread-safe to share the same
    // reference across request handlers, but it's good enough for this the purposes of this test.
    Poco::Net::MessageHeader last_request_header;

public:
    TestPocoHTTPServer():
        server_socket(std::make_unique<Poco::Net::ServerSocket>(0)),
        handler_factory(new HTTPRequestHandlerFactory(last_request_header)),
        server_params(new Poco::Net::HTTPServerParams()),
        server(std::make_unique<Poco::Net::HTTPServer>(handler_factory, *server_socket, server_params))
    {
        server->start();
    }

    std::string getUrl()
    {
        return "http://" + server_socket->address().toString();
    }

    const Poco::Net::MessageHeader & getLastRequestHeader() const
    {
        return last_request_header;
    }
};
