#pragma once

#include <Server/HTTP/HTTPRequestHandlerFactory.h>

#include <Poco/Net/HTTPServerParams.h>
#include <Poco/Net/TCPServer.h>

#include <common/types.h>


namespace DB
{

class Context;

class HTTPServer : public Poco::Net::TCPServer
{
public:
    explicit HTTPServer(
        ContextPtr context,
        HTTPRequestHandlerFactoryPtr factory,
        UInt16 port_number = 80,
        Poco::Net::HTTPServerParams::Ptr params = new Poco::Net::HTTPServerParams);

    HTTPServer(
        ContextPtr context,
        HTTPRequestHandlerFactoryPtr factory,
        const Poco::Net::ServerSocket & socket,
        Poco::Net::HTTPServerParams::Ptr params);

    HTTPServer(
        ContextPtr context,
        HTTPRequestHandlerFactoryPtr factory,
        Poco::ThreadPool & thread_pool,
        const Poco::Net::ServerSocket & socket,
        Poco::Net::HTTPServerParams::Ptr params);

    ~HTTPServer() override;

    void stopAll(bool abort_current = false);

private:
    HTTPRequestHandlerFactoryPtr factory;
};

}
