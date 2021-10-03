#pragma once

#include <Server/HTTP/HTTPRequestHandlerFactory.h>

#include <Poco/Net/HTTPServerParams.h>
#include <Poco/Net/TCPServer.h>

#include <base/types.h>


namespace DB
{

class Context;
class HTTPInterfaceConfigBase;

class HTTPServer : public Poco::Net::TCPServer
{
public:
    HTTPServer(
        ContextPtr context,
        HTTPRequestHandlerFactoryPtr factory,
        Poco::ThreadPool & thread_pool,
        const Poco::Net::ServerSocket & socket,
        Poco::Net::HTTPServerParams::Ptr params,
        const HTTPInterfaceConfigBase & config);

    ~HTTPServer() override;

    void stopAll(bool abort_current = false);

private:
    HTTPRequestHandlerFactoryPtr factory;
};

}
