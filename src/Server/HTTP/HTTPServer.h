#pragma once

#include <Server/HTTP/HTTPRequestHandlerFactory.h>
#include <Server/HTTP/HTTPContext.h>
#include <Server/TCPServer.h>

#include <Poco/Net/HTTPServerParams.h>

#include <base/types.h>


namespace DB
{

class HTTPServer : public TCPServer
{
public:
    explicit HTTPServer(
        HTTPContextPtr context,
        HTTPRequestHandlerFactoryPtr factory,
        Poco::ThreadPool & thread_pool,
        Poco::Net::ServerSocket & socket,
        Poco::Net::HTTPServerParams::Ptr params);

    ~HTTPServer() override;

    void stopAll(bool abort_current = false);

private:
    HTTPRequestHandlerFactoryPtr factory;
};

}
