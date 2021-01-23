#pragma once

#include <Server/HTTP/HTTPRequestHandlerFactory.h>

#include <Poco/Net/HTTPServerParams.h>
#include <Poco/Net/TCPServer.h>

#include <common/types.h>


namespace DB
{

class HTTPServer : public Poco::Net::TCPServer
{
public:
    explicit HTTPServer(
        HTTPRequestHandlerFactoryPtr factory,
        UInt16 portNumber = 80,
        Poco::Net::HTTPServerParams::Ptr params = new Poco::Net::HTTPServerParams);

    HTTPServer(HTTPRequestHandlerFactoryPtr factory, const Poco::Net::ServerSocket & socket, Poco::Net::HTTPServerParams::Ptr params);

    HTTPServer(
        HTTPRequestHandlerFactoryPtr factory,
        Poco::ThreadPool & threadPool,
        const Poco::Net::ServerSocket & socket,
        Poco::Net::HTTPServerParams::Ptr params);

    ~HTTPServer() override;

    void stopAll(bool abortCurrent = false);

private:
    HTTPRequestHandlerFactoryPtr factory;
};

}
