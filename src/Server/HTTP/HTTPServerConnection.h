#pragma once

#include <Server/HTTP/HTTPRequestHandlerFactory.h>
#include <Server/HTTP/HTTPContext.h>

#include <Poco/Net/HTTPServerParams.h>
#include <Poco/Net/HTTPServerSession.h>
#include <Poco/Net/TCPServerConnection.h>

namespace DB
{
class TCPServer;

class HTTPServerConnection : public Poco::Net::TCPServerConnection
{
public:
    HTTPServerConnection(
        HTTPContextPtr context,
        TCPServer & tcp_server,
        const Poco::Net::StreamSocket & socket,
        Poco::Net::HTTPServerParams::Ptr params,
        HTTPRequestHandlerFactoryPtr factory);

    void run() override;

protected:
    static void sendErrorResponse(Poco::Net::HTTPServerSession & session, Poco::Net::HTTPResponse::HTTPStatus status);

private:
    HTTPContextPtr context;
    TCPServer & tcp_server;
    Poco::Net::HTTPServerParams::Ptr params;
    HTTPRequestHandlerFactoryPtr factory;
    bool stopped;
    std::mutex mutex;  // guards the |factory| with assumption that creating handlers is not thread-safe.
};

}
