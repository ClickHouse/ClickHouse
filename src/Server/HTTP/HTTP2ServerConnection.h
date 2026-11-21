#pragma once

#include "HTTP2RequestHandlerFactory.h"
// #include "HTTP2ServerStream.h"

#include <Server/HTTP/HTTPServerConnection.h>
#include <Server/HTTP/HTTP2ServerStream.h>

namespace DB
{
class TCPServer;

class HTTP2ServerConnection : public Poco::Net::TCPServerConnection // TODO: HTTPServerConnection
{
public:
    HTTP2ServerConnection(
        const Poco::Net::StreamSocket & socket_,
        HTTP2RequestHandlerFactoryPtr factory_)
    : Poco::Net::TCPServerConnection(socket_)
    , factory(factory_)
    , stopped(false)
    {}

    void run() override;

    void handleRequest(HTTP2ServerStream* stream);

protected:
    static void sendErrorResponse(Poco::Net::HTTPServerSession & session, Poco::Net::HTTPResponse::HTTPStatus status);

private:
    HTTP2RequestHandlerFactoryPtr factory;
    bool stopped;
    std::mutex mutex;  // guards the |factory| with assumption that creating handlers is not thread-safe.
};

}
