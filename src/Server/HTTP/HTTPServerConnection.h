#pragma once

#include <Interpreters/Context.h>
#include <Server/IndirectServerConnection.h>
#include <Server/HTTP/HTTPRequestHandlerFactory.h>
#include <Server/HTTPInterfaceConfig.h>

#include <Poco/Net/HTTPServerParams.h>
#include <Poco/Net/HTTPServerSession.h>

namespace DB
{

class HTTPServerConnection : public IndirectHTTPServerConnection
{
public:
    HTTPServerConnection(
        ContextPtr context,
        const Poco::Net::StreamSocket & socket,
        Poco::Net::HTTPServerParams::Ptr params,
        HTTPRequestHandlerFactoryPtr factory,
        const HTTPInterfaceConfigBase & config
    );

    void run() override;

protected:
    static void sendErrorResponse(Poco::Net::HTTPServerSession & session, Poco::Net::HTTPResponse::HTTPStatus status);

private:
    ContextPtr context;
    Poco::Net::HTTPServerParams::Ptr params;
    HTTPRequestHandlerFactoryPtr factory;
    bool stopped;
    HTTPInterfaceConfig config;
    std::mutex mutex;  // guards the |factory| with assumption that creating handlers is not thread-safe.
};

}
