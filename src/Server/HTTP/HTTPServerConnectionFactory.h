#pragma once

#include <Interpreters/Context.h>
#include <Server/HTTP/HTTPRequestHandlerFactory.h>
#include <Server/HTTPInterfaceConfig.h>

#include <Poco/Net/HTTPServerParams.h>
#include <Poco/Net/TCPServerConnectionFactory.h>

namespace DB
{

class HTTPServerConnectionFactory : public Poco::Net::TCPServerConnectionFactory
{
public:
    HTTPServerConnectionFactory(
        ContextPtr context,
        Poco::Net::HTTPServerParams::Ptr params,
        HTTPRequestHandlerFactoryPtr factory,
        const HTTPInterfaceConfigBase & config
    );

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket) override;

private:
    ContextPtr context;
    Poco::Net::HTTPServerParams::Ptr params;
    HTTPRequestHandlerFactoryPtr factory;
    HTTPInterfaceConfig config;
};

}
