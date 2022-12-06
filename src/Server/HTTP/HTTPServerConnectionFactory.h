#pragma once

#include <Interpreters/Context.h>
#include <Server/HTTP/HTTPRequestHandlerFactory.h>
#include <Server/TCPServerConnectionFactory.h>

#include <Poco/Net/HTTPServerParams.h>

namespace DB
{

class HTTPServerConnectionFactory : public TCPServerConnectionFactory
{
public:
    HTTPServerConnectionFactory(ContextPtr context, Poco::Net::HTTPServerParams::Ptr params, HTTPRequestHandlerFactoryPtr factory);

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server) override;

private:
    ContextPtr context;
    Poco::Net::HTTPServerParams::Ptr params;
    HTTPRequestHandlerFactoryPtr factory;
};

}
