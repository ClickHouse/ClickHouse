#pragma once

#include <Server/HTTP/HTTPRequestHandlerFactory.h>
#include <Server/HTTP/HTTPContext.h>
#include <Server/TCPServerConnectionFactory.h>

#include <Poco/Net/HTTPServerParams.h>

namespace DB
{

class HTTPServerConnectionFactory : public TCPServerConnectionFactory
{
public:
    HTTPServerConnectionFactory(HTTPContextPtr context, Poco::Net::HTTPServerParams::Ptr params, HTTPRequestHandlerFactoryPtr factory);

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server) override;
    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server, TCPProtocolStackData & stack_data) override;

private:
    HTTPContextPtr context;
    Poco::Net::HTTPServerParams::Ptr params;
    HTTPRequestHandlerFactoryPtr factory;
};

}
