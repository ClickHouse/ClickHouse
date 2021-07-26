#pragma once

#include <Interpreters/Context.h>
#include <Server/HTTP/HTTPRequestHandlerFactory.h>

#include <Poco/Net/HTTPServerParams.h>
#include <Poco/Net/TCPServerConnectionFactory.h>

namespace DB
{

class HTTPServerConnectionFactory : public Poco::Net::TCPServerConnectionFactory
{
public:
    HTTPServerConnectionFactory(const Context & context, Poco::Net::HTTPServerParams::Ptr params, HTTPRequestHandlerFactoryPtr factory);

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket) override;

private:
    Context context;
    Poco::Net::HTTPServerParams::Ptr params;
    HTTPRequestHandlerFactoryPtr factory;
};

}
