#pragma once

#include <Server/HTTP/HTTP2/HTTP2ServerParams.h>
#include <Server/HTTP/HTTPRequestHandlerFactory.h>
#include <Server/HTTP/HTTPContext.h>
#include <Server/TCPServerConnectionFactory.h>

#include <Poco/Net/HTTPServerParams.h>
#include <Poco/ThreadPool.h>

namespace DB
{

class HTTPServerConnectionFactory : public TCPServerConnectionFactory
{
public:
    HTTPServerConnectionFactory(
        HTTPContextPtr context,
        Poco::Net::HTTPServerParams::Ptr http1_params,
        HTTP2ServerParams::Ptr http2_params,
        HTTPRequestHandlerFactoryPtr factory,
        Poco::ThreadPool & thread_pool,
        const ProfileEvents::Event & read_event = ProfileEvents::end(),
        const ProfileEvents::Event & write_event = ProfileEvents::end());

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server) override;
    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server, TCPProtocolStackData & stack_data) override;

private:
    HTTPContextPtr context;
    Poco::Net::HTTPServerParams::Ptr http1_params;
    HTTP2ServerParams::Ptr http2_params;
    HTTPRequestHandlerFactoryPtr factory;
    [[maybe_unused]] Poco::ThreadPool & thread_pool;
    ProfileEvents::Event read_event;
    ProfileEvents::Event write_event;
};

}
