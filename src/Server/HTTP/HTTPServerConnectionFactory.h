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
    HTTPServerConnectionFactory(HTTPContextPtr context, Poco::Net::HTTPServerParams::Ptr params, HTTPRequestHandlerFactoryPtr factory, const CurrentMetrics::Metric & read_metric_ = CurrentMetrics::end(), const CurrentMetrics::Metric & write_metric_ = CurrentMetrics::end());

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server) override;
    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server, TCPProtocolStackData & stack_data) override;

private:
    HTTPContextPtr context;
    Poco::Net::HTTPServerParams::Ptr params;
    HTTPRequestHandlerFactoryPtr factory;
    CurrentMetrics::Metric read_metric;
    CurrentMetrics::Metric write_metric;
};

}
