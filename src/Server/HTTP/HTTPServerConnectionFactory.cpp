#include <Server/HTTP/HTTPServerConnectionFactory.h>

#include <Server/HTTP/HTTPServerConnection.h>

namespace DB
{
HTTPServerConnectionFactory::HTTPServerConnectionFactory(
    HTTPContextPtr context_, Poco::Net::HTTPServerParams::Ptr params_, HTTPRequestHandlerFactoryPtr factory_, const CurrentMetrics::Metric & read_metric_, const CurrentMetrics::Metric & write_metric_)
    : context(std::move(context_)), params(params_), factory(factory_), read_metric(read_metric_), write_metric(write_metric_)
{
    poco_check_ptr(factory);
}

Poco::Net::TCPServerConnection * HTTPServerConnectionFactory::createConnection(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server)
{
    return new HTTPServerConnection(context, tcp_server, socket, params, factory, read_metric, write_metric);
}

Poco::Net::TCPServerConnection * HTTPServerConnectionFactory::createConnection(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server, TCPProtocolStackData & stack_data)
{
    return new HTTPServerConnection(context, tcp_server, socket, params, factory, stack_data.forwarded_for, read_metric, write_metric);
}

}
