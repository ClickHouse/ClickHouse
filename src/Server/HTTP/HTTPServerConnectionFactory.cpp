#include <Server/HTTP/HTTPServerConnectionFactory.h>
#include <Server/HTTP/HTTP1/HTTP1ServerConnection.h>
#include <Server/HTTP/HTTP2/HTTP2ServerConnection.h>

#include <Poco/ThreadPool.h>

#include <config.h>

namespace DB
{
HTTPServerConnectionFactory::HTTPServerConnectionFactory(
    HTTPContextPtr context_,
    Poco::Net::HTTPServerParams::Ptr http1_params_,
    HTTP2ServerParams::Ptr http2_params_,
    HTTPRequestHandlerFactoryPtr factory_,
    Poco::ThreadPool & thread_pool_,
    const ProfileEvents::Event & read_event_,
    const ProfileEvents::Event & write_event_)
    : context(std::move(context_)), http1_params(http1_params_), http2_params(http2_params_), factory(factory_), thread_pool(thread_pool_), read_event(read_event_), write_event(write_event_)
{
    poco_check_ptr(factory);
}

Poco::Net::TCPServerConnection * HTTPServerConnectionFactory::createConnection(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server)
{
#if USE_NGHTTP2
    if (isHTTP2Connection(socket, http2_params))
        return new HTTP2ServerConnection(context, tcp_server, socket, http2_params, factory, thread_pool, read_event, write_event);
#endif
    return new HTTP1ServerConnection(context, tcp_server, socket, http1_params, factory, read_event, write_event);
}

Poco::Net::TCPServerConnection * HTTPServerConnectionFactory::createConnection(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server, TCPProtocolStackData & stack_data)
{
#if USE_NGHTTP2
    if (isHTTP2Connection(socket, http2_params))
        return new HTTP2ServerConnection(context, tcp_server, socket, http2_params, factory, thread_pool, stack_data.forwarded_for, read_event, write_event);
#endif
    return new HTTP1ServerConnection(context, tcp_server, socket, http1_params, factory, stack_data.forwarded_for, read_event, write_event);
}

}
