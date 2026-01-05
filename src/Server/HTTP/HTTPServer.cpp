#include <Server/HTTP/HTTPServer.h>

#include <Server/HTTP/HTTPServerConnectionFactory.h>


namespace DB
{
HTTPServer::HTTPServer(
    HTTPContextPtr context,
    HTTPRequestHandlerFactoryPtr factory_,
    Poco::ThreadPool & thread_pool,
    Poco::Net::ServerSocket & socket_,
    Poco::Net::HTTPServerParams::Ptr http1_params,
    HTTP2ServerParams::Ptr http2_params,
    const TCPServerConnectionFilter::Ptr & filter,
    const ProfileEvents::Event & read_event,
    const ProfileEvents::Event & write_event)
    : TCPServer(new HTTPServerConnectionFactory(context, http1_params, http2_params, factory_, thread_pool, read_event, write_event), thread_pool, socket_, http1_params, filter), factory(factory_)
{
}

HTTPServer::~HTTPServer()
{
    /// We should call stop and join thread here instead of destructor of parent TCPHandler,
    /// because there's possible race on 'vptr' between this virtual destructor and 'run' method.
    stop();
}

void HTTPServer::stopAll(bool /* abortCurrent */)
{
    stop();
}

}
