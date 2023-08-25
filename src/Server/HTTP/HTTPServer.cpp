#include <Server/HTTP/HTTPServer.h>

#include <Server/HTTP/HTTPServerConnectionFactory.h>


namespace DB
{
HTTPServer::HTTPServer(
    ContextPtr context,
    HTTPRequestHandlerFactoryPtr factory_,
    Poco::ThreadPool & thread_pool,
    Poco::Net::ServerSocket & socket_,
    Poco::Net::HTTPServerParams::Ptr params)
    : TCPServer(new HTTPServerConnectionFactory(context, params, factory_), thread_pool, socket_, params), factory(factory_)
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
