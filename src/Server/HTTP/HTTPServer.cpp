#include <Server/HTTP/HTTPServer.h>
#include <Server/HTTP/HTTPServerConnectionFactory.h>
#include <Server/InterfaceConfig.h>

namespace DB
{

HTTPServer::HTTPServer(
    ContextPtr context,
    HTTPRequestHandlerFactoryPtr factory_,
    Poco::ThreadPool & thread_pool,
    const Poco::Net::ServerSocket & socket,
    Poco::Net::HTTPServerParams::Ptr params,
    const HTTPInterfaceConfigBase & config
)
    : TCPServer(new HTTPServerConnectionFactory(context, params, factory_, config), thread_pool, socket, params)
    , factory(factory_)
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
