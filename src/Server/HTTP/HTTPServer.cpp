#include <Server/HTTP/HTTPServer.h>

#include <Server/HTTP/HTTPServerConnectionFactory.h>


namespace DB
{
HTTPServer::HTTPServer(
    const Context & context,
    HTTPRequestHandlerFactoryPtr factory_,
    UInt16 portNumber,
    Poco::Net::HTTPServerParams::Ptr params)
    : TCPServer(new HTTPServerConnectionFactory(context, params, factory_), portNumber, params), factory(factory_)
{
}

HTTPServer::HTTPServer(
    const Context & context,
    HTTPRequestHandlerFactoryPtr factory_,
    const Poco::Net::ServerSocket & socket,
    Poco::Net::HTTPServerParams::Ptr params)
    : TCPServer(new HTTPServerConnectionFactory(context, params, factory_), socket, params), factory(factory_)
{
}

HTTPServer::HTTPServer(
    const Context & context,
    HTTPRequestHandlerFactoryPtr factory_,
    Poco::ThreadPool & threadPool,
    const Poco::Net::ServerSocket & socket,
    Poco::Net::HTTPServerParams::Ptr params)
    : TCPServer(new HTTPServerConnectionFactory(context, params, factory_), threadPool, socket, params), factory(factory_)
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
