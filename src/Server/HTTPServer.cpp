#include "HTTPServer.h"

namespace DB
{

HTTPServer::HTTPServer(
    Poco::Net::HTTPRequestHandlerFactory::Ptr pFactory,
    Poco::ThreadPool & threadPool,
    const Poco::Net::ServerSocket & socket,
    Poco::Net::HTTPServerParams::Ptr pParams)
    : Poco::Net::HTTPServer(pFactory, threadPool, socket, pParams)
{
}

void HTTPServer::start()
{
    Poco::Net::HTTPServer::start();
}

void HTTPServer::stop()
{
    Poco::Net::HTTPServer::stop();
}

int HTTPServer::currentConnections() const {
    return Poco::Net::HTTPServer::currentConnections();
}

}
