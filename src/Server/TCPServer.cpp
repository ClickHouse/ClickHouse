#include "TCPServer.h"

namespace DB
{

TCPServer::TCPServer(
    Poco::Net::TCPServerConnectionFactory::Ptr pFactory,
    Poco::ThreadPool & threadPool,
    const Poco::Net::ServerSocket & socket,
    Poco::Net::TCPServerParams::Ptr pParams)
    : Poco::Net::TCPServer(pFactory, threadPool, socket, pParams)
{
}

void TCPServer::start()
{
    Poco::Net::TCPServer::start();
}

void TCPServer::stop()
{
    Poco::Net::TCPServer::stop();
}

int TCPServer::currentConnections() const {
    return Poco::Net::TCPServer::currentConnections();
}

}
