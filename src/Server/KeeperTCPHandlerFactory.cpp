#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#    include "config_core.h"
#endif

#if USE_NURAFT

#include <Poco/Net/NetException.h>
#include <common/logger_useful.h>
#include <string>
#include <Server/IServer.h>
#include <Server/KeeperTCPHandler.h>
#include <Server/KeeperTCPHandlerFactory.h>

namespace DB
{

class DummyKeeperTCPHandler : public Poco::Net::TCPServerConnection
{
public:
    using Poco::Net::TCPServerConnection::TCPServerConnection;
    void run() override {}
};

KeeperTCPHandlerFactory::KeeperTCPHandlerFactory(IServer & server_, bool secure)
    : server(server_)
    , log(&Poco::Logger::get(std::string{"KeeperTCP"} + (secure ? "S" : "") + "HandlerFactory"))
{
}

Poco::Net::TCPServerConnection * KeeperTCPHandlerFactory::createConnection(const Poco::Net::StreamSocket & socket)
{
    try
    {
        LOG_TRACE(log, "Keeper request. Address: {}", socket.peerAddress().toString());
        return new KeeperTCPHandler(server, socket);
    }
    catch (const Poco::Net::NetException &)
    {
        LOG_TRACE(log, "TCP Request. Client is not connected (most likely RST packet was sent).");
        return new DummyKeeperTCPHandler(socket);
    }
}

}

#endif
