#pragma once

#include <Server/KeeperTCPHandler.h>
#include <Poco/Net/TCPServerConnectionFactory.h>
#include <Poco/Net/NetException.h>
#include <base/logger_useful.h>
#include <Server/IServer.h>
#include <string>

namespace DB
{

class KeeperTCPHandlerFactory : public Poco::Net::TCPServerConnectionFactory
{
private:
    IServer & server;
    Poco::Logger * log;
    class DummyTCPHandler : public Poco::Net::TCPServerConnection
    {
    public:
        using Poco::Net::TCPServerConnection::TCPServerConnection;
        void run() override {}
    };
public:
    KeeperTCPHandlerFactory(IServer & server_, bool secure)
        : server(server_)
        , log(&Poco::Logger::get(std::string{"KeeperTCP"} + (secure ? "S" : "") + "HandlerFactory"))
    {
    }

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket) override
    {
        try
        {
            LOG_TRACE(log, "Keeper request. Address: {}", socket.peerAddress().toString());
            return new KeeperTCPHandler(server, socket);
        }
        catch (const Poco::Net::NetException &)
        {
            LOG_TRACE(log, "TCP Request. Client is not connected (most likely RST packet was sent).");
            return new DummyTCPHandler(socket);
        }
    }
};

}
