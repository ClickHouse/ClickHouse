#pragma once
#include <Server/TestKeeperTCPHandler.h>
#include <Poco/Net/TCPServerConnectionFactory.h>
#include <Poco/Net/NetException.h>
#include <common/logger_useful.h>
#include <Server/IServer.h>

namespace DB
{

class TestKeeperTCPHandlerFactory : public Poco::Net::TCPServerConnectionFactory
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
    TestKeeperTCPHandlerFactory(IServer & server_)
        : server(server_)
        , log(&Poco::Logger::get("TestKeeperTCPHandlerFactory"))
    {
    }

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket) override
    {
        try
        {
            LOG_TRACE(log, "Test keeper request. Address: {}", socket.peerAddress().toString());
            return new TestKeeperTCPHandler(server, socket);
        }
        catch (const Poco::Net::NetException &)
        {
            LOG_TRACE(log, "TCP Request. Client is not connected (most likely RST packet was sent).");
            return new DummyTCPHandler(socket);
        }
    }
};

}
