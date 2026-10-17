#pragma once

#include <Server/KeeperTCPHandler.h>
#include <Server/TCPServerConnectionFactory.h>
#include <Poco/Net/NetException.h>
#include <Common/logger_useful.h>
#include <Server/IServer.h>
#include <string>

namespace DB
{

class KeeperTCPHandlerFactory : public TCPServerConnectionFactory
{
private:
    std::shared_ptr<KeeperDispatcher> keeper_dispatcher;
    LoggerPtr log;
    Poco::Timespan receive_timeout;
    Poco::Timespan send_timeout;

    class DummyTCPHandler : public Poco::Net::TCPServerConnection
    {
    public:
        using Poco::Net::TCPServerConnection::TCPServerConnection;
        void run() override {}
    };

public:
    KeeperTCPHandlerFactory(
        std::shared_ptr<KeeperDispatcher> keeper_dispatcher_,
        uint64_t receive_timeout_seconds,
        uint64_t send_timeout_seconds,
        bool secure)
        : keeper_dispatcher(keeper_dispatcher_)
        , log(getLogger(std::string{"KeeperTCP"} + (secure ? "S" : "") + "HandlerFactory"))
        , receive_timeout(/* seconds = */ receive_timeout_seconds, /* microseconds = */ 0)
        , send_timeout(/* seconds = */ send_timeout_seconds, /* microseconds = */ 0)
    {
    }

    Poco::Net::TCPServerConnection * createConnectionImpl(const Poco::Net::StreamSocket & socket, TCPServer &) override
    {
        try
        {
            LOG_TRACE(log, "Keeper request. Address: {}", socket.peerAddress().toString());
            return new KeeperTCPHandler(keeper_dispatcher, receive_timeout, send_timeout, socket);
        }
        catch (const Poco::Net::NetException &)
        {
            LOG_TRACE(log, "TCP Request. Client is not connected (most likely RST packet was sent).");
            return new DummyTCPHandler(socket);
        }
    }

};

}
