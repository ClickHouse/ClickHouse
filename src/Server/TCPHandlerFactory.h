#pragma once

#include <Poco/Net/TCPServerConnectionFactory.h>
#include <Poco/Net/NetException.h>
#include <common/logger_useful.h>
#include <Server/IServer.h>
#include <Server/TCPHandler.h>

namespace Poco { class Logger; }

namespace DB
{

class TCPHandlerFactory : public Poco::Net::TCPServerConnectionFactory
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
    explicit TCPHandlerFactory(IServer & server_, bool secure_ = false)
        : server(server_)
        , log(&Logger::get(std::string("TCP") + (secure_ ? "S" : "") + "HandlerFactory"))
    {
    }

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket) override
    {
        try
        {
            LOG_TRACE(log, "TCP Request. Address: {}", socket.peerAddress().toString());
            return new TCPHandler(server, socket);
        }
        catch (const Poco::Net::NetException &)
        {
            LOG_TRACE(log, "TCP Request. Client is not connected (most likely RST packet was sent).");
            return new DummyTCPHandler(socket);
        }
    }
};

}
