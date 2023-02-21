#pragma once

#include <Poco/Net/NetException.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Common/logger_useful.h>
#include "Server/TCPProtocolStackData.h"
#include <Server/IServer.h>
#include <Server/RemoteFSHandler.h>
#include <Server/TCPServerConnectionFactory.h>

namespace Poco { class Logger; }

namespace DB
{

class RemoteFSHandlerFactory : public TCPServerConnectionFactory
{
private:
    IServer & server;
    Poco::Logger * log;
    std::string server_display_name;

    class DummyTCPHandler : public Poco::Net::TCPServerConnection
    {
    public:
        using Poco::Net::TCPServerConnection::TCPServerConnection;
        void run() override {}
    };

public:
    RemoteFSHandlerFactory(IServer & server_, bool secure_)
        : server(server_)
        , log(&Poco::Logger::get(std::string("TCP") + (secure_ ? "S" : "") + "HandlerFactory"))
    {
        server_display_name = server.config().getString("display_name", getFQDNOrHostName());
    }

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server) override
    {
        try
        {
            LOG_TRACE(log, "TCP Request. Address: {}", socket.peerAddress().toString());

            return new RemoteFSHandler(server, tcp_server, socket, server_display_name);
        }
        catch (const Poco::Net::NetException &)
        {
            LOG_TRACE(log, "TCP Request. Client is not connected (most likely RST packet was sent).");
            return new DummyTCPHandler(socket);
        }
    }

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server, TCPProtocolStackData & stack_data) override
    {
        try
        {
            LOG_TRACE(log, "TCP Request. Address: {}", socket.peerAddress().toString());

            return new RemoteFSHandler(server, tcp_server, socket, stack_data, server_display_name);
        }
        catch (const Poco::Net::NetException &)
        {
            LOG_TRACE(log, "TCP Request. Client is not connected (most likely RST packet was sent).");
            return new DummyTCPHandler(socket);
        }
    }
};

}
