#pragma once

#include <memory>
#include <list>

#include <Poco/Net/NetException.h>
#include <Poco/Net/TCPServerConnection.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Server/TCPServerConnectionFactory.h>
#include <Common/logger_useful.h>
#include <Server/IServer.h>
#include <base/getFQDNOrHostName.h>
#include <Server/TCPServer.h>

#include "base/types.h"


namespace DB
{

class TCPProtocolStack : public Poco::Net::TCPServerConnection
{
    using StreamSocket = Poco::Net::StreamSocket;
    using TCPServerConnection = Poco::Net::TCPServerConnection;
private:
    TCPServer & tcp_server;

public:
    TCPProtocolStack(TCPServer & tcp_server_, const StreamSocket & socket) : TCPServerConnection(socket), tcp_server(tcp_server_) {}

    void append(std::unique_ptr<TCPServerConnectionFactory> factory)
    {
        stack.emplace_back(std::move(factory));
    }

    void run() override
    {
        for (auto & factory : stack)
        {
            std::unique_ptr<TCPServerConnection> connection(factory->createConnection(socket(), tcp_server));
            connection->run();
        }
    }

private:
    std::list<std::unique_ptr<TCPServerConnectionFactory>> stack;
};


class TCPProtocolStackFactory : public TCPServerConnectionFactory
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
    explicit TCPProtocolStackFactory(IServer & server_) :
        server(server_), log(&Poco::Logger::get("TCPProtocolStackFactory"))
    {
        server_display_name = server.config().getString("display_name", getFQDNOrHostName());
    }

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server) override
    {
        try
        {
            LOG_TRACE(log, "TCP Request. Address: {}", socket.peerAddress().toString());
            return new TCPProtocolStack(tcp_server, socket);
        }
        catch (const Poco::Net::NetException &)
        {
            LOG_TRACE(log, "TCP Request. Client is not connected (most likely RST packet was sent).");
            return new DummyTCPHandler(socket);
        }
    }
};


}
