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
#include <Poco/Net/SecureStreamSocket.h>

#include "Poco/Net/SSLManager.h"

#include "base/types.h"


namespace DB
{

class TCPConnectionAccessor : public Poco::Net::TCPServerConnection
{
public:
    using Poco::Net::TCPServerConnection::socket;
    explicit TCPConnectionAccessor(const Poco::Net::StreamSocket & socket) : Poco::Net::TCPServerConnection(socket) {}
};

class TCPProtocolStack : public Poco::Net::TCPServerConnection
{
    using StreamSocket = Poco::Net::StreamSocket;
    using TCPServerConnection = Poco::Net::TCPServerConnection;
private:
    TCPServer & tcp_server;
    std::list<TCPServerConnectionFactory::Ptr> stack;

public:
    TCPProtocolStack(TCPServer & tcp_server_, const StreamSocket & socket, const std::list<TCPServerConnectionFactory::Ptr> & stack_)
        : TCPServerConnection(socket), tcp_server(tcp_server_), stack(stack_)
    {}

    void run() override
    {
        for (auto & factory : stack)
        {
            std::unique_ptr<TCPServerConnection> connection(factory->createConnection(socket(), tcp_server));
            connection->run();
            if (auto * accessor = dynamic_cast<TCPConnectionAccessor*>(connection.get()); accessor)
                socket() = accessor->socket();
        }
    }
};


class TCPProtocolStackFactory : public TCPServerConnectionFactory
{
private:
    IServer & server;
    Poco::Logger * log;
    std::string server_display_name;
    std::list<TCPServerConnectionFactory::Ptr> stack;

    class DummyTCPHandler : public Poco::Net::TCPServerConnection
    {
    public:
        using Poco::Net::TCPServerConnection::TCPServerConnection;
        void run() override {}
    };

public:
    template <typename... T>
    explicit TCPProtocolStackFactory(IServer & server_, T... factory)
        : server(server_), log(&Poco::Logger::get("TCPProtocolStackFactory")), stack({factory...})
    {
        server_display_name = server.config().getString("display_name", getFQDNOrHostName());
    }

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server) override
    {
        try
        {
            LOG_TRACE(log, "TCP Request. Address: {}", socket.peerAddress().toString());
            return new TCPProtocolStack(tcp_server, socket, stack);
        }
        catch (const Poco::Net::NetException &)
        {
            LOG_TRACE(log, "TCP Request. Client is not connected (most likely RST packet was sent).");
            return new DummyTCPHandler(socket);
        }
    }

    void append(TCPServerConnectionFactory::Ptr factory)
    {
        stack.push_back(factory);
    }
};



class TLSHandler : public TCPConnectionAccessor
{
    using StreamSocket = Poco::Net::StreamSocket;
    using SecureStreamSocket = Poco::Net::SecureStreamSocket;
public:
    explicit TLSHandler(const StreamSocket & socket) : TCPConnectionAccessor(socket) {}

    void run() override
    {
        socket() = SecureStreamSocket::attach(socket(), Poco::Net::SSLManager::instance().defaultServerContext());
    }
};


class TLSHandlerFactory : public TCPServerConnectionFactory
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
    explicit TLSHandlerFactory(IServer & server_)
        : server(server_), log(&Poco::Logger::get("TLSHandlerFactory"))
    {
        server_display_name = server.config().getString("display_name", getFQDNOrHostName());
    }

    Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket, TCPServer &/* tcp_server*/) override
    {
        try
        {
            LOG_TRACE(log, "TCP Request. Address: {}", socket.peerAddress().toString());
            return new TLSHandler(socket);
        }
        catch (const Poco::Net::NetException &)
        {
            LOG_TRACE(log, "TCP Request. Client is not connected (most likely RST packet was sent).");
            return new DummyTCPHandler(socket);
        }
    }
};


}
