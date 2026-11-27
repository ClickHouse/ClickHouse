#pragma once

#include <Server/TCPServer.h>
#include <Server/TCPServerConnectionFactory.h>

#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Poco/Net/HTTPServerConnection.h>
#include <Poco/Net/HTTPServerParams.h>
#include <Poco/Net/ServerSocket.h>

#include <atomic>


namespace DB
{

/// Serves `Poco::Net::HTTPRequestHandlerFactory` handlers on a `TCPServer`, for unit tests.
class TestHTTPServer
{
public:
    TestHTTPServer(
        Poco::Net::HTTPRequestHandlerFactory::Ptr factory,
        const Poco::Net::ServerSocket & socket_,
        Poco::Net::HTTPServerParams::Ptr params = new Poco::Net::HTTPServerParams)
        : server_socket(socket_)
        , thread_pool(params->getMaxThreads() > 0 ? params->getMaxThreads() : 16)
        , server(new ConnectionFactory(*this, params, factory), thread_pool, server_socket, params)
    {
    }

    TestHTTPServer(
        Poco::Net::HTTPRequestHandlerFactory::Ptr factory,
        UInt16 port,
        Poco::Net::HTTPServerParams::Ptr params = new Poco::Net::HTTPServerParams)
        : TestHTTPServer(factory, Poco::Net::ServerSocket(port), params)
    {
    }

    ~TestHTTPServer() { stop(); }

    void start() { server.start(); }

    /// Stops accepting and waits until every connection has finished: the connections reference the server.
    void stop()
    {
        server.stop();
        thread_pool.wait();
    }

    const Poco::Net::ServerSocket & socket() const { return server_socket; }

    /// Connections being served.
    size_t currentConnections() const { return current_connections; }

    /// Connections served so far.
    size_t totalConnections() const { return total_connections; }

private:
    /// Poco's connection, counted from its creation until it is destroyed (after its socket is closed).
    class Connection : public Poco::Net::HTTPServerConnection
    {
    public:
        Connection(
            TestHTTPServer & owner_,
            const Poco::Net::StreamSocket & socket,
            Poco::Net::HTTPServerParams::Ptr params,
            Poco::Net::HTTPRequestHandlerFactory::Ptr factory)
            : Poco::Net::HTTPServerConnection(socket, params, factory)
            , owner(owner_)
        {
            ++owner.total_connections;
            ++owner.current_connections;
        }

        ~Connection() override { --owner.current_connections; }

    private:
        TestHTTPServer & owner;
    };

    class ConnectionFactory : public TCPServerConnectionFactory
    {
    public:
        ConnectionFactory(
            TestHTTPServer & owner_,
            Poco::Net::HTTPServerParams::Ptr params_,
            Poco::Net::HTTPRequestHandlerFactory::Ptr factory_)
            : owner(owner_)
            , params(params_)
            , factory(factory_)
        {
        }

        Poco::Net::TCPServerConnection * createConnectionImpl(const Poco::Net::StreamSocket & socket, TCPServer &) override
        {
            return new Connection(owner, socket, params, factory);
        }

    private:
        TestHTTPServer & owner;
        Poco::Net::HTTPServerParams::Ptr params;
        Poco::Net::HTTPRequestHandlerFactory::Ptr factory;
    };

    Poco::Net::ServerSocket server_socket;
    std::atomic<size_t> current_connections = 0;
    std::atomic<size_t> total_connections = 0;
    TCPServerThreadPool thread_pool;
    /// Declared after the pool it uses.
    TCPServer server;
};

}
