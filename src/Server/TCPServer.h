#pragma once

#include <Poco/Net/TCPServer.h>
#include <Poco/Net/TCPServerParams.h>

#include <base/types.h>
#include <Server/TCPServerConnectionFactory.h>
#include <Core/ServerSettings.h>

#include <functional>
#include <mutex>
#include <set>


namespace DB
{
class Context;

class TCPServerConnectionFilter : public Poco::Net::TCPServerConnectionFilter
{
public:
    explicit TCPServerConnectionFilter(std::function<bool()> filter_func_) : filter_func(std::move(filter_func_)) {}

    bool accept(const Poco::Net::StreamSocket &) override { return filter_func(); }

protected:
    ~TCPServerConnectionFilter() override = default;

private:
    std::function<bool()> filter_func;
};

class TCPServer : public Poco::Net::TCPServer
{
public:
    explicit TCPServer(
        TCPServerConnectionFactory::Ptr factory,
        Poco::ThreadPool & thread_pool,
        Poco::Net::ServerSocket & socket,
        Poco::Net::TCPServerParams::Ptr params = new Poco::Net::TCPServerParams,
        const TCPServerConnectionFilter::Ptr & filter = nullptr);

    /// Close the socket and ask existing connections to stop serving queries
    void stop()
    {
        if (!is_open)
            return;

        // FIXME: On darwin calling shutdown(SHUT_RD) on the socket blocked in accept() leads to ENOTCONN
#ifndef OS_DARWIN
        // Shutdown the listen socket before stopping tcp server to avoid 2.5second delay
        socket.shutdownReceive();
#endif

        Poco::Net::TCPServer::stop();
        // This notifies already established connections that they should stop serving
        // queries and close their socket as soon as they can.
        is_open = false;
        // Poco's stop() stops listening on the socket but leaves it open.
        // To be able to hand over control of the listening port to a new server, and
        // to get fast connection refusal instead of timeouts, we also need to close
        // the listening socket.
        socket.close();
    }

    bool isOpen() const { return is_open; }

    UInt16 portNumber() const { return port_number; }

    const Poco::Net::ServerSocket& getSocket() { return socket; }

    /// Register an active connection socket for potential forced shutdown.
    void registerConnection(const Poco::Net::StreamSocket & sock);

    /// Unregister a connection socket when the handler exits.
    void unregisterConnection(const Poco::Net::StreamSocket & sock);

    /// Shutdown all registered connection sockets to force them to close immediately.
    void closeConnections();

private:
    TCPServerConnectionFactory::Ptr factory;
    Poco::Net::ServerSocket socket;
    std::atomic<bool> is_open;
    UInt16 port_number;

    std::mutex connections_mutex;
    std::set<int> registered_fds; /// raw file descriptors of active connections
};

/// RAII guard for connection registration. Registers the socket on construction,
/// unregisters on destruction.
class TCPConnectionRegistration
{
public:
    TCPConnectionRegistration(TCPServer & server_, const Poco::Net::StreamSocket & socket_)
        : server(server_), sock(socket_)
    {
        server.registerConnection(sock);
    }

    ~TCPConnectionRegistration()
    {
        server.unregisterConnection(sock);
    }

    TCPConnectionRegistration(const TCPConnectionRegistration &) = delete;
    TCPConnectionRegistration & operator=(const TCPConnectionRegistration &) = delete;

private:
    TCPServer & server;
    const Poco::Net::StreamSocket & sock;
};

}
