#pragma once

#include <Common/config.h>

#include <base/types.h>
#include <memory>
#include <string>

namespace DB
{
class GRPCServer;
class TCPServer;

/// Provides an unified interface to access a protocol implementing server
/// no matter what type it has (HTTPServer, TCPServer, MySQLServer, GRPCServer, ...).
class ProtocolServerAdapter
{
    friend class ProtocolServers;
public:
    ProtocolServerAdapter(ProtocolServerAdapter && src) = default;
    ProtocolServerAdapter & operator =(ProtocolServerAdapter && src) = default;
    ProtocolServerAdapter(const std::string & listen_host_, const char * port_name_, const std::string & description_, std::unique_ptr<TCPServer> tcp_server_);

#if USE_GRPC && !defined(KEEPER_STANDALONE_BUILD)
    ProtocolServerAdapter(const std::string & listen_host_, const char * port_name_, const std::string & description_, std::unique_ptr<GRPCServer> grpc_server_);
#endif

    /// Starts the server. A new thread will be created that waits for and accepts incoming connections.
    void start() { impl->start(); }

    /// Stops the server. No new connections will be accepted.
    void stop() { impl->stop(); }

    bool isStopping() const { return impl->isStopping(); }

    /// Returns the number of currently handled connections.
    size_t currentConnections() const { return impl->currentConnections(); }

    /// Returns the number of current threads.
    size_t currentThreads() const { return impl->currentThreads(); }

    /// Returns the port this server is listening to.
    UInt16 portNumber() const { return impl->portNumber(); }

    const std::string & getListenHost() const { return listen_host; }

    const std::string & getPortName() const { return port_name; }

    const std::string & getDescription() const { return description; }

private:
    class Impl
    {
    public:
        virtual ~Impl() = default;
        virtual void start() = 0;
        virtual void stop() = 0;
        virtual bool isStopping() const = 0;
        virtual UInt16 portNumber() const = 0;
        virtual size_t currentConnections() const = 0;
        virtual size_t currentThreads() const = 0;
    };
    class TCPServerAdapterImpl;
    class GRPCServerAdapterImpl;

    std::string listen_host;
    std::string port_name;
    std::string description;
    std::unique_ptr<Impl> impl;
};

}
