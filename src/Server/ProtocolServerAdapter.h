#pragma once

#include "config.h"

#include <Server/ServerType.h>

#include <base/types.h>
#include <memory>
#include <string>


namespace DB
{

class IGRPCServer;
class TCPServer;

/// Provides an unified interface to access a protocol implementing server
/// no matter what type it has (HTTPServer, TCPServer, MySQLServer, GRPCServer, ...).
class ProtocolServerAdapter
{
    friend class ProtocolServers;
public:
    ProtocolServerAdapter(ProtocolServerAdapter && src) = default;
    ProtocolServerAdapter & operator =(ProtocolServerAdapter && src) = default;
    ProtocolServerAdapter(
        const std::string & listen_host_,
        const char * port_name_,
        ServerType::Type protocol_type_,
        const std::string & description_,
        std::unique_ptr<TCPServer> tcp_server_,
        bool supports_runtime_reconfiguration_ = true);

#if USE_GRPC
    ProtocolServerAdapter(
        const std::string & listen_host_,
        const char * port_name_,
        ServerType::Type protocol_type_,
        const std::string & description_,
        std::unique_ptr<IGRPCServer> grpc_server_,
        bool supports_runtime_reconfiguration_ = true);
#endif

    /// Starts the server. A new thread will be created that waits for and accepts incoming connections.
    /// Does nothing if the server has already been started: a server may be started ahead of the
    /// common start loop (e.g. Prometheus starts before tables are loaded), and the underlying
    /// implementation does not support being started twice.
    void start()
    {
        if (started)
            return;
        impl->start();
        started = true;
    }

    /// Stops the server. No new connections will be accepted.
    void stop() { impl->stop(); }

    bool isStopping() const { return impl->isStopping(); }

    /// Returns the number of currently handled connections.
    size_t currentConnections() const { return impl->currentConnections(); }

    size_t refusedConnections() const { return impl->refusedConnections(); }

    /// Returns the number of current threads.
    size_t currentThreads() const { return impl->currentThreads(); }

    /// Returns the port this server is listening to.
    UInt16 portNumber() const { return impl->portNumber(); }

    /// Whether the listening socket is bound by `start` instead of when this adapter is created.
    /// gRPC-based servers (gRPC and Arrow Flight) let gRPC own the socket, so for them binding - and
    /// therefore a possible `EADDRINUSE` - happens on `start`, while every other protocol is already
    /// bound and listening by the time the adapter exists.
    bool bindsOnStart() const { return impl->bindsOnStart(); }

    bool supportsRuntimeReconfiguration() const { return supports_runtime_reconfiguration; }

    const std::string & getListenHost() const { return listen_host; }

    const std::string & getPortName() const { return port_name; }

    /// The protocol this server speaks. Used to report per-protocol metrics without matching
    /// on `port_name`: `port_name` is a config key, and a server declared under `<protocols>`
    /// has `port_name == "protocols.<name>.port"`, which no fixed list can enumerate.
    /// For such servers this is the type of the innermost layer of the protocol stack
    /// (e.g. `TCP_SECURE` for a `tls` layer over `tcp`), not `CUSTOM`.
    /// `END` means the server has no protocol type of its own (Keeper listeners).
    ServerType::Type getProtocolType() const { return protocol_type; }

    const std::string & getDescription() const { return description; }

private:
    class Impl
    {
    public:
        virtual ~Impl() = default;
        virtual void start() = 0;
        virtual void stop() = 0;
        virtual bool isStopping() const = 0;
        virtual bool bindsOnStart() const = 0;
        virtual UInt16 portNumber() const = 0;
        virtual size_t currentConnections() const = 0;
        virtual size_t currentThreads() const = 0;
        virtual size_t refusedConnections() const = 0;
    };
    class TCPServerAdapterImpl;
    class GRPCServerAdapterImpl;

    std::string listen_host;
    std::string port_name;
    ServerType::Type protocol_type;
    std::string description;
    std::unique_ptr<Impl> impl;
    bool supports_runtime_reconfiguration = true;
    bool started = false;
};

}
