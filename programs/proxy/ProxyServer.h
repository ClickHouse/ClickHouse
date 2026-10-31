#pragma once

#include "config.h"

#if USE_SILK

#include <Frontend.h>
#include <HealthMonitor.h>
#include <ProxyConfig.h>
#include <Router.h>

#include <Common/Logger.h>

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace Poco::Net
{
class ServerSocket;
}

namespace Poco::Util
{
class AbstractConfiguration;
}

namespace DB::Proxy
{

/// The connections being handled, so that the server can end them and wait for their handlers at shutdown.
class ConnectionRegistry
{
public:
    /// A handler of the connection with the client socket @p fd is starting / its fiber has completed.
    void add(int fd);
    void remove(int fd);

    /// Shuts down the client sockets, which ends the handshakes and the relays of their connections.
    void shutdownAll();

    /// Waits until every handler has completed. Called from a thread outside of the fiber scheduler.
    void waitAll();

private:
    std::mutex mutex;
    std::condition_variable all_finished;
    size_t handlers = 0;
    std::unordered_map<int, size_t> fds;     /// A descriptor may be reused while its previous handler is completing.
};

/// The proxy engine: binds the listeners, runs the silk fiber scheduler, and dispatches every
/// accepted connection to the handler for its protocol. Also owns the router and the health monitor.
class ProxyServer
{
public:
    ProxyServer(const Poco::Util::AbstractConfiguration & abstract_config, LoggerPtr log_);
    ~ProxyServer();

    void start(const Poco::Util::AbstractConfiguration & abstract_config);
    void stop();

private:
    ProxyConfiguration config;
    LoggerPtr log;

    std::unique_ptr<Router> router;
    std::unique_ptr<HealthMonitor> health;

#if USE_SSL
    Poco::Net::Context::Ptr server_tls_context;
    Poco::Net::Context::Ptr client_tls_context;
#endif

    std::vector<std::unique_ptr<FrontendContext>> frontend_contexts;
    std::vector<std::unique_ptr<Poco::Net::ServerSocket>> listen_sockets;

    ConnectionRegistry connections;

    std::atomic<bool> stopped {false};
    bool silk_initialized = false;

    /// Held so accept-loop fibers can deliver their results; waited on at shutdown.
    struct AcceptState;
    std::vector<std::unique_ptr<AcceptState>> accept_states;

    void bindAndListen();
    bool anySecureListener() const;
    bool anyBackendSecure() const;
};

}

#endif
