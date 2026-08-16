#pragma once

#include "config.h"

#if USE_SILK

#include <Frontend.h>
#include <HealthMonitor.h>
#include <ProxyConfig.h>
#include <Router.h>

#include <Common/Logger.h>

#include <atomic>
#include <memory>
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
    bool acme_enabled = false;
#endif

    std::vector<std::unique_ptr<FrontendContext>> frontend_contexts;
    std::vector<std::unique_ptr<Poco::Net::ServerSocket>> listen_sockets;

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
