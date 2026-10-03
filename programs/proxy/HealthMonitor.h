#pragma once

#include "config.h"

#if USE_SILK

#include <Backend.h>
#include <ProxyConfig.h>

#include <Common/Logger.h>

#include <atomic>
#include <chrono>
#include <map>
#include <memory>

#if USE_SSL
#include <Poco/Net/Context.h>
#endif

namespace silk
{
class FiberFuture;
}

namespace DB::Proxy
{

class Router;

/// Actively monitors backend health and, optionally, resource usage.
/// A supervisor fiber periodically probes every backend concurrently:
///   - a TCP connect to the backend's health check port (see `healthCheckPort`) measures latency and liveness;
///   - if the backend has monitoring credentials, an HTTP(S) query reads its CPU and memory usage.
/// Backends are discovered from the router (both statically configured and dynamically created ones).
class HealthMonitor
{
public:
#if USE_SSL
    /// @p client_tls_context is used to poll the resources of secure backends; may be null if there are none.
    HealthMonitor(const ProxyConfiguration & config_, Router & router_, Poco::Net::Context::Ptr client_tls_context_);
#else
    HealthMonitor(const ProxyConfiguration & config_, Router & router_);
#endif
    ~HealthMonitor();

    /// Spawn the supervisor fiber. Returns immediately. Throws if the fiber cannot be started.
    void start();
    void stop() { stopped.store(true, std::memory_order_relaxed); }

    /// Block until the supervisor fiber has finished (call after stop()).
    void join();

    /// Probe one backend once (a liveness connect and, when `poll_resources` is set, a resource poll).
    /// The supervisor decides whether resources are due so that `resource_poll_interval_ms` is honored
    /// independently of the liveness `interval_ms`. Public so it can run on a fiber.
    void checkBackend(Backend & backend, bool poll_resources);

private:
    const ProxyConfiguration & config;
    Router & router;
#if USE_SSL
    Poco::Net::Context::Ptr client_tls_context;
#endif
    LoggerPtr log;
    std::atomic<bool> stopped {false};

    /// Last time each backend's resource usage was polled. Touched only by the supervisor fiber.
    /// Keyed by the `Backend` object identity rather than its name: backend names are unique only
    /// within a single pool, so two backends in different pools can share a name and would otherwise
    /// clobber each other's throttle entry. Each cycle drops the entries of backends that are no longer
    /// registered (evicted dynamic backends), so a stale pointer never outlives its backend's registration.
    std::map<Backend *, std::chrono::steady_clock::time_point> last_resource_poll;

    std::unique_ptr<silk::FiberFuture> supervisor_future;

    void superviseLoop();
    void interruptibleSleep(UInt64 total_ms);
    void pollResources(Backend & backend);
    std::vector<BackendPtr> collectBackends() const;
};

}

#endif
