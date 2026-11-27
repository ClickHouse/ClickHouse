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
#include <vector>

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
///   - a TCP connect (with a TLS handshake for a secure backend) measures latency and liveness. The health
///     of a backend is tracked per port (see `Backend::isAlive`), so every port that a listener reaches
///     the backend on is probed, and the result applies to the connections of the protocols served on it.
///     The backend's `health_check_port`, if configured, is probed too and stands for the backend as a whole;
///   - if the backend has monitoring credentials, an HTTP(S) query reads its CPU and memory usage.
/// Backends are discovered from the router (both statically configured and dynamically created ones).
class HealthMonitor
{
public:
#if USE_SSL
    /// @p client_tls_context is used to check and poll secure backends; may be null if there are none.
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

    /// Probe one port of a backend once with a liveness connect, and record the result under @p health_key:
    /// the port itself, or `Backend::whole_backend` for the probe of `health_check_port`.
    /// Public so it can run on a fiber.
    void probePort(Backend & backend, UInt16 health_key, UInt16 port);

    /// Read the resource usage of a backend. The supervisor decides whether it is due, so that
    /// `resource_poll_interval_ms` is honored independently of the liveness `interval_ms`.
    /// Public so it can run on a fiber.
    void pollResources(Backend & backend);

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
    std::vector<BackendPtr> collectBackends() const;
};

}

#endif
