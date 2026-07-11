#pragma once

#include "config.h"

#if USE_SILK

#include "Backend.h"
#include "ProxyConfig.h"

#include <Common/Logger.h>

#include <atomic>
#include <chrono>
#include <map>
#include <memory>

namespace silk
{
class FiberFuture;
}

namespace DB::Proxy
{

class Router;

/// Actively monitors backend health and, optionally, resource usage.
/// A supervisor fiber periodically probes every backend concurrently:
///   - a TCP connect to the native port measures latency and liveness;
///   - if the backend has monitoring credentials, an HTTP query reads its CPU and memory usage.
/// Backends are discovered from the router (both statically configured and dynamically created ones).
class HealthMonitor
{
public:
    HealthMonitor(const ProxyConfiguration & config_, Router & router_);
    ~HealthMonitor();

    /// Spawn the supervisor fiber. Returns immediately.
    void start();
    void stop() { stopped.store(true, std::memory_order_relaxed); }

    /// Block until the supervisor fiber has finished (call after stop()).
    void join();

    /// Probe one backend once (connect + optional resource poll). Public so it can run on a fiber.
    void checkBackend(Backend & backend);

private:
    const ProxyConfiguration & config;
    Router & router;
    LoggerPtr log;
    std::atomic<bool> stopped {false};

    /// Last time each backend's resource usage was polled. Touched only by the supervisor fiber.
    std::map<String, std::chrono::steady_clock::time_point> last_resource_poll;

    std::unique_ptr<silk::FiberFuture> supervisor_future;

    void superviseLoop();
    void interruptibleSleep(UInt64 total_ms);
    void pollResources(Backend & backend);
    std::vector<BackendPtr> collectBackends() const;
};

}

#endif
