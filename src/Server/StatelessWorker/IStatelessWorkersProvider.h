#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <memory>
#include <boost/core/noncopyable.hpp>
#include <Server/StatelessWorker/StatelessWorkerAllocation_fwd.h>

namespace Poco
{
namespace Util
{
    class AbstractConfiguration;
}
}

namespace DB
{

/// Snapshot of one worker as seen by a provider (everything the query initiator
/// knows about it), for introspection.
struct StatelessWorkerState
{
    std::string worker_id;
    std::string hostname;
    uint16_t port = 0;
    bool secure = false;
    uint64_t worker_version = 0;
    uint64_t memory_bytes = 0;
    uint64_t cpu_cores = 0;
    std::string status;
    std::string tenant_id;
    int64_t lease_expiration_time = 0;
    /// Graceful-shutdown marker: 0 while healthy, a positive value (the worker's
    /// absolute shutdown deadline in epoch seconds) once it is draining and thus
    /// excluded from new allocations.
    uint64_t shutdown_deadline = 0;
    /// Number of active allocations on this server referencing the worker.
    uint64_t refcount = 0;
    /// False for a worker that is referenced by a running query but has
    /// already been dropped from the provider's cache.
    bool cached = false;
};

/// Provider of stateless workers for one worker cluster. A query
/// selects a cluster (via the `distributed_plan_worker_cluster` setting) and the
/// provider registry resolves it to one provider that hands out per-query worker
/// allocations. `StaticStatelessWorkersProvider` serves a fixed set of workers; other
/// implementations acquire workers dynamically.
class IStatelessWorkersProvider : private boost::noncopyable
{
public:
    virtual ~IStatelessWorkersProvider() = default;

    /// Create a per-query allocation of workers. Returns a lightweight RAII handle
    /// (`StatelessWorkerAllocation`) carrying the worker endpoints to dial.
    virtual StatelessWorkerAllocationPtr allocate(size_t desired_workers, uint64_t provisioning_timeout_ms) = 0;

    /// Snapshot of all workers this provider knows about (backs `system.stateless_workers`).
    virtual std::vector<StatelessWorkerState> getWorkersState() const = 0;

    /// Number of nodes the optimizer should plan the query for when this provider serves it: a
    /// dynamic provider acquires up to `requested_workers`, a static provider serves its fixed
    /// configured set regardless of the request. Lets plan sizing and the allocation size from the same
    /// provider instance the query resolves (see `Context::getStatelessWorkersProvider`).
    virtual size_t getPlanningNodeCount(size_t requested_workers) const = 0;

    /// The token to present when dialing this provider's workers, or empty when there is none
    /// (static clusters carry no token).
    virtual std::string getAuthToken() const = 0;

    /// Begin/stop the provider's background work.
    /// A no-op for providers that do not run background threads.
    virtual void start() = 0;
    virtual void shutdown() = 0;

    /// Re-apply the server configuration to a running provider (e.g. re-read a token file
    /// after it changed). Providers with nothing to reload leave this a no-op.
    virtual void reloadConfig(const Poco::Util::AbstractConfiguration & config) { (void)config; }

protected:
    friend class StatelessWorkerAllocation;

    /// Return the workers of a destroyed allocation (by worker_id). Nothing to return by default.
    virtual void release(const std::vector<std::string> & worker_ids) { (void)worker_ids; }
};

using StatelessWorkersProviderPtr = std::shared_ptr<IStatelessWorkersProvider>;

}
