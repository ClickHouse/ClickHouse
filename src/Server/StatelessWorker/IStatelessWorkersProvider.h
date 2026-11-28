#pragma once

#include <atomic>
#include <cstdint>
#include <ctime>
#include <optional>
#include <string>
#include <utility>
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

/// Provider of stateless workers for one worker cluster; hands out per-query worker allocations.
class IStatelessWorkersProvider : private boost::noncopyable
{
public:
    virtual ~IStatelessWorkersProvider() = default;

    /// Create a per-query allocation of workers.
    virtual StatelessWorkerAllocationPtr allocate(size_t desired_workers, uint64_t provisioning_timeout_ms) = 0;

    /// Like `allocate`, but returns nullptr instead of throwing when no worker can be provided.
    virtual StatelessWorkerAllocationPtr tryAllocate(size_t desired_workers, uint64_t provisioning_timeout_ms) = 0;

    /// Snapshot of all workers this provider knows about, for introspection.
    [[nodiscard]] virtual std::vector<StatelessWorkerState> getWorkersState() const = 0;

    /// Number of nodes to plan a query for, given the requested worker count.
    [[nodiscard]] virtual size_t getPlanningNodeCount(size_t requested_workers) const = 0;

    /// Returns a provider with the workers that are usable now, if the usable workers can change
    /// while the provider lives (e.g. when a replica stops). A query takes it one time and then plans
    /// with it and allocates from it, so both steps see the same workers. Null means that the query
    /// uses this provider.
    virtual std::shared_ptr<IStatelessWorkersProvider> tryGetQuerySnapshot() { return nullptr; }

    /// The token to present when dialing this provider's workers; empty when there is none.
    [[nodiscard]] virtual std::string getAuthToken() const = 0;

    /// Begin/stop the provider's background work, if any.
    virtual void start() = 0;
    virtual void shutdown() = 0;

    /// Re-apply the server configuration to a running provider; `config_prefix` is its cluster's
    /// configuration path.
    virtual void reloadConfig(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
    {
        (void)config;
        (void)config_prefix;
    }

    /// Called on a config reload for a retired provider, whose cluster may have been removed or redefined
    /// since it was built. Re-reads its credentials only from sources that `config` shares across all
    /// clusters, never from the new definition of its cluster.
    virtual void refreshSharedCredentials(const Poco::Util::AbstractConfiguration & config) { (void)config; }

    /// When the provider was retired because its cluster was removed or redefined while queries still used
    /// it; unset while it serves the current configuration.
    std::optional<time_t> getRetireTime() const
    {
        const time_t retired_at = retire_time.load();
        return retired_at ? std::optional<time_t>(retired_at) : std::nullopt;
    }

    /// Called by the registry when it retires the provider.
    void markRetired() { retire_time.store(::time(nullptr)); }

    /// The registry key of the provider's cluster: empty for the default cluster. Set by the registry before
    /// it hands the provider out, and never changed afterwards.
    const std::string & getClusterName() const { return cluster_name; }
    void setClusterName(std::string name) { cluster_name = std::move(name); }

protected:
    friend class StatelessWorkerAllocation;

    /// Return the workers of a destroyed allocation (by worker_id). Nothing to return by default.
    virtual void release(const std::vector<std::string> & worker_ids) { (void)worker_ids; }

private:
    std::atomic<time_t> retire_time = 0;
    std::string cluster_name;
};

using StatelessWorkersProviderPtr = std::shared_ptr<IStatelessWorkersProvider>;

}
