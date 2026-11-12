#pragma once

#include <Core/BackgroundSchedulePoolTaskHolder.h>
#include <Server/StatelessWorker/IStatelessWorkersProvider.h>
#include <Server/StatelessWorker/StatelessWorkerAllocation.h>
#include <base/defines.h>
#include <base/types.h>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <vector>

namespace Poco
{
namespace Util
{
    class AbstractConfiguration;
}
}

namespace DB
{

/// Provider for a statically-configured worker cluster: either `<cluster>` (a single-shard
/// `remote_servers` cluster whose replicas are the workers, with their `stateless_worker_port` and
/// `streaming_exchange_port`) or `<host>` (one host with default ports). The workers are resolved once,
/// when the provider is built; a config change rebuilds the provider rather than updating it.
/// A query does not use the replicas of `<cluster>` that are known to be inactive.
class StaticStatelessWorkersProvider final : public IStatelessWorkersProvider
{
public:
    /// `config_prefix` is the cluster's configuration path. Throws on a broken configuration.
    StaticStatelessWorkersProvider(const Poco::Util::AbstractConfiguration & config, const String & config_prefix);
    /// A provider for exactly these workers.
    explicit StaticStatelessWorkersProvider(std::vector<StatelessWorkerAddress> addresses_);
    ~StaticStatelessWorkersProvider() override = default;

    /// Returns a per-query allocation over the workers snapshotted at construction.
    StatelessWorkerAllocationPtr allocate(size_t desired_workers, uint64_t provisioning_timeout_ms) override;

    std::vector<StatelessWorkerState> getWorkersState() const override;

    /// A static cluster serves its fixed snapshot regardless of the request, so plan for that many.
    size_t getPlanningNodeCount(size_t /*requested_workers*/) const override { return addresses.size(); }

    /// For `<cluster>`: a provider for the active replicas. A task sent to an inactive replica
    /// (shut down, removed, or not synced yet) would fail the whole query. The liveness can come from
    /// Keeper, so a background task reads it while queries come, and a query takes the last result.
    /// When the task does not run, one query reads the liveness itself and the others wait for it.
    StatelessWorkersProviderPtr tryGetQuerySnapshot() override;

    std::string getAuthToken() const override { return {}; }

    void start() override {}
    void shutdown() override;

    /// The worker addresses for the cluster at `config_prefix` in the current server configuration.
    /// Also signs the configuration, so a `remote_servers` change under `<cluster>` rebuilds the provider.
    static std::vector<StatelessWorkerAddress> resolveWorkerAddresses(
        const Poco::Util::AbstractConfiguration & config, const String & config_prefix);

private:
    /// A provider for the replicas of `cluster_name` that are active now. Null if there is no usable
    /// liveness; then all configured workers are used.
    StatelessWorkersProviderPtr makeActiveWorkersProvider() const;
    /// The body of `refresh_task`: updates `active_workers`, and stops when no query comes.
    void refreshActiveWorkers();

    /// The configured `<cluster>`; empty for `<host>` and for a query snapshot.
    String cluster_name;
    /// The worker addresses resolved from the configuration when the provider was built.
    std::vector<StatelessWorkerAddress> addresses;

    /// Short, because a replica that stops can get tasks until the next read of the liveness.
    static constexpr std::chrono::milliseconds refresh_period{1000};
    /// The refresh stops after this time without queries, so an idle server sends nothing to Keeper.
    static constexpr std::chrono::seconds refresh_idle_time{60};

    std::mutex active_workers_mutex;
    /// The last result of `makeActiveWorkersProvider`.
    StatelessWorkersProviderPtr active_workers TSA_GUARDED_BY(active_workers_mutex);
    /// True while `refresh_task` keeps `active_workers` up to date.
    bool is_refresh_running TSA_GUARDED_BY(active_workers_mutex) = false;
    /// True while a query reads the liveness itself. The other queries wait on `query_read_finished`,
    /// so that only one read is in progress.
    bool is_query_read_running TSA_GUARDED_BY(active_workers_mutex) = false;
    std::condition_variable query_read_finished;
    std::chrono::steady_clock::time_point last_query_time TSA_GUARDED_BY(active_workers_mutex);
    /// Empty for `<host>` and for a query snapshot. It is the last member, so that it stops before the
    /// members that it uses are destroyed.
    BackgroundSchedulePoolTaskHolder refresh_task;
};

}
