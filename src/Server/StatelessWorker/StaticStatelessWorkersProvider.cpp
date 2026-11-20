#include <Server/StatelessWorker/StaticStatelessWorkersProvider.h>
#include <Server/StatelessWorker/StatelessWorkersProviderFactory.h>
#include <Server/StatelessWorker/StatelessWorkerAllocation.h>
#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/StorageID.h>
#include <Common/Exception.h>
#include <Common/UniqueLock.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <base/scope_guard.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
}

std::vector<StatelessWorkerAddress> StaticStatelessWorkersProvider::resolveWorkerAddresses(
    const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
{
    auto global_context = Context::getGlobalContextInstance();
    if (!global_context)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "No global context to resolve static stateless workers");

    std::vector<StatelessWorkerAddress> addresses;

    const String cluster_name = config.getString(config_prefix + ".cluster", "");
    if (!cluster_name.empty())
    {
        auto cluster = global_context->tryGetCluster(cluster_name);
        if (!cluster)
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                "Stateless worker cluster '{}' not found in server configuration", cluster_name);

        const auto shard_addresses = cluster->getShardsAddresses();
        if (shard_addresses.empty())
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                "Stateless worker cluster '{}' has no shards", cluster_name);
        /// Only a single-shard worker cluster is supported.
        if (shard_addresses.size() > 1)
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                "Stateless worker cluster '{}' must have a single shard, got {}",
                cluster_name, shard_addresses.size());

        addresses.reserve(shard_addresses[0].size());
        for (const auto & replica : shard_addresses[0])
            addresses.push_back({replica.host_name, replica.stateless_worker_port, replica.streaming_exchange_port});
    }
    else
    {
        const String host = config.getString(config_prefix + ".host", "");
        if (!host.empty())
            addresses.push_back({host, 0, 0});
    }

    if (addresses.empty())
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "No hosts specified for the static stateless worker client configuration");

    return addresses;
}

StaticStatelessWorkersProvider::StaticStatelessWorkersProvider(
    const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
    : cluster_name(config.getString(config_prefix + ".cluster", ""))
    , addresses(resolveWorkerAddresses(config, config_prefix))
{
    if (!cluster_name.empty())
        refresh_task = Context::getGlobalContextInstance()->getSchedulePool()->createTask(
            StorageID::createEmpty(), "StaticStatelessWorkersProvider", [this] { refreshActiveWorkers(); });
}

StaticStatelessWorkersProvider::StaticStatelessWorkersProvider(std::vector<StatelessWorkerAddress> addresses_)
    : addresses(std::move(addresses_))
{
}

StatelessWorkersProviderPtr StaticStatelessWorkersProvider::tryGetQuerySnapshot()
{
    if (cluster_name.empty())
        return nullptr;

    {
        UniqueLock lock(active_workers_mutex);
        last_query_time = std::chrono::steady_clock::now();
        query_read_finished.wait(
            lock.getUnderlyingLock(),
            [this]() TSA_REQUIRES(active_workers_mutex) { return is_refresh_running || !is_query_read_running; });
        if (is_refresh_running)
            return active_workers;
        is_query_read_running = true;
    }
    SCOPE_EXIT({
        std::lock_guard lock(active_workers_mutex);
        is_query_read_running = false;
        query_read_finished.notify_all();
    });

    /// The refresh task does not run: this is the first query, or no query came for `refresh_idle_time`.
    /// This query reads the liveness itself and starts the task. The read can go to Keeper, so it runs
    /// without the lock. The queries that come during the read wait for its result.
    auto current_active_workers = makeActiveWorkersProvider();

    std::lock_guard lock(active_workers_mutex);
    active_workers = current_active_workers;
    /// If the task is not scheduled, or the call throws, the next query does the read again.
    is_refresh_running = refresh_task->scheduleAfter(refresh_period.count());
    return current_active_workers;
}

void StaticStatelessWorkersProvider::refreshActiveWorkers()
{
    {
        std::lock_guard lock(active_workers_mutex);
        if (std::chrono::steady_clock::now() - last_query_time > refresh_idle_time)
        {
            is_refresh_running = false;
            return;
        }
    }

    try
    {
        auto current_active_workers = makeActiveWorkersProvider();

        std::lock_guard lock(active_workers_mutex);
        active_workers = std::move(current_active_workers);
        is_refresh_running = refresh_task->scheduleAfter(refresh_period.count());
    }
    catch (...)
    {
        /// A task of the pool must not throw. The refresh stops, so the next query does the read itself,
        /// and an error of that read goes to the query.
        tryLogCurrentException(__PRETTY_FUNCTION__);
        std::lock_guard lock(active_workers_mutex);
        is_refresh_running = false;
    }
}

void StaticStatelessWorkersProvider::shutdown()
{
    if (refresh_task)
        refresh_task->deactivate();
}

StatelessWorkersProviderPtr StaticStatelessWorkersProvider::makeActiveWorkersProvider() const
{
    auto cluster = Context::getGlobalContextInstance()->tryGetCluster(cluster_name);
    if (!cluster)
        return nullptr;

    /// The replicas and their liveness come from the same cluster, so their order is the same.
    const auto shard_addresses = cluster->getShardsAddresses();
    const std::vector<bool> is_active = ClusterProxy::getReplicasLiveness(cluster_name, cluster);

    /// Liveness that is unknown, that does not match the cluster, or that reports no active replica
    /// is not usable.
    if (shard_addresses.size() != 1 || is_active.size() != shard_addresses[0].size()
        || std::ranges::find(is_active, true) == is_active.end())
        return nullptr;

    std::vector<StatelessWorkerAddress> active_addresses;
    for (size_t replica_index = 0; replica_index < is_active.size(); ++replica_index)
    {
        if (!is_active[replica_index])
            continue;
        const auto & replica = shard_addresses[0][replica_index];
        active_addresses.push_back({replica.host_name, replica.stateless_worker_port, replica.streaming_exchange_port});
    }
    return std::make_shared<StaticStatelessWorkersProvider>(std::move(active_addresses));
}

StatelessWorkerAllocationPtr StaticStatelessWorkersProvider::allocate(
    size_t /*desired_workers*/, uint64_t /*provisioning_timeout_ms*/)
{
    /// No provider and no worker IDs: nothing to return when the allocation is destroyed.
    return StatelessWorkerAllocationPtr(
        new StatelessWorkerAllocation(nullptr, {}, addresses));
}

void registerStaticStatelessWorkersProvider(StatelessWorkersProviderFactory & factory)
{
    StatelessWorkersProviderFactory::ProviderKind kind{
        .create = [](const Poco::Util::AbstractConfiguration & config, const String & config_prefix) -> StatelessWorkersProviderPtr
        {
            return std::make_shared<StaticStatelessWorkersProvider>(config, config_prefix);
        },
        /// Sign the resolved workers, because the `remote_servers` entry of `cluster` can change. Sign the
        /// `cluster` name too, because the provider reads the liveness of the cluster with this name.
        .config_signature = [](const Poco::Util::AbstractConfiguration & config, const String & config_prefix) -> String
        {
            String signature = config.getString(config_prefix + ".cluster", "") + "|";
            for (const auto & address : StaticStatelessWorkersProvider::resolveWorkerAddresses(config, config_prefix))
                signature += address.host + ":" + std::to_string(address.stateless_worker_port)
                    + ":" + std::to_string(address.streaming_exchange_port) + "|";
            return signature;
        },
    };
    factory.registerProviderKind("cluster", kind);
    factory.registerProviderKind("host", kind);
}

std::vector<StatelessWorkerState> StaticStatelessWorkersProvider::getWorkersState() const
{
    /// No refcounting: every worker is reported as cached with a zero refcount.
    std::vector<StatelessWorkerState> result;
    result.reserve(addresses.size());
    for (const auto & address : addresses)
    {
        StatelessWorkerState state;
        state.worker_id = address.host + ":" + std::to_string(address.stateless_worker_port);
        state.hostname = address.host;
        state.port = address.stateless_worker_port;
        state.status = "STATIC";
        state.cached = true;
        state.refcount = 0;
        result.push_back(std::move(state));
    }
    return result;
}

}
