#include <Server/StatelessWorker/StaticStatelessWorkersProvider.h>
#include <Server/StatelessWorker/StatelessWorkersProviderFactory.h>
#include <Server/StatelessWorker/StatelessWorkerAllocation.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/Context.h>
#include <Common/Exception.h>
#include <Poco/Util/AbstractConfiguration.h>

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

        const auto & replicas = shard_addresses[0];

        /// Skip replicas that are known to be inactive (shut down, removed, or not synced yet):
        /// a task sent to such a replica would fail the whole query. Liveness that does not match
        /// the cluster, or that reports no active replica, is not usable: then all replicas are workers.
        std::vector<bool> is_active = ClusterProxy::getReplicasLiveness(cluster_name, cluster);
        if (is_active.size() != replicas.size() || std::ranges::find(is_active, true) == is_active.end())
            is_active.assign(replicas.size(), true);

        addresses.reserve(replicas.size());
        for (size_t replica_index = 0; replica_index < replicas.size(); ++replica_index)
        {
            if (!is_active[replica_index])
                continue;
            const auto & replica = replicas[replica_index];
            addresses.push_back({replica.host_name, replica.stateless_worker_port, replica.streaming_exchange_port});
        }
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
    : StaticStatelessWorkersProvider(resolveWorkerAddresses(config, config_prefix))
{
}

StaticStatelessWorkersProvider::StaticStatelessWorkersProvider(std::vector<StatelessWorkerAddress> addresses_)
    : addresses(std::move(addresses_))
{
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
    auto kind = [](const Poco::Util::AbstractConfiguration & config, const String & config_prefix) -> StatelessWorkersProviderFactory::ResolvedProvider
    {
        auto addresses = StaticStatelessWorkersProvider::resolveWorkerAddresses(config, config_prefix);

        /// Sign the resolved workers, not the literal `cluster` name, whose `remote_servers` entry can change.
        String signature;
        for (const auto & address : addresses)
            signature += address.host + ":" + std::to_string(address.stateless_worker_port)
                + ":" + std::to_string(address.streaming_exchange_port) + "|";

        return {std::move(signature), [addresses] { return std::make_shared<StaticStatelessWorkersProvider>(addresses); }};
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
