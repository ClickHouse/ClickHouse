#include <Server/StatelessWorker/StaticStatelessWorkersProvider.h>
#include <Server/StatelessWorker/StatelessWorkersProviderFactory.h>
#include <Server/StatelessWorker/StatelessWorkerAllocation.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Common/Exception.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
}

std::vector<StatelessWorkerAddress> StaticStatelessWorkersProvider::resolveWorkerAddresses(const String & config_prefix)
{
    auto global_context = Context::getGlobalContextInstance();
    if (!global_context)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "No global context to resolve static stateless workers");

    const auto & config = global_context->getConfigRef();
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

StaticStatelessWorkersProvider::StaticStatelessWorkersProvider(String config_prefix_)
    /// Resolve the worker set once, now, so the provider is an immutable snapshot of its configuration
    /// (a config reload rebuilds it for new queries). A broken static config therefore fails here, when
    /// the provider is built, rather than at allocation time.
    : addresses(resolveWorkerAddresses(config_prefix_))
{
}

StatelessWorkerAllocationPtr StaticStatelessWorkersProvider::allocate(
    size_t /*desired_workers*/, uint64_t /*provisioning_timeout_ms*/)
{
    /// Hand out a copy of the workers snapshotted at construction. Static allocations carry no worker
    /// IDs and no refcounting: the provider pointer is null so the destructor skips the release call.
    return StatelessWorkerAllocationPtr(
        new StatelessWorkerAllocation(nullptr, {}, addresses));
}

void registerStaticStatelessWorkersProvider(StatelessWorkersProviderFactory & factory)
{
    factory.registerProviderKind("static", StatelessWorkersProviderFactory::ProviderKind{
        .count_worker_sources = [](const Poco::Util::AbstractConfiguration & config, const String & config_prefix) -> size_t
        {
            return (config.getString(config_prefix + ".cluster", "").empty() ? 0 : 1)
                + (config.getString(config_prefix + ".host", "").empty() ? 0 : 1);
        },
        .create = [](const Poco::Util::AbstractConfiguration &, const String & config_prefix) -> StatelessWorkersProviderPtr
        {
            return std::make_shared<StaticStatelessWorkersProvider>(config_prefix);
        },
        /// Hash the RESOLVED worker set, not just the literal `cluster`/`host`: a `<cluster>` form points
        /// at a `remote_servers` entry whose replicas/ports can change while the `cluster` name stays the
        /// same, and the provider snapshots its workers at build — so the signature must change when the
        /// resolved set does, to retire and rebuild.
        .config_signature = [](const Poco::Util::AbstractConfiguration &, const String & config_prefix) -> String
        {
            String signature;
            for (const auto & address : StaticStatelessWorkersProvider::resolveWorkerAddresses(config_prefix))
                signature += address.host + ":" + std::to_string(address.stateless_worker_port)
                    + ":" + std::to_string(address.streaming_exchange_port) + "|";
            return signature;
        },
    });
}

std::vector<StatelessWorkerState> StaticStatelessWorkersProvider::getWorkersState() const
{
    /// Report the snapshotted workers so `system.stateless_workers` shows a built static cluster — an
    /// empty result would be indistinguishable from "provider not built yet". A static cluster has no
    /// refcounting, so each worker is simply present (`cached`) with a zero refcount and a
    /// `STATIC` status.
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
