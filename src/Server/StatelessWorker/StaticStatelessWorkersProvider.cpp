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
    : addresses(resolveWorkerAddresses(config, config_prefix))
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
    StatelessWorkersProviderFactory::ProviderKind kind{
        .create = [](const Poco::Util::AbstractConfiguration & config, const String & config_prefix) -> StatelessWorkersProviderPtr
        {
            return std::make_shared<StaticStatelessWorkersProvider>(config, config_prefix);
        },
        /// Sign the resolved workers, not the literal `cluster` name, whose `remote_servers` entry can change.
        .config_signature = [](const Poco::Util::AbstractConfiguration & config, const String & config_prefix) -> String
        {
            String signature;
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
