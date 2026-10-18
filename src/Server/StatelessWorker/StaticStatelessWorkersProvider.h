#pragma once

#include <Server/StatelessWorker/IStatelessWorkersProvider.h>
#include <Server/StatelessWorker/StatelessWorkerAllocation.h>
#include <base/types.h>
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
class StaticStatelessWorkersProvider final : public IStatelessWorkersProvider
{
public:
    /// `config_prefix` is the cluster's configuration path. Throws on a broken configuration.
    StaticStatelessWorkersProvider(const Poco::Util::AbstractConfiguration & config, const String & config_prefix);
    ~StaticStatelessWorkersProvider() override = default;

    /// Returns a per-query allocation over the workers snapshotted at construction.
    StatelessWorkerAllocationPtr allocate(size_t desired_workers, uint64_t provisioning_timeout_ms) override;

    [[nodiscard]] std::vector<StatelessWorkerState> getWorkersState() const override;

    /// A static cluster serves its fixed snapshot regardless of the request, so plan for that many.
    [[nodiscard]] size_t getPlanningNodeCount(size_t /*requested_workers*/) const override { return addresses.size(); }

    [[nodiscard]] std::string getAuthToken() const override { return {}; }

    void start() override {}
    void shutdown() override {}

    /// The worker addresses for the cluster at `config_prefix` in the current server configuration.
    /// Also signs the configuration, so a `remote_servers` change under `<cluster>` rebuilds the provider.
    static std::vector<StatelessWorkerAddress> resolveWorkerAddresses(
        const Poco::Util::AbstractConfiguration & config, const String & config_prefix);

private:
    /// The worker addresses resolved from the configuration when the provider was built.
    std::vector<StatelessWorkerAddress> addresses;
};

}
