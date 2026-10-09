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

/// Provider for a statically-configured worker cluster. The workers are resolved from
/// the server configuration once, when the provider is built, and `allocate` hands out a copy of
/// that snapshot.
///
/// Supports two config layouts under the cluster's config prefix:
///   - `<cluster>` — a named cluster whose single shard's replicas are the workers; each
///     replica carries `stateless_worker_port` and `streaming_exchange_port` for exchange.
///   - `<host>` — a single host with default ports (resolved by the executor).
///
/// The built provider is an immutable snapshot of its configuration: a
/// config reload rebuilds it for new queries (see the registry), while a retired provider keeps
/// serving the workers it was built for until the queries holding it drain. start / shutdown /
/// reloadConfig are no-ops.
class StaticStatelessWorkersProvider final : public IStatelessWorkersProvider
{
public:
    /// `config_prefix_` is this cluster's configuration path: `stateless_worker_client` for the
    /// default cluster, or `stateless_worker_client.clusters.<name>` for a named cluster. The workers
    /// are resolved here from `<config_prefix_>.cluster` or `<config_prefix_>.host`, applying the
    /// single-shard and non-empty guards, so a broken static config fails when the provider is built.
    explicit StaticStatelessWorkersProvider(String config_prefix_);
    ~StaticStatelessWorkersProvider() override = default;

    /// Returns a per-query allocation over the workers snapshotted at construction.
    StatelessWorkerAllocationPtr allocate(size_t desired_workers, uint64_t provisioning_timeout_ms) override;

    std::vector<StatelessWorkerState> getWorkersState() const override;

    /// A static cluster serves its fixed snapshot regardless of the request, so plan for that many.
    size_t getPlanningNodeCount(size_t /*requested_workers*/) const override { return addresses.size(); }

    std::string getAuthToken() const override { return {}; }

    void start() override {}
    void shutdown() override {}

    /// Resolve the worker addresses for the cluster at `config_prefix` from the current server
    /// configuration (the `<cluster>` remote_servers lookup or the `<host>` form, with the single-shard
    /// and non-empty guards). Throws on a broken config. Shared by the constructor (to snapshot them) and
    /// by the registry's config signature, so a `remote_servers` change under a `<cluster>` form — which
    /// leaves the literal `cluster` name unchanged — still retires and rebuilds the provider.
    static std::vector<StatelessWorkerAddress> resolveWorkerAddresses(const String & config_prefix);

private:
    /// The worker addresses resolved from the configuration when the provider was built.
    std::vector<StatelessWorkerAddress> addresses;
};

}
