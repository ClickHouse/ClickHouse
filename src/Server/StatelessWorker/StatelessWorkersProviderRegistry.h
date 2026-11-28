#pragma once

#include <map>
#include <mutex>
#include <utility>
#include <vector>
#include <boost/core/noncopyable.hpp>
#include <base/types.h>
#include <Common/logger_useful.h>
#include <Server/StatelessWorker/IStatelessWorkersProvider.h>

namespace Poco
{
namespace Util
{
    class AbstractConfiguration;
}
}

namespace DB
{

/// Owns one stateless-worker provider per configured worker cluster and resolves a query's
/// `distributed_plan_worker_cluster` to it; the empty name is the default cluster (the flat
/// `stateless_worker_client` block). Providers are built on first use and reused across queries.
class StatelessWorkersProviderRegistry : private boost::noncopyable
{
public:
    StatelessWorkersProviderRegistry();
    ~StatelessWorkersProviderRegistry();

    /// The provider for `cluster_name` (empty = the default cluster), building it on first use.
    /// Throws when the selection is invalid.
    StatelessWorkersProviderPtr getProvider(const String & cluster_name);

    /// The provider for `cluster_name` if it is already built, otherwise null.
    StatelessWorkersProviderPtr tryGetProvider(const String & cluster_name);

    /// The providers built so far: the current ones, and the retired ones that queries still hold (see
    /// `IStatelessWorkersProvider::getRetireTime`). Several can have the same cluster name. Builds none.
    std::vector<StatelessWorkersProviderPtr> getBuiltProviders();

    /// The name to show for the cluster with registry key `cluster_name`: the default cluster's
    /// `default_cluster_name`, or empty when it has none.
    static String getClusterDisplayName(const Poco::Util::AbstractConfiguration & config, const String & cluster_name);

    /// On a config reload, retire providers whose cluster was removed or reconfigured (queries holding
    /// them drain on them) and refresh the others in place. Never throws: a cluster whose new
    /// configuration cannot be applied is retired, and the error surfaces to the next query that selects it.
    /// A retired provider that queries still hold re-reads its credentials only from sources the new
    /// configuration shares across clusters, never from its cluster's new definition, so a cluster
    /// redefined under the same name cannot redirect it or hand it credentials.
    void reloadConfig(const Poco::Util::AbstractConfiguration & config);

    /// Shut down and drop every provider, retired ones included (process teardown).
    void shutdown();

    /// The registry key of the cluster a `distributed_plan_worker_cluster` value selects: empty for the
    /// default cluster (an empty value or `default_cluster_name`), the name otherwise. Throws for an
    /// unknown cluster, one without exactly one worker source, or an empty value without a default.
    static String canonicalizeClusterName(const Poco::Util::AbstractConfiguration & config, const String & worker_cluster);

    /// The configuration subtree of a canonical cluster key: the flat `stateless_worker_client` block
    /// for the default cluster (empty key), or `stateless_worker_client.clusters.<name>` for a named one.
    static String clusterConfigPrefix(const String & canonical_cluster_name);

private:
    struct ProviderEntry
    {
        StatelessWorkersProviderPtr provider;
        String config_signature;
    };

    LoggerPtr log;
    std::mutex mutex;
    std::map<String, ProviderEntry> providers TSA_GUARDED_BY(mutex);
    /// Weak, so the queries holding a retired provider alone decide how long it lives.
    std::vector<std::weak_ptr<IStatelessWorkersProvider>> retired_providers TSA_GUARDED_BY(mutex);

    /// Remember a provider just removed from `providers`; forget the retired ones no query holds any more.
    void retireProvider(const StatelessWorkersProviderPtr & provider) TSA_REQUIRES(mutex);

    /// The retired providers that queries still hold; forgets the others.
    std::vector<StatelessWorkersProviderPtr> lockRetiredProviders() TSA_REQUIRES(mutex);

    /// Build the provider for `cluster_name` from the current server configuration.
    StatelessWorkersProviderPtr buildProvider(const Poco::Util::AbstractConfiguration & config, const String & cluster_name) TSA_REQUIRES(mutex);

    /// A digest of the configuration `cluster_name` is built from; empty when it is no longer configured.
    /// A reload rebuilds only a cluster whose digest changed.
    static String computeConfigSignatureForCluster(const Poco::Util::AbstractConfiguration & config, const String & cluster_name);
};

using StatelessWorkersProviderRegistryPtr = std::shared_ptr<StatelessWorkersProviderRegistry>;

}
