#pragma once

#include <map>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>
#include <boost/core/noncopyable.hpp>
#include <base/types.h>
#include <Common/logger_useful.h>
#include <Server/StatelessWorker/IStatelessWorkersProvider.h>
#include <Server/StatelessWorker/StatelessWorkersProviderFactory.h>

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

    /// The providers built so far, by cluster name (the default cluster's `default_cluster_name`, or empty
    /// when it has none). Builds none.
    std::vector<std::pair<String, StatelessWorkersProviderPtr>> getBuiltProviders();

    /// On a config reload, retire providers whose cluster was removed or reconfigured (queries holding
    /// them drain on them) and refresh the others in place. Never throws: a cluster whose new
    /// configuration cannot be applied is retired, and the error surfaces to the next query that selects it.
    void reloadConfig(const Poco::Util::AbstractConfiguration & config);

    /// Shut down and drop every provider (process teardown).
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
        /// Identifies the state that `provider` was built from.
        String signature;
    };

    LoggerPtr log;
    std::mutex mutex;
    std::map<String, ProviderEntry> providers TSA_GUARDED_BY(mutex);

    /// Reads the state of `cluster_name` one time; empty when the cluster is no longer configured.
    /// The read can be slow, so it runs without the lock.
    static std::optional<StatelessWorkersProviderFactory::ResolvedProvider> resolveCluster(
        const Poco::Util::AbstractConfiguration & config, const String & cluster_name);
};

using StatelessWorkersProviderRegistryPtr = std::shared_ptr<StatelessWorkersProviderRegistry>;

}
