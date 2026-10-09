#pragma once

#include <map>
#include <mutex>
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

/// Owns the stateless-worker providers — one per configured worker cluster —
/// and resolves a query's cluster selection to the provider that serves it. A query names
/// its cluster via the `distributed_plan_worker_cluster` setting; the empty name is the
/// default cluster (the flat `stateless_worker_client` config), which preserves the
/// single-cluster behaviour. Providers are built lazily on first use and reused across
/// queries. Keeping this out of Context isolates the per-cluster lifecycle from the rest
/// of the server context.
class StatelessWorkersProviderRegistry : private boost::noncopyable
{
public:
    StatelessWorkersProviderRegistry();
    ~StatelessWorkersProviderRegistry();

    /// The provider for `cluster_name` (empty = the default cluster), building it on first
    /// use. Throws when the named cluster is not configured.
    StatelessWorkersProviderPtr getProvider(const String & cluster_name);

    /// The provider for `cluster_name` if it has already been built, otherwise nullptr;
    /// never instantiates one (used for introspection).
    StatelessWorkersProviderPtr tryGetProvider(const String & cluster_name);

    /// Re-apply the server configuration on a config reload: retire providers whose cluster was
    /// removed or reconfigured (in-flight queries keep them alive and drain), and refresh the
    /// unchanged ones in place (e.g. re-read a token file after it changed). Added
    /// clusters are built lazily on first use.
    /// Never throws: a cluster whose new configuration cannot be applied is retired, and the error
    /// surfaces to the next query that selects it.
    void reloadConfig(const Poco::Util::AbstractConfiguration & config);

    /// Shut down and drop every provider (process teardown).
    void shutdown();

    /// Resolve a `distributed_plan_worker_cluster` setting value to the canonical registry key of the
    /// cluster it selects: the empty string for the default cluster (selected by an empty value, or by
    /// the configured `default_cluster_name`), or the named cluster otherwise. Throws
    /// NO_ELEMENTS_IN_CONFIG when the selection is invalid — an unknown name, a cluster that names no
    /// worker source, or an empty value when no default cluster is configured. Shared by plan sizing
    /// and allocation so both resolve a query's cluster identically.
    static String canonicalizeClusterName(const Poco::Util::AbstractConfiguration & config, const String & worker_cluster);

    /// The configuration subtree of a canonical cluster key: the flat `stateless_worker_client` block
    /// for the default cluster (empty key), or `stateless_worker_client.clusters.<name>` for a named one.
    static String clusterConfigPrefix(const String & canonical_cluster_name);

private:
    /// A built provider plus a digest of the configuration it was built from, so a reload can
    /// tell whether the cluster's configuration changed.
    struct ProviderEntry
    {
        StatelessWorkersProviderPtr provider;
        String config_signature;
    };

    LoggerPtr log;
    std::mutex mutex;
    std::map<String, ProviderEntry> providers TSA_GUARDED_BY(mutex);

    /// Build the provider for `cluster_name` from the current server configuration.
    StatelessWorkersProviderPtr buildProvider(const String & cluster_name) TSA_REQUIRES(mutex);

    /// A stable digest of `cluster_name`'s configuration (its worker source and the settings it is built from);
    /// empty when the cluster is disabled or no longer configured. Two builds with the same digest
    /// are interchangeable, so a reload rebuilds only a cluster whose digest changed.
    static String computeConfigSignature(const Poco::Util::AbstractConfiguration & config, const String & cluster_name);
};

using StatelessWorkersProviderRegistryPtr = std::shared_ptr<StatelessWorkersProviderRegistry>;

}
