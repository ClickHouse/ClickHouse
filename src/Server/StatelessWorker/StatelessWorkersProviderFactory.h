#pragma once

#include <Server/StatelessWorker/IStatelessWorkersProvider.h>
#include <base/types.h>
#include <boost/core/noncopyable.hpp>
#include <functional>
#include <map>

namespace Poco
{
namespace Util
{
    class AbstractConfiguration;
}
}

namespace DB
{

/// Builds stateless-worker providers by kind. Each kind recognizes its own worker-source keys in a
/// cluster's configuration; the registry requires exactly one source per cluster and builds the
/// provider of the kind that owns it.
class StatelessWorkersProviderFactory : private boost::noncopyable
{
public:
    struct ProviderKind
    {
        /// How many worker sources of this kind the cluster at `config_prefix` configures.
        std::function<size_t(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)> count_worker_sources;
        /// Builds and starts the provider for the cluster at `config_prefix`.
        std::function<StatelessWorkersProviderPtr(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)> create;
        /// A digest of the configuration `create` reads, so a config reload rebuilds the provider only on change.
        std::function<String(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)> config_signature;
    };

    static StatelessWorkersProviderFactory & instance();

    void registerProviderKind(const String & name, ProviderKind kind);

    /// Total number of worker sources the cluster at `config_prefix` configures, over all kinds.
    size_t countWorkerSources(const Poco::Util::AbstractConfiguration & config, const String & config_prefix) const;

    /// The kind (name and callbacks) that serves the cluster at `config_prefix`. The cluster must configure
    /// exactly one worker source.
    const std::pair<const String, ProviderKind> & getKind(const Poco::Util::AbstractConfiguration & config, const String & config_prefix) const;

private:
    StatelessWorkersProviderFactory();

    std::map<String, ProviderKind> kinds;
};

/// Each provider kind registers itself; the discovery kind exists only in ClickHouse Cloud builds.
void registerStaticStatelessWorkersProvider(StatelessWorkersProviderFactory & factory);
void registerDiscoveryStatelessWorkersProvider(StatelessWorkersProviderFactory & factory);

}
