#pragma once

#include <Server/StatelessWorker/IStatelessWorkersProvider.h>
#include <Core/Types.h>
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

/// Builds stateless-worker providers. A provider kind is registered under the configuration key that
/// selects it (its worker source, e.g. `host`), relative to a cluster's configuration; a cluster sets
/// exactly one worker source.
class StatelessWorkersProviderFactory : private boost::noncopyable
{
public:
    /// The result of one read of the state of a worker cluster.
    struct ResolvedProvider
    {
        /// Identifies the state that was read. A built provider is reused while it does not change.
        String signature;
        /// Builds and starts the provider from the state that was read. It can refer to the
        /// configuration of the read, so it runs before the caller releases that configuration.
        std::function<StatelessWorkersProviderPtr()> build;
    };

    /// Reads the state of the cluster at `config_prefix`.
    using ProviderKind = std::function<ResolvedProvider(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)>;

    static StatelessWorkersProviderFactory & instance();

    void registerProviderKind(const String & source_key, ProviderKind kind);

    /// The worker sources the cluster at `config_prefix` sets.
    Strings getWorkerSources(const Poco::Util::AbstractConfiguration & config, const String & config_prefix) const;

    const ProviderKind & get(const String & source_key) const;

private:
    StatelessWorkersProviderFactory();

    std::map<String, ProviderKind> kinds;
};

/// Each provider kind registers itself; the discovery kind exists only in ClickHouse Cloud builds.
void registerStaticStatelessWorkersProvider(StatelessWorkersProviderFactory & factory);
void registerDiscoveryStatelessWorkersProvider(StatelessWorkersProviderFactory & factory);

}
