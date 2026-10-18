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
    struct ProviderKind
    {
        /// Builds and starts the provider for the cluster at `config_prefix`.
        std::function<StatelessWorkersProviderPtr(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)> create;
        /// A digest of the configuration `create` reads, so a config reload rebuilds the provider only on change.
        std::function<String(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)> config_signature;
    };

    static StatelessWorkersProviderFactory & instance();

    void registerProviderKind(const String & source_key, ProviderKind kind);

    /// The worker sources the cluster at `config_prefix` sets.
    [[nodiscard]] Strings getWorkerSources(const Poco::Util::AbstractConfiguration & config, const String & config_prefix) const;

    [[nodiscard]] const ProviderKind & get(const String & source_key) const;

private:
    StatelessWorkersProviderFactory();

    std::map<String, ProviderKind> kinds;
};

/// Each provider kind registers itself; the discovery kind exists only in ClickHouse Cloud builds.
void registerStaticStatelessWorkersProvider(StatelessWorkersProviderFactory & factory);
void registerDiscoveryStatelessWorkersProvider(StatelessWorkersProviderFactory & factory);

}
