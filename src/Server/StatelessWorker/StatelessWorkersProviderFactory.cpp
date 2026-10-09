#include "config.h"

#include <Server/StatelessWorker/StatelessWorkersProviderFactory.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

StatelessWorkersProviderFactory::StatelessWorkersProviderFactory()
{
    registerStaticStatelessWorkersProvider(*this);
#if CLICKHOUSE_CLOUD
    registerDiscoveryStatelessWorkersProvider(*this);
#endif
}

StatelessWorkersProviderFactory & StatelessWorkersProviderFactory::instance()
{
    static StatelessWorkersProviderFactory factory;
    return factory;
}

void StatelessWorkersProviderFactory::registerProviderKind(const String & name, ProviderKind kind)
{
    if (!kinds.emplace(name, std::move(kind)).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Stateless worker provider kind '{}' is already registered", name);
}

size_t StatelessWorkersProviderFactory::countWorkerSources(const Poco::Util::AbstractConfiguration & config, const String & config_prefix) const
{
    size_t sources = 0;
    for (const auto & [_, kind] : kinds)
        sources += kind.count_worker_sources(config, config_prefix);
    return sources;
}

const std::pair<const String, StatelessWorkersProviderFactory::ProviderKind> & StatelessWorkersProviderFactory::getKind(
    const Poco::Util::AbstractConfiguration & config, const String & config_prefix) const
{
    for (const auto & entry : kinds)
    {
        if (entry.second.count_worker_sources(config, config_prefix) > 0)
            return entry;
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "No stateless worker provider kind serves '{}'", config_prefix);
}

}
