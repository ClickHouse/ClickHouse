#include "config.h"

#include <Server/StatelessWorker/StatelessWorkersProviderFactory.h>
#include <Common/Exception.h>
#include <Poco/Util/AbstractConfiguration.h>

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

void StatelessWorkersProviderFactory::registerProviderKind(const String & source_key, ProviderKind kind)
{
    if (!kinds.emplace(source_key, std::move(kind)).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Stateless worker provider kind '{}' is already registered", source_key);
}

Strings StatelessWorkersProviderFactory::getWorkerSources(const Poco::Util::AbstractConfiguration & config, const String & config_prefix) const
{
    Strings sources;
    for (const auto & [source_key, _] : kinds)
        if (!config.getString(config_prefix + "." + source_key, "").empty())
            sources.push_back(source_key);
    return sources;
}

const StatelessWorkersProviderFactory::ProviderKind & StatelessWorkersProviderFactory::get(const String & source_key) const
{
    auto it = kinds.find(source_key);
    if (it == kinds.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown stateless worker provider kind '{}'", source_key);
    return it->second;
}

}
