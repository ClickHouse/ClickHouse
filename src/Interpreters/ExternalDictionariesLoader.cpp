#include <Interpreters/ExternalDictionariesLoader.h>
#include <Dictionaries/DictionaryFactory.h>
#include <Dictionaries/DictionaryStructure.h>

#if !defined(ARCADIA_BUILD)
#    include "config_core.h"
#endif

#if USE_MYSQL
#   include <mysqlxx/PoolFactory.h>
#endif

namespace DB
{

/// Must not acquire Context lock in constructor to avoid possibility of deadlocks.
ExternalDictionariesLoader::ExternalDictionariesLoader(Context & context_)
    : ExternalLoader("external dictionary", &Poco::Logger::get("ExternalDictionariesLoader"))
    , context(context_)
{
    setConfigSettings({"dictionary", "name", "database", "uuid"});
    enableAsyncLoading(true);
    enablePeriodicUpdates(true);
}


ExternalLoader::LoadablePtr ExternalDictionariesLoader::create(
        const std::string & name, const Poco::Util::AbstractConfiguration & config,
        const std::string & key_in_config, const std::string & repository_name) const
{
    /// For dictionaries from databases (created with DDL queries) we have to perform
    /// additional checks, so we identify them here.
    bool dictionary_from_database = !repository_name.empty();
    return DictionaryFactory::instance().create(name, config, key_in_config, context, dictionary_from_database);
}


DictionaryStructure
ExternalDictionariesLoader::getDictionaryStructure(const Poco::Util::AbstractConfiguration & config, const std::string & key_in_config)
{
    return {config, key_in_config + ".structure"};
}

DictionaryStructure ExternalDictionariesLoader::getDictionaryStructure(const ObjectConfig & config)
{
    return getDictionaryStructure(*config.config, config.key_in_config);
}


void ExternalDictionariesLoader::resetAll()
{
#if USE_MYSQL
    mysqlxx::PoolFactory::instance().reset();
#endif
}

}
