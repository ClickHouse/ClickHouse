#include <Interpreters/ExternalDictionariesLoader.h>
#include <Dictionaries/DictionaryFactory.h>
#include "config_core.h"

#if USE_MYSQL
#   include <mysqlxx/PoolFactory.h>
#endif

namespace DB
{

/// Must not acquire Context lock in constructor to avoid possibility of deadlocks.
ExternalDictionariesLoader::ExternalDictionariesLoader(Context & context_)
    : ExternalLoader("external dictionary", &Logger::get("ExternalDictionariesLoader"))
    , context(context_)
{
    setConfigSettings({"dictionary", "name", "database"});
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

void ExternalDictionariesLoader::resetAll()
{
    #if USE_MYSQL
        mysqlxx::PoolFactory::instance().reset();
    #endif
}

}
