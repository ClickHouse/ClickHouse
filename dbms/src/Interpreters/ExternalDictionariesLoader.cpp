#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/Context.h>
#include <Dictionaries/DictionaryFactory.h>

#if USE_MYSQL
#   include <mysqlxx/PoolFactory.h>
#endif

namespace DB
{

/// Must not acquire Context lock in constructor to avoid possibility of deadlocks.
ExternalDictionariesLoader::ExternalDictionariesLoader(
    ExternalLoaderConfigRepositoryPtr config_repository, Context & context_)
    : ExternalLoader("external dictionary", &Logger::get("ExternalDictionariesLoader"))
    , context(context_)
{
    addConfigRepository(std::move(config_repository), {"dictionary", "name"});
    enableAsyncLoading(true);
    enablePeriodicUpdates(true);
}


ExternalLoader::LoadablePtr ExternalDictionariesLoader::create(
        const std::string & name, const Poco::Util::AbstractConfiguration & config, const std::string & key_in_config) const
{
    return DictionaryFactory::instance().create(name, config, key_in_config, context);
}

void ExternalDictionaries::reload(const String & name, bool load_never_loading)
{
    #if USE_MYSQL
        mysqlxx::PoolFactory::instance().reset();
    #endif
    ExternalLoader::reload(name, load_never_loading);
}

void ExternalDictionaries::reload(bool load_never_loading)
{
    #if USE_MYSQL
        mysqlxx::PoolFactory::instance().reset();
    #endif
    ExternalLoader::reload(load_never_loading);
}

}
