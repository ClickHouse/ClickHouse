#include <Interpreters/ExternalDictionariesLoader.h>
#include <Dictionaries/DictionaryFactory.h>

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


void ExternalDictionariesLoader::reload(const String & name, bool load_never_loading)
{
    #if USE_MYSQL
        mysqlxx::PoolFactory::instance().reset();
    #endif
    ExternalLoader::reload(name, load_never_loading);
}

void ExternalDictionariesLoader::reload(bool load_never_loading)
{
    #if USE_MYSQL
        mysqlxx::PoolFactory::instance().reset();
    #endif
    ExternalLoader::reload(load_never_loading);
}

void ExternalDictionariesLoader::addConfigRepository(
    const std::string & repository_name, std::unique_ptr<IExternalLoaderConfigRepository> config_repository)
{
    ExternalLoader::addConfigRepository(repository_name, std::move(config_repository), {"dictionary", "name"});
}


void ExternalDictionariesLoader::addDictionaryWithConfig(
    const String & dictionary_name, const String & repo_name, const ASTCreateQuery & query, bool load_never_loading) const
{
    ExternalLoader::addObjectAndLoad(
        dictionary_name, /// names are equal
        dictionary_name,
        repo_name,
        getDictionaryConfigurationFromAST(query),
        "dictionary", load_never_loading);
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
}
