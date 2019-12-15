#include <Interpreters/ExternalDictionariesLoader.h>
#include <Dictionaries/DictionaryFactory.h>

namespace DB
{

/// Must not acquire Context lock in constructor to avoid possibility of deadlocks.
ExternalDictionariesLoader::ExternalDictionariesLoader(Context & context_)
    : ExternalLoader("external dictionary", &Logger::get("ExternalDictionariesLoader"))
    , context(context_)
{
    enableAsyncLoading(true);
    enablePeriodicUpdates(true);
}


ExternalLoader::LoadablePtr ExternalDictionariesLoader::create(
        const std::string & name, const Poco::Util::AbstractConfiguration & config, const std::string & key_in_config) const
{
    return DictionaryFactory::instance().create(name, config, key_in_config, context);
}

void ExternalDictionariesLoader::addConfigRepository(
    const std::string & repository_name, std::unique_ptr<IExternalLoaderConfigRepository> config_repository)
{
    ExternalLoader::addConfigRepository(repository_name, std::move(config_repository), {"dictionary", "name"});
}

}
