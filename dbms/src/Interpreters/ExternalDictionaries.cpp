#include <Interpreters/ExternalDictionaries.h>
#include <Interpreters/Context.h>
#include <Dictionaries/DictionaryFactory.h>

namespace DB
{

/// Must not acquire Context lock in constructor to avoid possibility of deadlocks.
ExternalDictionaries::ExternalDictionaries(
    std::unique_ptr<IExternalLoaderConfigRepository> config_repository,
    const Poco::Util::AbstractConfiguration & config,
    Context & context_)
        : ExternalLoader(config,
                         "external dictionary",
                         &Logger::get("ExternalDictionaries")),
        context(context_)
{
    addConfigRepository(std::move(config_repository), {"dictionary", "name", "dictionaries_config"});
    enableAsyncLoading(true);
    enablePeriodicUpdates(true);
}


ExternalLoader::LoadablePtr ExternalDictionaries::create(
        const std::string & name, const Poco::Util::AbstractConfiguration & config, const std::string & key_in_config) const
{
    return DictionaryFactory::instance().create(name, config, key_in_config, context);
}

}
