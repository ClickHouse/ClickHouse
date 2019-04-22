#include <Interpreters/ExternalDictionaries.h>
#include <Interpreters/Context.h>
#include <Dictionaries/DictionaryFactory.h>

namespace DB
{

namespace
{
    const ExternalLoaderUpdateSettings externalDictionariesUpdateSettings {};

    const ExternalLoaderConfigSettings & getExternalDictionariesConfigSettings()
    {
        static ExternalLoaderConfigSettings settings;
        static std::once_flag flag;

        std::call_once(flag, []
        {
            settings.external_config = "dictionary";
            settings.external_name = "name";
            settings.path_setting_name = "dictionaries_config";
        });

        return settings;
    }
}


/// Must not acquire Context lock in constructor to avoid possibility of deadlocks.
ExternalDictionaries::ExternalDictionaries(
    std::unique_ptr<IExternalLoaderConfigRepository> config_repository,
    const Poco::Util::AbstractConfiguration & config,
    Context & context)
        : ExternalLoader(config,
                         externalDictionariesUpdateSettings,
                         getExternalDictionariesConfigSettings(),
                         std::move(config_repository),
                         &Logger::get("ExternalDictionaries"),
                         "external dictionary"),
        context(context)
{
}


std::unique_ptr<IExternalLoadable> ExternalDictionaries::create(
        const std::string & name, const Configuration & config, const std::string & config_prefix) const
{
    return DictionaryFactory::instance().create(name, config, config_prefix, context);
}

}
