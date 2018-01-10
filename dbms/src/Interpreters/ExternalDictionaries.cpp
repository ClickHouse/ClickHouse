#include <Interpreters/ExternalDictionaries.h>
#include <Interpreters/Context.h>
#include <Dictionaries/DictionaryFactory.h>

namespace DB
{

namespace
{
    const ExternalLoaderUpdateSettings externalDictionariesUpdateSettings { };

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


ExternalDictionaries::ExternalDictionaries(
    std::unique_ptr<IExternalLoaderConfigRepository> config_repository,
    Context & context,
    bool throw_on_error)
        : ExternalLoader(context.getConfigRef(),
                         externalDictionariesUpdateSettings,
                         getExternalDictionariesConfigSettings(),
                         std::move(config_repository),
                         &Logger::get("ExternalDictionaries"),
                         "external dictionary"),
          context(context)
{
    init(throw_on_error);
}

std::unique_ptr<IExternalLoadable> ExternalDictionaries::create(
        const std::string & name, const Configuration & config, const std::string & config_prefix)
{
    return DictionaryFactory::instance().create(name, config, config_prefix, context);
}

}
