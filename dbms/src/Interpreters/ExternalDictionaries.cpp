#include <Interpreters/ExternalDictionaries.h>
#include <Interpreters/Context.h>
#include <Dictionaries/DictionaryFactory.h>

namespace DB
{

namespace
{
    const ExternalLoaderUpdateSettings & getExternalDictionariesUpdateSettings()
    {
        static ExternalLoaderUpdateSettings settings;
        static std::once_flag flag;

        std::call_once(flag, [] {
            settings.check_period_sec = 5;
            settings.backoff_initial_sec = 5;
            /// 10 minutes
            settings.backoff_max_sec = 10 * 60;
        });

        return settings;
    }

    const ExternalLoaderConfigSettings & getExternalDictionariesConfigSettings()
    {
        static ExternalLoaderConfigSettings settings;
        static std::once_flag flag;

        std::call_once(flag, [] {
            settings.external_config = "dictionary";
            settings.external_name = "name";

            settings.path_setting_name = "dictionaries_config";
        });

        return settings;
    }
}


ExternalDictionaries::ExternalDictionaries(Context & context, bool throw_on_error)
        : ExternalLoader(context.getConfigRef(),
                         getExternalDictionariesUpdateSettings(),
                         getExternalDictionariesConfigSettings(),
                         &Logger::get("ExternalDictionaries"),
                         "external dictionary", throw_on_error),
          context(context)
{

}

std::unique_ptr<IExternalLoadable> ExternalDictionaries::create(
        const std::string & name, const Configuration & config, const std::string & config_prefix)
{
    return DictionaryFactory::instance().create(name, config, config_prefix, context);
}

}
