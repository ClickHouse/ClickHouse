#include <Interpreters/ExternalModels.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
}

namespace
{
const ExternalLoaderUpdateSettings & getExternalModelsUpdateSettings()
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

const ExternalLoaderConfigSettings & getExternalModelsConfigSettings()
{
    static ExternalLoaderConfigSettings settings;
    static std::once_flag flag;

    std::call_once(flag, [] {
        settings.external_config = "model";
        settings.external_name = "name";

        settings.path_setting_name = "models_config";
    });

    return settings;
}
}


ExternalModels::ExternalModels(Context & context, bool throw_on_error)
        : ExternalLoader(context.getConfigRef(),
                         getExternalModelsUpdateSettings(),
                         getExternalModelsConfigSettings(),
                         &Logger::get("ExternalModels"),
                         "external model"),
          context(context)
{
    init(throw_on_error);
}

std::unique_ptr<IExternalLoadable> ExternalModels::create(
        const std::string & name, const Configuration & config, const std::string & config_prefix)
{
    String type = config.getString(config_prefix + ".type");
    ExternalLoadableLifetime lifetime(config, config_prefix);

    /// TODO: add models factory.
    if (type == "catboost")
    {
        return std::make_unique<CatBoqostModel>(
                name, config.getString(config_prefix + ".path"),
                context.getConfigRef().getString("catboost_dynamic_library_path"),
                lifetime, config.getUInt(config_prefix + ".float_features_count"),
                config.getUInt(config_prefix + ".cat_features_count")
        );
    }
    else
    {
        throw Exception("Unknown model type: " + type, ErrorCodes::INVALID_CONFIG_PARAMETER);
    }
}

}
