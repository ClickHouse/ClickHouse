#include <Interpreters/ExternalModelsLoader.h>
#include <Interpreters/Context.h>
#include <Interpreters/TensorFlowModel.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
}


ExternalModelsLoader::ExternalModelsLoader(ContextPtr context_)
    : ExternalLoader("external model", &Poco::Logger::get("ExternalModelsLoader")), WithContext(context_)
{
    setConfigSettings({"model", "name", {}, {}});
    enablePeriodicUpdates(true);
}

std::shared_ptr<const IExternalLoadable> ExternalModelsLoader::create(
    const std::string & name, const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix, const std::string & /* repository_name */) const
{
    String type = config.getString(config_prefix + ".type");
    ExternalLoadableLifetime lifetime(config, config_prefix + ".lifetime");

    /// TODO: add models factory.
    if (type == "catboost")
    {
        return std::make_unique<CatBoostModel>(
                name, config.getString(config_prefix + ".path"),
                getContext()->getConfigRef().getString("catboost_dynamic_library_path"),
                lifetime
        );
    }
    else if (type == "tensorflow")
    {
        int32_t num_threads = 1;
        if (config.has(config_prefix + ".num_threads")) 
            num_threads = config.getInt(config_prefix + ".num_threads");

        return std::make_unique<TensorFlowModel>(
                name, config.getString(config_prefix + ".path"),
                context.getConfigRef().getString("tensorflow_lite_dynamic_library_path"),
                lifetime, num_threads
        );
    }
    else
    {
        throw Exception("Unknown model type: " + type, ErrorCodes::INVALID_CONFIG_PARAMETER);
    }
}
}
