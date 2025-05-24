#include <Models/ModelRegistry.h>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int MODEL_NOT_FOUND;
    extern const int MODEL_ALREADY_EXISTS;
}

ModelRegistry & ModelRegistry::instance()
{
    static ModelRegistry inst;
    return inst;
}

ModelPtr ModelRegistry::registerModel(const String& model_name, ModelPtr model)
{
    std::lock_guard lock(mutex);
    if (models.contains(model_name)) {
        throw Exception(ErrorCodes::MODEL_ALREADY_EXISTS, "Model '{}' was already registered", model_name);
    }

    models[model_name] = model;
    return model;
}

ModelPtr ModelRegistry::getModel(const std::string & model_name) const
{
    std::lock_guard lock(mutex);
    if (!models.contains(model_name)) {
        throw Exception(ErrorCodes::MODEL_NOT_FOUND, "No model '{}' exists", model_name);
    }

    return models.at(model_name);
}

bool ModelRegistry::hasModel(const String& model_name) const
{
    return models.contains(model_name);
}

void ModelRegistry::unregisterModel(const String& model_name)
{
    if (!models.contains(model_name)) {
        throw Exception(ErrorCodes::MODEL_NOT_FOUND, "No model '{}' exists", model_name);
    }

    models.erase(model_name);
}

}
