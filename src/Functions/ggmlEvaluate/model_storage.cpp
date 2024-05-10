#include "model_storage.h"
#include "Common/Exception.h"

namespace DB {

namespace ErrorCodes
{
    extern const int NO_ELEMENTS_IN_CONFIG;
}

GgmlModelBuilders & GgmlModelBuilders::instance()
{
    static GgmlModelBuilders instance;
    return instance;
}

std::shared_ptr<IGgmlModel> GgmlModelStorage::get(const std::string & key)
{
    std::lock_guard lock{mtx};
    if (models.contains(key))
        return models[key];
    auto & builders = GgmlModelBuilders::instance();
    auto it = builders.find(key);
    if (it == builders.end())
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "No model with name '{}'", key);
    std::cout << "building model with key " << key;
    return models[key] = it->second();
}

}
