#include "model_storage.h"

#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
extern const int NO_ELEMENTS_IN_CONFIG;
}

GgmlModelBuilders & GgmlModelBuilders::instance()
{
    static GgmlModelBuilders instance;
    return instance;
}

std::shared_ptr<IGgmlModel> GgmlModelStorage::get(const std::string & key, const std::string & builder_name)
{
    std::lock_guard lock{mtx};

    if (models.contains(key))
        return models[key];

    auto & builders = GgmlModelBuilders::instance();
    auto it = builders.find(builder_name);
    if (it == builders.end())
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "No builder with name '{}'", builder_name);
    return models[key] = it->second();
}

}
