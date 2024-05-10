#include "model_storage.h"

namespace DB {

std::shared_ptr<IGgmlModel> GgmlModelStorage::get(const std::string & key, ModelBuilder builder)
{
    std::lock_guard lock{mtx};
    if (models.contains(key)) {
        return models[key];
    }
    return models[key] = builder();
}

}
