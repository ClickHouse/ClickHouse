#pragma once

#include "IGgmlModel.h"

#include <mutex>
#include <string>
#include <unordered_map>

namespace DB
{

class GgmlModelStorage
{
public:
    using ModelBuilder = std::function<std::shared_ptr<IGgmlModel>()>;

    std::shared_ptr<IGgmlModel> get(const std::string & key, ModelBuilder builder)
    {
        std::lock_guard lock{mtx};
        if (!models.contains(key)) {
            models[key] = builder();
        }
        return models[key];
    }
private:
    std::unordered_map<std::string, std::shared_ptr<IGgmlModel>> models;
    std::mutex mtx;
};

}
