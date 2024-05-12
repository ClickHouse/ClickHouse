#pragma once

#include "IGgmlModel.h"

#include <mutex>
#include <string>
#include <unordered_map>


namespace DB
{

using GgmlModelBuilderFunctionPtr = std::shared_ptr<IGgmlModel> (*)();

struct GgmlModelBuilders : public std::unordered_map<std::string_view, GgmlModelBuilderFunctionPtr>
{
    static GgmlModelBuilders & instance();
};

template <typename T>
class GgmlModelRegister
{
public:
    explicit GgmlModelRegister(std::string_view name) { GgmlModelBuilders::instance().emplace(name, &GgmlModelRegister<T>::Create); }

private:
    static std::shared_ptr<IGgmlModel> Create() { return std::make_shared<T>(); }
};

class GgmlModelStorage
{
public:
    std::shared_ptr<IGgmlModel> get(const std::string & key);

private:
    std::unordered_map<std::string, std::shared_ptr<IGgmlModel>> models;
    std::mutex mtx;
};

}
