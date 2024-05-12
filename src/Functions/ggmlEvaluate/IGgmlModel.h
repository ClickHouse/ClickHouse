#pragma once

#include <base/types.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <Core/Field.h>

#include <mutex>
#include <string>

namespace DB
{

using ConfigPtr = Poco::AutoPtr<const Poco::Util::AbstractConfiguration>;

// User-provided model parameters
// Often they override those defined in model file
// Each model might have its own set of supported parameters
using GgmlModelParams = FieldMap;

class IGgmlModel
{
public:
    virtual ~IGgmlModel() = default;

    void load(ConfigPtr config);
    std::string eval(const std::string & input, const GgmlModelParams & user_params);

private:
    virtual void loadImpl(ConfigPtr config) = 0;
    virtual std::string evalImpl(const std::string & input, const GgmlModelParams & user_params) = 0;

    bool loaded = false;
    std::mutex load_mutex;
};

}
