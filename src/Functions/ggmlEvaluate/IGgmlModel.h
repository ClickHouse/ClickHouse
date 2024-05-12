#pragma once

#include <base/types.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <mutex>
#include <string>

namespace DB
{

using ConfigPtr = Poco::AutoPtr<const Poco::Util::AbstractConfiguration>;

using GgmlModelParams = std::tuple<Int32>;

class IGgmlModel
{
public:
    virtual ~IGgmlModel() = default;

    void load(ConfigPtr config);
    std::string eval(GgmlModelParams params, const std::string & input);

private:
    virtual void loadImpl(ConfigPtr config) = 0;
    virtual std::string evalImpl(GgmlModelParams params, const std::string & input) = 0;

    bool loaded = false;
    std::mutex load_mutex;
};

}
