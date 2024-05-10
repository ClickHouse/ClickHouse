#pragma once

#include <Poco/Util/AbstractConfiguration.h>

#include <mutex>
#include <string>

namespace DB {

using ConfigPtr = Poco::AutoPtr<const Poco::Util::AbstractConfiguration>;

class IGgmlModel {
public:
    virtual ~IGgmlModel() = default;

    void load(ConfigPtr config);

    virtual std::string eval(const std::string & input) = 0;

private:
    virtual void LoadImpl(ConfigPtr config) = 0;

    bool loaded{false};
    std::mutex load_mutex;
};

}
