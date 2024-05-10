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
    std::string eval(const std::string & input);

private:
    virtual void loadImpl(ConfigPtr config) = 0;
    virtual std::string evalImpl(const std::string & input) = 0;

    bool loaded = false;
    std::mutex load_mutex;
};

}
