#pragma once

#include <mutex>
#include <string>

namespace DB {

class IGgmlModel {
public:
    virtual ~IGgmlModel() = default;

    void load(const std::string & fname);

    virtual std::string eval(const std::string & input) = 0;

private:
    virtual void LoadImpl(const std::string & fname) = 0;

    bool loaded{false};
    std::mutex load_mutex;
};

}
