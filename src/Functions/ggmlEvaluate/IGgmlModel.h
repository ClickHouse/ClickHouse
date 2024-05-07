#pragma once

#include <cstdint>
#include <iostream>
#include <map>
#include <mutex>
#include <random>
#include <string>
#include <thread>

class IGgmlModel {
public:
    virtual ~IGgmlModel() = default;

    bool load(const std::string & fname)
    {
        std::lock_guard lock{load_mutex};
        if (!loaded) {
            loaded = LoadImpl(fname);
        }
        return loaded;
    }

    virtual std::string eval(const std::string & input) = 0;

private:
    virtual bool LoadImpl(const std::string & fname) = 0;

    bool loaded{false};
    std::mutex load_mutex;
};
