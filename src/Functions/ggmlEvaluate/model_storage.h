#pragma once

#include <Common/Exception.h>

#include <mutex>
#include <string>
#include <unordered_map>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

// TODO: shit below

template <typename T>
class GgmlModelStorage : public std::unordered_map<std::string, std::unique_ptr<T>>, public std::mutex
{
public:
    using Storage = std::unordered_map<std::string, std::unique_ptr<T>>;

    T* get(const std::string & key, std::function<std::unique_ptr<T>()> builder)
    {
        std::lock_guard lock{*this};
        auto it = Storage::find(key);
        if (it == Storage::end()) {
            it = Storage::emplace(key, builder()).first;
        }
        return it->second.get();
    }
};

}
