#pragma once

#include <Common/ErrorCodes.h>
#include <Common/Exception.h>

#include <IO/IResourceManager.h>

#include <boost/noncopyable.hpp>

#include <memory>
#include <mutex>
#include <unordered_map>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_SCHEDULER_NODE;
}

class ResourceManagerFactory : private boost::noncopyable
{
public:
    static ResourceManagerFactory & instance()
    {
        static ResourceManagerFactory ret;
        return ret;
    }

    ResourceManagerPtr get(const String & name)
    {
        std::lock_guard lock{mutex};
        if (auto iter = methods.find(name); iter != methods.end())
            return iter->second();
        throw Exception(ErrorCodes::INVALID_SCHEDULER_NODE, "Unknown scheduler node type: {}", name);
    }

    template <class TDerived>
    void registerMethod(const String & name)
    {
        std::lock_guard lock{mutex};
        methods[name] = [] ()
        {
            return std::make_shared<TDerived>();
        };
    }

private:
    std::mutex mutex;
    using Method = std::function<ResourceManagerPtr()>;
    std::unordered_map<String, Method> methods;
};

}
