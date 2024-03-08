#pragma once

#include <condition_variable>
#include <functional>
#include <iostream>
#include <mutex>
#include <thread>
#include <unordered_map>

#include <base/types.h>
#include <Common/ThreadPool.h>
#include <Common/setThreadName.h>


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

template <class Key, class Value>
class AsyncLoadingCache
{
public:
    using Loader = std::function<void(std::unordered_map<Key, Value>)>;

    AsyncLoadingCache(Loader loader_, UInt64 refresh_interval_sec_, String thread_name_ = "AsyncLoadingCache")
        : loader(loader_), refresh_interval_sec(refresh_interval_sec_), thread_name(thread_name_)
    {
        load_thread = ThreadFromGlobalPool(&AsyncLoadingCache::loadTask, this);
    }

    ~AsyncLoadingCache()
    {
        {
            std::unique_lock<std::mutex> lock(mutex);
            shutdown = true;
        }
        load_thread.join();
    }

    Value get(const Key & key, Value default_value)
    {
        std::unique_lock<std::mutex> lock(mutex);
        auto it = cache.find(key);
        return it != cache.end() ? it->second : default_value;
    }

private:
    void loadTask()
    {
        setThreadName(thread_name.c_str());
        while (true)
        {
            if (shutdown)
                break;

            std::this_thread::sleep_for(std::chrono::seconds(refresh_interval_sec));
            std::unique_lock<std::mutex> lock(mutex);
            loader(cache);
        }
    }

    String thread_name;

    std::unordered_map<Key, Value> cache;
    Loader loader;

    UInt64 refresh_interval_sec;
    ThreadFromGlobalPool load_thread;

    std::mutex mutex;
    bool shutdown = false;
};

}
