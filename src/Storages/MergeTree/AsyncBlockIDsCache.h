#pragma once

#include <Common/ZooKeeper/ZooKeeper.h>
#include <Core/BackgroundSchedulePool.h>
#include <Core/Types_fwd.h>

namespace DB
{

template <typename TStorage>
class AsyncBlockIDsCache
{
    struct Cache;
    using CachePtr = std::shared_ptr<Cache>;

    void update();

public:
    explicit AsyncBlockIDsCache(TStorage & storage_);

    void start();

    void stop() { task->deactivate(); }

    Strings detectConflicts(const Strings & paths, UInt64 & last_version);

    void triggerCacheUpdate();

private:

    TStorage & storage;

    const std::chrono::milliseconds update_wait;

    std::mutex mu;
    CachePtr cache_ptr;
    std::condition_variable cv;
    UInt64 version = 0;

    const String path;

    BackgroundSchedulePool::TaskHolder task;

    const String log_name;
    LoggerPtr log;
};

}
