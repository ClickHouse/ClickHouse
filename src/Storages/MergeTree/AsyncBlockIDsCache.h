#pragma once

#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/logger_useful.h>
#include <Core/BackgroundSchedulePool.h>

#include <chrono>

namespace DB
{

class StorageReplicatedMergeTree;

class AsyncBlockIDsCache
{
    struct Cache;
    using CachePtr = std::shared_ptr<Cache>;

    std::vector<String> getChildren();

    void update();

public:
    explicit AsyncBlockIDsCache(StorageReplicatedMergeTree & storage_);

    void start();

    void stop() { task->deactivate(); }

    Strings detectConflicts(const Strings & paths, UInt64 & last_version);

private:

    StorageReplicatedMergeTree & storage;

    std::atomic<std::chrono::steady_clock::time_point> last_updatetime;
    const std::chrono::milliseconds update_min_interval;

    std::mutex mu;
    CachePtr cache_ptr;
    std::condition_variable cv;
    UInt64 version = 0;

    const String path;

    BackgroundSchedulePool::TaskHolder task;

    const String log_name;
    Poco::Logger * log;
};

using AsyncBlockIDsCachePtr = std::shared_ptr<AsyncBlockIDsCache>;

}
