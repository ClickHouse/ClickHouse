#pragma once

#include <Common/ZooKeeper/ZooKeeper.h>
#include <Core/BackgroundSchedulePoolTaskHolder.h>
#include <Core/Types_fwd.h>
#include <Interpreters/InsertDeduplication.h>
#include <filesystem>

namespace DB
{

template <typename TStorage>
class AsyncBlockIDsCache
{
    struct Cache;
    using CachePtr = std::shared_ptr<Cache>;

    void update();

public:
    explicit AsyncBlockIDsCache(TStorage & storage_, const std::string & dir_name);

    void start();

    void stop();

    std::vector<DeduplicationHash> detectConflicts(const std::vector<DeduplicationHash> & deduplication_hashes, UInt64 & last_version);

    void triggerCacheUpdate();

    void truncate();

private:

    TStorage & storage;

    const std::chrono::milliseconds update_wait;

    std::mutex mu;
    CachePtr cache_ptr;
    std::condition_variable cv;
    UInt64 version = 0;

    const std::filesystem::path path;

    BackgroundSchedulePoolTaskHolder task;

    const String log_name;
    LoggerPtr log;
};

}
