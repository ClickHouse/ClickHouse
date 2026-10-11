#pragma once

#include <Common/ZooKeeper/ZooKeeper.h>
#include <Core/BackgroundSchedulePoolTaskHolder.h>
#include <Core/Types_fwd.h>
#include <Interpreters/InsertDeduplication.h>
#include <filesystem>

namespace DB
{

/// Snapshot of the deduplication hashes stored in Keeper under `dir_name`.
/// An insert checks its hashes here before the Keeper commit, because the commit reports only one duplicate.
/// Each refresh makes a new snapshot with a larger version. `truncate` drops the snapshot and keeps the version.
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

    struct Conflicts
    {
        std::vector<DeduplicationHash> hashes;
        /// Version of the checked snapshot.
        UInt64 version = 0;
    };

    /// Returns the hashes from `deduplication_hashes` that are in the snapshot.
    /// Waits up to `update_wait` for a snapshot newer than `wait_newer_than`. Pass 0 to take any loaded snapshot.
    Conflicts detectConflicts(const std::vector<DeduplicationHash> & deduplication_hashes, UInt64 wait_newer_than);

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
