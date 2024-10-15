#include <Storages/MergeTree/AsyncBlockIDsCache.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>

#include <unordered_set>

namespace ProfileEvents
{
    extern const Event AsyncInsertCacheHits;
}

namespace CurrentMetrics
{
    extern const Metric AsyncInsertCacheSize;
}

namespace DB
{

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsMilliseconds async_block_ids_cache_update_wait_ms;
    extern const MergeTreeSettingsBool use_async_block_ids_cache;
}

static constexpr int FAILURE_RETRY_MS = 3000;

template <typename TStorage>
struct AsyncBlockIDsCache<TStorage>::Cache : public std::unordered_set<String>
{
    CurrentMetrics::Increment cache_size_increment;
    explicit Cache(std::unordered_set<String> && set_)
        : std::unordered_set<String>(std::move(set_))
        , cache_size_increment(CurrentMetrics::AsyncInsertCacheSize, size())
    {}
};

template <typename TStorage>
void AsyncBlockIDsCache<TStorage>::update()
try
{
    auto zookeeper = storage.getZooKeeper();
    std::vector<String> paths = zookeeper->getChildren(path);
    std::unordered_set<String> set;
    for (String & p : paths)
    {
        set.insert(std::move(p));
    }
    {
        std::lock_guard lock(mu);
        cache_ptr = std::make_shared<Cache>(std::move(set));
        ++version;
    }
    cv.notify_all();
}
catch (...)
{
    LOG_INFO(log, "Updating async block ids cache failed. Reason: {}", getCurrentExceptionMessage(false));
    task->scheduleAfter(FAILURE_RETRY_MS);
}

template <typename TStorage>
AsyncBlockIDsCache<TStorage>::AsyncBlockIDsCache(TStorage & storage_)
    : storage(storage_)
    , update_wait((*storage.getSettings())[MergeTreeSetting::async_block_ids_cache_update_wait_ms])
    , path(storage.getZooKeeperPath() + "/async_blocks")
    , log_name(storage.getStorageID().getFullTableName() + " (AsyncBlockIDsCache)")
    , log(getLogger(log_name))
{
    task = storage.getContext()->getSchedulePool().createTask(log_name, [this]{ update(); });
}

template <typename TStorage>
void AsyncBlockIDsCache<TStorage>::start()
{
    if ((*storage.getSettings())[MergeTreeSetting::use_async_block_ids_cache])
        task->activateAndSchedule();
}

template <typename TStorage>
void AsyncBlockIDsCache<TStorage>::triggerCacheUpdate()
{
    /// Trigger task update. Watch-based updates may produce a lot of
    /// redundant work in case of multiple replicas, so we use manually controlled updates
    /// in case of duplicates
    if (!task->schedule())
        LOG_TRACE(log, "Task is already scheduled, will wait for update for {}ms", update_wait.count());
}

/// Caller will keep the version of last call. When the caller calls again, it will wait util gets a newer version.
template <typename TStorage>
Strings AsyncBlockIDsCache<TStorage>::detectConflicts(const Strings & paths, UInt64 & last_version)
{
    if (!(*storage.getSettings())[MergeTreeSetting::use_async_block_ids_cache])
        return {};

    CachePtr cur_cache;
    {
        std::unique_lock lk(mu);
        /// For first time access of this cache, the `last_version` is zero, so it will not block here.
        /// For retrying request, We compare the request version and cache version, because zk only returns
        /// incomplete information of duplication, we need to update the cache to find out more duplication.
        cv.wait_for(lk, update_wait, [&]{return version != last_version;});

        if (version == last_version)
            LOG_INFO(log, "Read cache with a old version {}", last_version);

        cur_cache = cache_ptr;
        last_version = version;
    }

    if (cur_cache == nullptr)
        return {};

    Strings conflicts;
    for (const String & p : paths)
    {
        if (cur_cache->contains(p))
        {
            conflicts.push_back(p);
        }
    }

    ProfileEvents::increment(ProfileEvents::AsyncInsertCacheHits, !conflicts.empty());

    return conflicts;
}

template class AsyncBlockIDsCache<StorageReplicatedMergeTree>;

}
