#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>
#include <Storages/MergeTree/AsyncBlockIDsCache.h>
#include <Storages/StorageReplicatedMergeTree.h>

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
std::vector<String> AsyncBlockIDsCache<TStorage>::getChildren()
{
    auto zookeeper = storage.getZooKeeper();

    auto watch_callback = [last_time = this->last_updatetime.load()
                           , my_update_min_interval = this->update_min_interval
                           , my_task = task->shared_from_this()](const Coordination::WatchResponse &)
    {
        auto now = std::chrono::steady_clock::now();
        if (now - last_time < my_update_min_interval)
        {
            std::chrono::milliseconds sleep_time = std::chrono::duration_cast<std::chrono::milliseconds>(my_update_min_interval - (now - last_time));
            my_task->scheduleAfter(sleep_time.count());
        }
        else
            my_task->schedule();
    };
    std::vector<String> children;
    Coordination::Stat stat;
    zookeeper->tryGetChildrenWatch(path, children, &stat, watch_callback);
    return children;
}

template <typename TStorage>
void AsyncBlockIDsCache<TStorage>::update()
try
{
    std::vector<String> paths = getChildren();
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
    last_updatetime = std::chrono::steady_clock::now();
}
catch (...)
{
    LOG_INFO(log, "Updating async block ids cache failed. Reason: {}", getCurrentExceptionMessage(false));
    task->scheduleAfter(update_min_interval.count());
}

template <typename TStorage>
AsyncBlockIDsCache<TStorage>::AsyncBlockIDsCache(TStorage & storage_)
    : storage(storage_),
    update_min_interval(storage.getSettings()->async_block_ids_cache_min_update_interval_ms),
    path(storage.getZooKeeperPath() + "/async_blocks"),
    log_name(storage.getStorageID().getFullTableName() + " (AsyncBlockIDsCache)"),
    log(&Poco::Logger::get(log_name))
{
    task = storage.getContext()->getSchedulePool().createTask(log_name, [this]{ update(); });
}

template <typename TStorage>
void AsyncBlockIDsCache<TStorage>::start()
{
    if (storage.getSettings()->use_async_block_ids_cache)
        task->activateAndSchedule();
}

/// Caller will keep the version of last call. When the caller calls again, it will wait util gets a newer version.
template <typename TStorage>
Strings AsyncBlockIDsCache<TStorage>::detectConflicts(const Strings & paths, UInt64 & last_version)
{
    if (!storage.getSettings()->use_async_block_ids_cache)
        return {};

    std::unique_lock lk(mu);
    /// For first time access of this cache, the `last_version` is zero, so it will not block here.
    /// For retrying request, We compare the request version and cache version, because zk only returns
    /// incomplete information of duplication, we need to update the cache to find out more duplication.
    /// The timeout here is to prevent deadlock, just in case.
    cv.wait_for(lk, update_min_interval * 2, [&]{return version != last_version;});

    if (version == last_version)
        LOG_INFO(log, "Read cache with a old version {}", last_version);

    CachePtr cur_cache;
    cur_cache = cache_ptr;
    last_version = version;

    lk.unlock();

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
