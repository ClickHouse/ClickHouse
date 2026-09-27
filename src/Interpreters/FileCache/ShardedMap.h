#pragma once

#include <Common/ProfiledLocks.h>
#include <base/scope_guard.h>

#include <array>
#include <atomic>
#include <functional>
#include <mutex>
#include <unordered_map>
#include <boost/noncopyable.hpp>


namespace FileCacheUtils
{

/// Hash-sharded map: keys are spread across independently-locked buckets, so
/// concurrent operations on different keys rarely contend. `lock_wait_event`
/// records time spent waiting on a contended shard lock.
template <typename Key, typename Value, size_t num_shards = 32>
class ShardedMap : private boost::noncopyable
{
public:
    using Map = std::unordered_map<Key, Value>;

    explicit ShardedMap(ProfileEvents::Event lock_wait_event_)
        : lock_wait_event(lock_wait_event_)
    {
    }

    /// Run `f(map)` under the owning shard's lock.
    template <typename F>
    auto withShard(const Key & key, F && f) const
    {
        Shard & shard = shards[std::hash<Key>{}(key) % num_shards];
        DB::ProfiledMutexLock lock(shard.mutex, lock_wait_event);
        const size_t size_before = shard.map.size();
        SCOPE_EXIT(accountSizeDelta(size_before, shard.map.size()));
        return f(shard.map);
    }

    /// Run `f(map)` under each shard's lock in turn.
    template <typename F>
    void forEachShard(F && f) const
    {
        for (Shard & shard : shards)
        {
            DB::ProfiledMutexLock lock(shard.mutex, lock_wait_event);
            const size_t size_before = shard.map.size();
            SCOPE_EXIT(accountSizeDelta(size_before, shard.map.size()));
            f(shard.map);
        }
    }

    size_t size() const { return total_count.load(std::memory_order_relaxed); }

private:
    struct Shard
    {
        mutable std::mutex mutex;
        Map map;
    };

    void accountSizeDelta(size_t before, size_t after) const
    {
        if (after > before)
            total_count.fetch_add(after - before, std::memory_order_relaxed);
        else if (after < before)
            total_count.fetch_sub(before - after, std::memory_order_relaxed);
    }

    const ProfileEvents::Event lock_wait_event;
    mutable std::array<Shard, num_shards> shards;
    mutable std::atomic<size_t> total_count{0};
};

}
