#pragma once
#include <Interpreters/Cache/IFileCachePriority.h>
#include <Interpreters/Cache/UserInfo.h>
#include <boost/noncopyable.hpp>

namespace DB
{

/// This mutex guard is needed only for correctness of `OvercommitFileCachePriority::check` method,
/// but not for cache correctness in general,
/// because it makes sure per client's counters
/// (`total_size` and `total_elements` in `CacheUsage` per client state)
/// are updated atomically with main cache state counters.
/// The per-client `total_size` and `total_elements` counters are used for:
/// 1. to decide from whom to evict according to overcommit policy
/// 2. for system tables
/// In both cases some temporary divergence from actual cache counters is fine,
/// but we now have no divergence as atomicity is needed for `OvercommitFileCachePriority::check`'s success.
struct CacheUsageStatGuard : private boost::noncopyable
{
    struct Lock : public std::unique_lock<std::mutex> { explicit Lock(std::mutex & mutex_) : std::unique_lock<std::mutex>(mutex_) {} };
    Lock lock() { return Lock(mutex); }
    std::mutex mutex;
};

/// A caching eviction strategy, which allows to evict more from users which use the cache more.
/// From each user cache is evicted according to LRU/SLRU eviction policies.
struct CacheUsage
{
    CacheUsage(const FileCacheUserInfo & user_, FileCachePriorityPtr priority_);

    const FileCacheUserInfo user;
    /// A user priority, contains only entries which belong to `user`
    /// by corresponding eviction strategy priority.
    const FileCachePriorityPtr priority{};

    std::shared_ptr<CacheUsageStatGuard> guard;

    void update(int64_t size, int64_t elements, const CacheUsageStatGuard::Lock & lock);

    std::atomic<UInt64> total_size = 0;
    std::atomic<UInt64> total_elements = 0;

    bool operator <(const CacheUsage & other) const;
    bool operator ==(const CacheUsage & other) const;

    bool lessWithAssumption(const CacheUsage & other, size_t released_size_assumption, size_t other_released_size_assumption) const;

};
using CacheUsagePtr = std::shared_ptr<CacheUsage>;

}
