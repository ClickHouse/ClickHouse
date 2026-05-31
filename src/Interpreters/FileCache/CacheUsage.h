#pragma once
#include <Interpreters/FileCache/IFileCachePriority.h>
#include <Interpreters/FileCache/FileCacheOriginInfo.h>
#include <Common/CurrentMetrics.h>
#include <base/defines.h>
#include <boost/noncopyable.hpp>
#include <mutex>
#include <unordered_map>

namespace CurrentMetrics
{
    extern const Metric FilesystemCacheOvercommitUsers;
}

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
    /// priority is a non-owning pointer; the pointed-to object is owned by CacheUsagePerUser::CacheUserData.
    CacheUsage(const FileCacheOriginInfo & origin_info_, IFileCachePriority * priority_);

    const FileCacheOriginInfo origin_info;
    /// A user priority, contains only entries which belong to `user`
    /// by corresponding eviction strategy priority.
    IFileCachePriority * const priority{};

    std::shared_ptr<CacheUsageStatGuard> guard;

    void update(int64_t size, int64_t elements, const CacheUsageStatGuard::Lock & lock);

    std::atomic<UInt64> total_size = 0;
    std::atomic<UInt64> total_elements = 0;

    bool operator <(const CacheUsage & other) const;
    bool operator ==(const CacheUsage & other) const;

    bool lessWithAssumption(const CacheUsage & other, size_t released_size_assumption, size_t other_released_size_assumption) const;

};
using CacheUsagePtr = std::shared_ptr<CacheUsage>;


/// Owns the per-user cache usage map, each method is thread-safe.
struct CacheUsagePerUser : private boost::noncopyable
{
    using UserID = std::string;

    size_t size() const;

    /// Returns the usages of all non-empty users (lazy cleanup of empty entries as a side effect).
    std::vector<CacheUsagePtr> snapshot() const;

    /// Returns nullptr when not found.
    CacheUsagePtr tryGet(const UserID & user_id) const;

    /// Returns the existing entry or inserts a new one created by make().
    /// The returned CacheUsagePtr keeps use_count > 1 and prevents
    /// user's usage from being cleaned up due to being empty.
    CacheUsagePtr getOrSet(
        const UserID & user_id,
        std::function<std::pair<FileCachePriorityPtr, CacheUsagePtr>()> make);

private:
    struct CacheUserData
    {
        CacheUsagePtr usage;
        FileCachePriorityPtr priority;
        CurrentMetrics::Increment metric_increment{CurrentMetrics::FilesystemCacheOvercommitUsers};
    };

    bool canRemoveUser(const CacheUsagePtr & usage) const TSA_REQUIRES(mutex);

    mutable std::unordered_map<UserID, CacheUserData> map TSA_GUARDED_BY(mutex);
    mutable std::mutex mutex;
};

}
