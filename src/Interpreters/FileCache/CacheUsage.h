#pragma once
#include <Interpreters/FileCache/IFileCachePriority.h>
#include <Interpreters/FileCache/FileCacheOriginInfo.h>
#include <Interpreters/FileCache/ShardedMap.h>
#include <Common/CurrentMetrics.h>
#include <base/defines.h>
#include <fmt/format.h>
#include <boost/noncopyable.hpp>
#include <atomic>
#include <chrono>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>

namespace CurrentMetrics
{
    extern const Metric FilesystemCacheOvercommitUsers;
}

namespace DB
{

/// A caching eviction strategy, which allows to evict more from users which use the cache more.
/// From each user cache is evicted according to LRU/SLRU eviction policies.
struct CacheUsage
{
    /// priority is a non-owning pointer; the pointed-to object is owned by CacheUsagePerUser::CacheUserData.
    /// Creating the entry counts as the client's first access, unless it is created by the
    /// startup load from the filesystem: then the last access is not known.
    CacheUsage(const FileCacheOriginInfo & origin_info_, IFileCachePriority * priority_, bool is_initial_load = false);

    /// The wall clock, so that the last access can be reported as is in system tables.
    /// The value does not survive a restart anyway, so monotonicity buys nothing here.
    using Clock = std::chrono::system_clock;
    using TimePoint = Clock::time_point;

    const FileCacheOriginInfo origin_info;
    /// A user priority, contains only entries which belong to `user`
    /// by corresponding eviction strategy priority.
    IFileCachePriority * const priority{};

    /// Applies a delta to `total_size`/`total_elements`. They are plain atomics, updated separately
    /// from the priority's own counters, so they can transiently diverge from them; they feed only
    /// overcommit eviction weights and system tables.
    /// Defined inline: the public repo has no `CacheUsage.cpp`, but its `LRUFileCachePriority`
    /// calls `update`, so the definition must travel with this header.
    void update(int64_t size, int64_t elements)
    {
        if (!size && !elements)
            return;

        chassert(
            int64_t(total_size) + size >= 0,
            fmt::format("Inconsistency in cache. Total size: {}, update: {}", total_size.load(), size));
        chassert(
            int64_t(total_elements) + elements >= 0,
            fmt::format("Inconsistency in cache. Total elements: {}, update: {}", total_elements.load(), elements));

        total_size += size;
        total_elements += elements;
    }

    std::pair<size_t, size_t> getTotal() const
    {
        return {total_size, total_elements};
    }

    std::atomic<UInt64> total_size = 0;
    std::atomic<UInt64> total_elements = 0;

    /// Last cache access by this client, for the idle-client TTL.
    void touch(TimePoint now) { last_access.store(now, std::memory_order_relaxed); }
    /// Empty for a client whose entries were only loaded from the filesystem on startup:
    /// its last access happened before the restart and is not known.
    std::optional<TimePoint> getLastAccess() const
    {
        const auto value = last_access.load(std::memory_order_relaxed);
        if (value == TimePoint{})
            return {};
        return value;
    }
    /// While the last access is unknown, the TTL counts from the moment the client's
    /// entries appeared, so a client loaded from disk and never used still expires.
    bool idleFor(std::chrono::seconds ttl, TimePoint now) const
    {
        return now - getLastAccess().value_or(created_at) >= ttl;
    }

    bool operator <(const CacheUsage & other) const;
    bool operator ==(const CacheUsage & other) const;

    bool lessWithAssumption(const CacheUsage & other, size_t released_size_assumption, size_t other_released_size_assumption) const;

private:
    const TimePoint created_at;
    /// `TimePoint{}` means the last access is not known (loaded from the filesystem).
    std::atomic<TimePoint> last_access;
};
using CacheUsagePtr = std::shared_ptr<CacheUsage>;


/// Owns the per-user cache usage map, each method is thread-safe.
struct CacheUsagePerUser : private boost::noncopyable
{
    using UserID = std::string;

    CacheUsagePerUser();

    size_t size() const { return clients_map.size(); }

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

    void touchClient(const UserID & user_id);

    /// Idle clients (>= ttl) with their usage, so the caller can re-check without
    /// a second lookup. Reaps empty unreferenced entries in passing.
    std::vector<std::pair<UserID, CacheUsagePtr>> collectIdleClients(std::chrono::seconds ttl) const;

private:
    struct CacheUserData
    {
        FileCachePriorityPtr priority;
        /// `usage` is declared after `priority`, so it is destroyed first: the final
        /// reference-count decrement of `usage` synchronizes with the releases of the
        /// copies handed out by `snapshot`, ordering the destruction of `priority`
        /// (and its queue) after any concurrent use of it through those copies.
        CacheUsagePtr usage;
        CurrentMetrics::Increment metric_increment{CurrentMetrics::FilesystemCacheOvercommitUsers};
    };

    static bool canRemoveUser(const CacheUsagePtr & usage);

    FileCacheUtils::ShardedMap<UserID, CacheUserData> clients_map;
};

}
