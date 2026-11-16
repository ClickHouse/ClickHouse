#pragma once
#include <mutex>
#include <boost/noncopyable.hpp>
#include <Common/ProfiledLocks.h>
#include <Common/SharedMutex.h>
#include <Common/SharedLockGuard.h>
#include <absl/synchronization/mutex.h>

namespace ProfileEvents
{
    extern const Event FilesystemCacheStateLockMicroseconds;
    extern const Event FilesystemCachePriorityWriteLockMicroseconds;
    extern const Event FilesystemCachePriorityReadLockMicroseconds;
    extern const Event FileSegmentLockMicroseconds;
    extern const Event FilesystemCacheLockKeyMicroseconds;
    extern const Event FilesystemCacheLockMetadataMicroseconds;
}

namespace DB
{
/**
 * Locking order - when taking a lock, every lock already held must be strictly above it here:
 *
 *   FileCache::apply_settings_mutex
 *   > FileCache::dynamic_resize_lock  (exclusive, try_lock_for: doDynamicResize; shared, try_lock: tryReserve, tryIncreasePriority)
 *   > CacheStateGuard                 (makes growth atomic: `canFit` + `entryAdd`; decrements are lock-free, so
 *                                      `entrySub` is callable under KeyGuard, and `getSize(Lock)` is exact only
 *                                      with respect to concurrent growth - its sole difference from `getSizeApprox`)
 *   > FileCacheQueryLimit::mutex      (`doTryReserve` calls `tryGetQueryContext` under the state lock)
 *   > CachePriorityGuard              (one per queue; taken by the priority itself, or by `EvictionCandidates`
 *                                      through `Iterator::getPriorityGuard`;
 *                                      SLRU: both sub-queues share one guard, entries move between them;
 *                                      Overcommit/Split wrappers: no own guard, `getPriorityGuard` throws)
 *   > CacheMetadataGuard
 *   > KeyGuard
 *   > FileSegmentGuard
 *
 * Leaf mutexes (nothing above is taken while they are held):
 *
 *   ShardedMap shard mutexes | LRUFileCachePriority::eviction_pos_mutex
 *   | LRUFileCachePriority::invalidated_mutex | SLRUIterator::entry_mutex
 *
 * The CacheStateGuard > CachePriorityGuard edge is exercised only by startup load (`add` takes the
 * queue write lock under the state lock) and by dynamic-resize entry removal and restore
 * (`EvictionCandidates::removeQueueEntries` / `restoreQueueEntries`).
 *
 * Introspection and system-table locks are not included.
 */

/**
 * Cache priority queue guard.
 * "Write" lock is for priority queue structure modifications,
 * (like adding, moving and removing elements).
 * "Read" lock is for read-only iteration of priority queue
 * (like collection of eviction candidates).
 */
struct CachePriorityGuard : private boost::noncopyable
{
    /// struct is used (not keyword `using`) to make CachePriorityGuard::Lock
    /// non-interchangable with other guards locks,
    /// so we wouldn't be able to pass CachePriorityGuard::Lock to a function
    /// which accepts KeyGuard::Lock.
    using WriteLock = ProfiledExclusiveLock<SharedMutex>;
    using ReadLock = ProfiledSharedLock<SharedMutex>;

    ReadLock tryReadLock() TSA_NO_THREAD_SAFETY_ANALYSIS
    {
        return ReadLock(mutex, ProfileEvents::FilesystemCachePriorityReadLockMicroseconds, std::try_to_lock);
    }
    WriteLock tryWriteLock() TSA_NO_THREAD_SAFETY_ANALYSIS
    {
        return WriteLock(mutex, ProfileEvents::FilesystemCachePriorityWriteLockMicroseconds, std::try_to_lock);
    }

    ReadLock readLock() TSA_NO_THREAD_SAFETY_ANALYSIS
    {
        return ReadLock(mutex, ProfileEvents::FilesystemCachePriorityReadLockMicroseconds);
    }

    WriteLock writeLock() TSA_NO_THREAD_SAFETY_ANALYSIS
    {
        return WriteLock(mutex, ProfileEvents::FilesystemCachePriorityWriteLockMicroseconds);
    }

private:
    SharedMutex mutex;
};

/// Makes cache growth atomic: `canFit` followed by the size/elements increment.
/// Does not protect the counters themselves - decrements are lock-free (see the
/// locking-order comment at the top of this file).
struct CacheStateGuard : private boost::noncopyable
{
    struct Lock : public ProfiledExclusiveLock<std::timed_mutex>
    {
        using Base = ProfiledExclusiveLock<std::timed_mutex>;
        using Base::Base;
    };

    Lock tryLock() TSA_NO_THREAD_SAFETY_ANALYSIS
    {
        return Lock(mutex, ProfileEvents::FilesystemCacheStateLockMicroseconds, std::try_to_lock);
    }

    Lock lock() TSA_NO_THREAD_SAFETY_ANALYSIS { return Lock(mutex, ProfileEvents::FilesystemCacheStateLockMicroseconds); }

    Lock tryLockFor(const std::chrono::milliseconds & acquire_timeout) TSA_NO_THREAD_SAFETY_ANALYSIS
    {
        return Lock(mutex, ProfileEvents::FilesystemCacheStateLockMicroseconds, std::chrono::duration<double, std::milli>(acquire_timeout));
    }

private:
    std::timed_mutex mutex;
};

/**
 * Guard for cache metadata.
 */
struct CacheMetadataGuard : private boost::noncopyable
{
    struct Lock : public ProfiledExclusiveLock<std::mutex>
    {
        explicit Lock(std::mutex & mutex_)
            : ProfiledExclusiveLock<std::mutex>(mutex_, ProfileEvents::FilesystemCacheLockMetadataMicroseconds) {}
    };

    Lock lock() { return Lock(mutex); }
    std::mutex mutex;
};

/**
 * Key guard. A separate guard for each cache key.
 */
struct KeyGuard : private boost::noncopyable
{
    struct Lock : public ProfiledExclusiveLock<std::mutex>
    {
        explicit Lock(std::mutex & mutex_)
            : ProfiledExclusiveLock<std::mutex>(mutex_, ProfileEvents::FilesystemCacheLockKeyMicroseconds) {}
    };

    Lock lock() { return Lock(mutex); }
    std::mutex mutex;
};

/**
 * Guard for a file segment.
 */
struct FileSegmentGuard : private boost::noncopyable
{
    struct Lock : public ProfiledExclusiveLock<std::mutex>
    {
        explicit Lock(std::mutex & mutex_)
            : ProfiledExclusiveLock<std::mutex>(mutex_, ProfileEvents::FileSegmentLockMicroseconds) {}
    };

    Lock lock() { return Lock(mutex); }
    std::mutex mutex;
};

}
