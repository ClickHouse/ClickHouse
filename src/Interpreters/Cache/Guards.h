#pragma once
#include <mutex>
#include <boost/noncopyable.hpp>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/SharedMutex.h>
#include <Common/SharedLockGuard.h>
#include <absl/synchronization/mutex.h>

namespace ProfileEvents
{
    extern const Event FilesystemCacheStateLockMicroseconds;
    extern const Event FilesystemCachePriorityWriteLockMicroseconds;
    extern const Event FilesystemCachePriorityReadLockMicroseconds;
}

namespace DB
{
/**
 * FileCache::get/getOrSet/set
 * 1. CacheMetadataGuard::Lock (take key lock and release metadata lock)
 * 2. KeyGuard::Lock (hold till the end of the method)
 *
 * FileCache::tryReserve
 * 1. CachePriorityGuard::WriteLock, CachePriorityGuard::ReadLock
 * 2. KeyGuard::Lock (taken without metadata lock)
 * 3. any number of KeyGuard::Lock's for files which are going to be evicted (taken via metadata lock)
 * 4. CacheStateGuard (to update state (total size/elements) after successful space reservation).
 *
 * FileCache::removeIfExists
 * 1. CachePriorityGuard::WriteLock
 * 2. KeyGuard::Lock (taken via metadata lock)
 * 3. FileSegmentGuard::Lock
 *
 * FileCache::removeAllReleasable
 * 1. CachePriorityGuard::WriteLock
 * 2. any number of KeyGuard::Lock's locks (taken via metadata lock), but at a moment of time only one key lock can be hold
 * 3. FileSegmentGuard::Lock
 *
 * FileCache::getSnapshot (for all cache)
 * 1. metadata lock
 * 2. any number of KeyGuard::Lock's locks (taken via metadata lock), but at a moment of time only one key lock can be hold
 * 3. FileSegmentGuard::Lock
 *
 * FileCache::getSnapshot(key)
 * 1. KeyGuard::Lock (taken via metadata lock)
 * 2. FileSegmentGuard::Lock
 *
 * FileSegment::complete
 * 1. KeyGuard::Lock (taken without metadata lock)
 * 2. FileSegmentGuard::Lock
 *
 * Rules:
 * 1. Priority of locking: CachePriorityGuard::Lock > CacheMetadataGuard::Lock > KeyGuard::Lock > FileSegmentGuard::Lock
 *
 *
 *                                 _CachePriorityGuard_ / _CacheStateGuard
 *                                 1. FileCache::tryReserve
 *                                 2. FileCache::removeIfExists(key)
 *                                 3. FileCache::removeAllReleasable
 *                                 4. FileSegment::complete
 *
 *             _KeyGuard_                                      _CacheMetadataGuard_
 *             1. all from CachePriorityGuard                  1. getOrSet/get/set
 *             2. getOrSet/get/Set
 *
 * *This table does not include locks taken for introspection and system tables.
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
    using WriteLock = std::unique_lock<SharedMutex>;
    using ReadLock = std::shared_lock<SharedMutex>;

    ReadLock tryReadLock() { return ReadLock(mutex, std::try_to_lock); }
    WriteLock tryWriteLock() { return WriteLock(mutex, std::try_to_lock); }

    ReadLock readLock()
    {
        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FilesystemCachePriorityReadLockMicroseconds);
        return ReadLock(mutex);
    }
    WriteLock writeLock()
    {
        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FilesystemCachePriorityWriteLockMicroseconds);
        return WriteLock(mutex);
    }

private:
    SharedMutex mutex;
};

/// State lock protects cache total size/elements counters.
struct CacheStateGuard : private boost::noncopyable
{
    using Mutex = std::timed_mutex;

    struct Lock : public std::unique_lock<Mutex>
    {
        using Base = std::unique_lock<Mutex>;
        using Base::Base;

        explicit Lock(Mutex & mutex_) : std::unique_lock<Mutex>(mutex_) {}
    };

    Lock tryLock() { return Lock(mutex, std::try_to_lock); }

    Lock lock()
    {
        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FilesystemCacheStateLockMicroseconds);
        return Lock(mutex);
    }

    Lock tryLockFor(const std::chrono::milliseconds & acquire_timeout)
    {
        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::FilesystemCacheStateLockMicroseconds);
        return Lock(mutex, std::chrono::duration<double, std::milli>(acquire_timeout));
    }

private:
    Mutex mutex;
};

/**
 * Guard for cache metadata.
 */
struct CacheMetadataGuard : private boost::noncopyable
{
    struct Lock : public std::unique_lock<std::mutex>
    {
        explicit Lock(std::mutex & mutex_) : std::unique_lock<std::mutex>(mutex_) {}
    };

    Lock lock() { return Lock(mutex); }
    std::mutex mutex;
};

/**
 * Key guard. A separate guard for each cache key.
 */
struct KeyGuard : private boost::noncopyable
{
    struct Lock : public std::unique_lock<std::mutex>
    {
        explicit Lock(std::mutex & mutex_) : std::unique_lock<std::mutex>(mutex_) {}
    };

    Lock lock() { return Lock(mutex); }
    std::mutex mutex;
};

/**
 * Guard for a file segment.
 */
struct FileSegmentGuard : private boost::noncopyable
{
    struct Lock : public std::unique_lock<std::mutex>
    {
        explicit Lock(std::mutex & mutex_) : std::unique_lock<std::mutex>(mutex_) {}
    };

    Lock lock() { return Lock(mutex); }
    std::mutex mutex;
};

}
