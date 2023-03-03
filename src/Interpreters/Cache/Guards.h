#pragma once
#include <mutex>
#include <Interpreters/Cache/FileCache_fwd.h>
#include <boost/noncopyable.hpp>
#include <map>

namespace DB
{
/**
 * FileCache::get/getOrSet/set
 * 1. CacheMetadataGuard::Lock (take key lock and relase metadata lock)
 * 2. KeyGuard::Lock (hold till the end of the method)
 *
 * FileCache::tryReserve
 * 1. CacheGuard::Lock
 * 2. KeyGuard::Lock (taken without metadata lock)
 * 3. any number of KeyGuard::Lock's for files which are going to be evicted (taken via metadata lock)
 *
 * FileCache::removeIfExists
 * 1. CacheGuard::Lock
 * 2. KeyGuard::Lock (taken via metadata lock)
 * 3. FileSegmentGuard::Lock
 *
 * FileCache::removeAllReleasable
 * 1. CacheGuard::Lock
 * 2. any number of KeyGuard::Lock's locks (takken via metadata lock), but at a moment of time only one key lock can be hold
 * 3. FileSegmentGuard::Lock
 *
 * FileCache::getSnapshot (for all cache)
 * 1. metadata lock
 * 2. any number of KeyGuard::Lock's locks (takken via metadata lock), but at a moment of time only one key lock can be hold
 * 3. FileSegmentGuard::Lock
 *
 * FileCache::getSnapshot(key)
 * 1. KeyGuard::Lock (taken via metadata lock)
 * 2. FileSegmentGuard::Lock
 *
 * FileSegment::complete
 * 1. CacheGuard::Lock
 * 2. KeyGuard::Lock (taken without metadata lock)
 * 3. FileSegmentGuard::Lock
 *
 * Rules:
 * 1. Priority of locking: CacheGuard::Lock > CacheMetadataGuard::Lock > KeyGuard::Lock > FileSegmentGuard::Lock
 * 2. If we take more than one key lock at a moment of time, we need to take CacheGuard::Lock (example: tryReserve())
 */

/**
 * Cache priority queue guard.
 */
struct CacheGuard
{
    struct Lock : public std::unique_lock<std::mutex>
    {
        explicit Lock(std::mutex & mutex_) : std::unique_lock<std::mutex>(mutex_) {}
    };

    Lock lock() { return Lock(mutex); }
    std::mutex mutex;
};

/**
 * Guard for cache metadata.
 */
struct CacheMetadataGuard
{
    struct Lock : public std::unique_lock<std::mutex>
    {
        explicit Lock(std::mutex & mutex_) : std::unique_lock<std::mutex>(mutex_) {}
    };

    Lock lock() { return Lock(mutex); }
    std::mutex mutex;
};

/**
 * Guard for a set of keys.
 * One guard per key prefix (first three digits of the path hash).
 */
struct KeyGuard
{
    struct Lock : public std::unique_lock<std::mutex>
    {
        explicit Lock(std::mutex & mutex_) : std::unique_lock<std::mutex>(mutex_) {}
    };

    Lock lock() { return Lock(mutex); }
    std::mutex mutex;
};
using KeyGuardPtr = std::shared_ptr<KeyGuard>;

/**
 * Guard for a file segment.
 */
struct FileSegmentGuard
{
    struct Lock : public std::unique_lock<std::mutex>
    {
        explicit Lock(std::mutex & mutex_) : std::unique_lock<std::mutex>(mutex_) {}
    };

    Lock lock() { return Lock(mutex); }
    std::mutex mutex;
};

}
