#pragma once
#include <mutex>
#include <Interpreters/Cache/FileCache_fwd.h>
#include <boost/noncopyable.hpp>
#include <map>

namespace DB
{
/**
 * Priority of locking:
 * Cache priority queue guard > key prefix guard > file segment guard.
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
