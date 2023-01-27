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
    struct Lock
    {
        explicit Lock(CacheGuard & guard) : lock(guard.mutex) {}
        std::unique_lock<std::mutex> lock;
    };
    using LockPtr = std::shared_ptr<Lock>;

    std::mutex mutex;

    LockPtr lock() { return std::make_shared<Lock>(*this); }

    CacheGuard() = default;
};

/**
 * Guard for cache metadata.
 */
struct CacheMetadataGuard
{
    struct Lock
    {
        explicit Lock(CacheMetadataGuard & guard) : lock(guard.mutex) {}
        std::unique_lock<std::mutex> lock;
    };
    using LockPtr = std::shared_ptr<Lock>;

    std::mutex mutex;

    Lock lock() { return Lock(*this); }

    CacheMetadataGuard() = default;
};

/**
 * Guard for a set of keys.
 * One guard per key prefix (first three digits of the path hash).
 */
struct KeyGuard
{
    struct Lock
    {
        explicit Lock(KeyGuard & guard) : lock(guard.mutex) {}
        std::unique_lock<std::mutex> lock;
    };

    std::mutex mutex;

    Lock lock() { return Lock(*this); }

    KeyGuard() = default;
};
using KeyGuardPtr = std::shared_ptr<KeyGuard>;

/**
 * Guard for a file segment.
 */
struct FileSegmentGuard
{
    struct Lock
    {
        explicit Lock(FileSegmentGuard & guard) : lock(guard.mutex) {}
        std::unique_lock<std::mutex> lock;
    };

    std::mutex mutex;

    Lock lock() { return Lock(*this); }
};

}
