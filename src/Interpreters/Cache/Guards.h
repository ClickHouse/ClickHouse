#pragma once
#include <mutex>
#include <Interpreters/Cache/FileCache_fwd.h>
#include <boost/noncopyable.hpp>
#include <map>

namespace DB
{

/**
    * Guard for the whole cache object.
    */
struct CacheGuard
{
    struct Lock
    {
        explicit Lock(CacheGuard & guard) : lock(guard.mutex) {}
        std::unique_lock<std::mutex> lock;
    };

    std::mutex mutex;

    Lock lock() { return Lock(*this); }
};

/**
    * Guard for a set of keys.
    * One guard per key prefix (first three digits of the path hash).
    */
struct KeyPrefixGuard
{
    struct Lock
    {
        explicit Lock(KeyPrefixGuard & guard) : lock(guard.mutex) {}
        std::unique_lock<std::mutex> lock;
    };

    std::mutex mutex;

    Lock lock() { return Lock(*this); }

    KeyPrefixGuard() = default;
};
using KeyPrefixGuardPtr = std::shared_ptr<KeyPrefixGuard>;

struct CachePriorityQueueGuard
{
    struct Lock
    {
        explicit Lock(CachePriorityQueueGuard & guard) : lock(guard.mutex) {}
        std::unique_lock<std::mutex> lock;
    };

    std::mutex mutex;

    Lock lock() { return Lock(*this); }
    std::shared_ptr<Lock> lockShared() { return std::make_shared<Lock>(*this); }

    CachePriorityQueueGuard() = default;
};

/**
 * Guard for a file segment.
 * Cache guard > key prefix guard > file segment guard.
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
