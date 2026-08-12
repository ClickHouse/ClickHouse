#pragma once

#include <Common/SharedMutex.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <base/defines.h>

#include <boost/noncopyable.hpp>

#include <mutex>
#include <shared_mutex>


namespace DB
{

/// RAII lock guard that measures wait-time on contention only for std::mutex.
/// Uses try_lock to detect contention: uncontended acquisitions have zero timing overhead.
/// Only increments the wait-event when the lock was actually contended (try_lock failed).
class TSA_SCOPED_LOCKABLE ProfiledMutexLock final : private boost::noncopyable
{
public:
    ProfiledMutexLock(std::mutex & mutex, ProfileEvents::Event wait_event_) TSA_ACQUIRE(mutex)
        : wait_event(wait_event_)
    {
        if (mutex.try_lock())
        {
            underlying_lock = std::unique_lock<std::mutex>(mutex, std::adopt_lock);
        }
        else
        {
            Stopwatch wait_watch;
            std::unique_lock<std::mutex> l(mutex);
            ProfileEvents::increment(wait_event, wait_watch.elapsedMicroseconds());
            underlying_lock = std::move(l);
        }
    }

    ~ProfiledMutexLock() TSA_RELEASE()
    {
        if (underlying_lock.owns_lock())
            underlying_lock.unlock();
    }

    void unlock() TSA_RELEASE()
    {
        underlying_lock.unlock();
    }

    void lock() TSA_ACQUIRE()
    {
        if (underlying_lock.try_lock())
            return;

        Stopwatch wait_watch;
        underlying_lock.lock();
        ProfileEvents::increment(wait_event, wait_watch.elapsedMicroseconds());
    }

private:
    std::unique_lock<std::mutex> underlying_lock;
    ProfileEvents::Event wait_event;
};

/// RAII lock guard that measures wait-time on contention only for exclusive (write) locks on SharedMutex.
class TSA_SCOPED_LOCKABLE ProfiledExclusiveLock final : private boost::noncopyable
{
public:
    ProfiledExclusiveLock(SharedMutex & mutex, ProfileEvents::Event wait_event_) TSA_ACQUIRE(mutex)
        : wait_event(wait_event_)
    {
        if (mutex.try_lock())
        {
            underlying_lock = std::unique_lock<SharedMutex>(mutex, std::adopt_lock);
        }
        else
        {
            Stopwatch wait_watch;
            std::unique_lock<SharedMutex> l(mutex);
            ProfileEvents::increment(wait_event, wait_watch.elapsedMicroseconds());
            underlying_lock = std::move(l);
        }
    }

    ~ProfiledExclusiveLock() TSA_RELEASE()
    {
        if (underlying_lock.owns_lock())
            underlying_lock.unlock();
    }

    void unlock() TSA_RELEASE()
    {
        underlying_lock.unlock();
    }

    void lock() TSA_ACQUIRE()
    {
        if (underlying_lock.try_lock())
            return;

        Stopwatch wait_watch;
        underlying_lock.lock();
        ProfileEvents::increment(wait_event, wait_watch.elapsedMicroseconds());
    }

private:
    std::unique_lock<SharedMutex> underlying_lock;
    ProfileEvents::Event wait_event;
};


/// RAII lock guard that measures wait-time on contention only for shared (read) locks on SharedMutex.
class TSA_SCOPED_LOCKABLE ProfiledSharedLock final : private boost::noncopyable
{
public:
    ProfiledSharedLock(SharedMutex & mutex, ProfileEvents::Event wait_event_) TSA_ACQUIRE_SHARED(mutex)
        : wait_event(wait_event_)
    {
        if (mutex.try_lock_shared())
        {
            underlying_lock = std::shared_lock<SharedMutex>(mutex, std::adopt_lock);
        }
        else
        {
            Stopwatch wait_watch;
            std::shared_lock<SharedMutex> l(mutex);
            ProfileEvents::increment(wait_event, wait_watch.elapsedMicroseconds());
            underlying_lock = std::move(l);
        }
    }

    ~ProfiledSharedLock() TSA_RELEASE()
    {
        if (underlying_lock.owns_lock())
            underlying_lock.unlock();
    }

    void unlock() TSA_RELEASE()
    {
        underlying_lock.unlock();
    }

    void lock() TSA_ACQUIRE_SHARED()
    {
        if (underlying_lock.try_lock())
            return;

        Stopwatch wait_watch;
        underlying_lock.lock();
        ProfileEvents::increment(wait_event, wait_watch.elapsedMicroseconds());
    }

private:
    std::shared_lock<SharedMutex> underlying_lock;
    ProfileEvents::Event wait_event;
};

}
