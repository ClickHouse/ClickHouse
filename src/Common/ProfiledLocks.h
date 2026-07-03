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

/// RAII lock guard that measures both wait-time and hold-time for std::mutex.
/// Increments the wait-event after the lock is acquired, and the hold-event when the guard is destroyed.
class TSA_SCOPED_LOCKABLE ProfiledMutexLock final : private boost::noncopyable
{
public:
    ProfiledMutexLock(std::mutex & mutex, ProfileEvents::Event wait_event_, ProfileEvents::Event hold_event_) TSA_ACQUIRE(mutex)
        : wait_event(wait_event_)
        , hold_event(hold_event_)
    {
        Stopwatch wait_watch;
        std::unique_lock<std::mutex> l(mutex);
        ProfileEvents::increment(wait_event, wait_watch.elapsedMicroseconds());
        underlying_lock = std::move(l);
        hold_watch.restart();
    }

    ~ProfiledMutexLock() TSA_RELEASE()
    {
        if (underlying_lock.owns_lock())
        {
            UInt64 elapsed = hold_watch.elapsedMicroseconds();
            underlying_lock.unlock();
            ProfileEvents::increment(hold_event, elapsed);
        }
    }

    void unlock() TSA_RELEASE()
    {
        UInt64 elapsed = hold_watch.elapsedMicroseconds();
        underlying_lock.unlock();
        ProfileEvents::increment(hold_event, elapsed);
    }

    void lock() TSA_ACQUIRE()
    {
        Stopwatch wait_watch;
        underlying_lock.lock();
        ProfileEvents::increment(wait_event, wait_watch.elapsedMicroseconds());
        hold_watch.restart();
    }

private:
    std::unique_lock<std::mutex> underlying_lock;
    ProfileEvents::Event wait_event;
    ProfileEvents::Event hold_event;
    Stopwatch hold_watch;
};

/// RAII lock guard that measures both wait-time and hold-time for exclusive (write) locks on SharedMutex.
/// Increments the wait-event after the lock is acquired, and the hold-event when the guard is destroyed.
class TSA_SCOPED_LOCKABLE ProfiledExclusiveLock final : private boost::noncopyable
{
public:
    ProfiledExclusiveLock(SharedMutex & mutex, ProfileEvents::Event wait_event_, ProfileEvents::Event hold_event_) TSA_ACQUIRE(mutex)
        : wait_event(wait_event_)
        , hold_event(hold_event_)
    {
        Stopwatch wait_watch;
        std::unique_lock<SharedMutex> l(mutex);
        ProfileEvents::increment(wait_event, wait_watch.elapsedMicroseconds());
        underlying_lock = std::move(l);
        hold_watch.restart();
    }

    ~ProfiledExclusiveLock() TSA_RELEASE()
    {
        if (underlying_lock.owns_lock())
        {
            UInt64 elapsed = hold_watch.elapsedMicroseconds();
            underlying_lock.unlock();
            ProfileEvents::increment(hold_event, elapsed);
        }
    }

    void unlock() TSA_RELEASE()
    {
        UInt64 elapsed = hold_watch.elapsedMicroseconds();
        underlying_lock.unlock();
        ProfileEvents::increment(hold_event, elapsed);
    }

    void lock() TSA_ACQUIRE()
    {
        Stopwatch wait_watch;
        underlying_lock.lock();
        ProfileEvents::increment(wait_event, wait_watch.elapsedMicroseconds());
        hold_watch.restart();
    }

private:
    std::unique_lock<SharedMutex> underlying_lock;
    ProfileEvents::Event wait_event;
    ProfileEvents::Event hold_event;
    Stopwatch hold_watch;
};


/// RAII lock guard that measures both wait-time and hold-time for shared (read) locks on SharedMutex.
class TSA_SCOPED_LOCKABLE ProfiledSharedLock final : private boost::noncopyable
{
public:
    ProfiledSharedLock(SharedMutex & mutex, ProfileEvents::Event wait_event_, ProfileEvents::Event hold_event_) TSA_ACQUIRE_SHARED(mutex)
        : wait_event(wait_event_)
        , hold_event(hold_event_)
    {
        Stopwatch wait_watch;
        std::shared_lock<SharedMutex> l(mutex);
        ProfileEvents::increment(wait_event, wait_watch.elapsedMicroseconds());
        underlying_lock = std::move(l);
        hold_watch.restart();
    }

    ~ProfiledSharedLock() TSA_RELEASE()
    {
        if (underlying_lock.owns_lock())
        {
            UInt64 elapsed = hold_watch.elapsedMicroseconds();
            underlying_lock.unlock();
            ProfileEvents::increment(hold_event, elapsed);
        }
    }

    void unlock() TSA_RELEASE()
    {
        UInt64 elapsed = hold_watch.elapsedMicroseconds();
        underlying_lock.unlock();
        ProfileEvents::increment(hold_event, elapsed);
    }

    void lock() TSA_ACQUIRE()
    {
        Stopwatch wait_watch;
        underlying_lock.lock();
        ProfileEvents::increment(wait_event, wait_watch.elapsedMicroseconds());
        hold_watch.restart();
    }

private:
    std::shared_lock<SharedMutex> underlying_lock;
    ProfileEvents::Event wait_event;
    ProfileEvents::Event hold_event;
    Stopwatch hold_watch;
};

}
