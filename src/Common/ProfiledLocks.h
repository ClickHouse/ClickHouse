#pragma once

#include <Common/SharedMutex.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <base/defines.h>

#include <mutex>
#include <shared_mutex>


namespace DB
{

/// RAII lock guard that measures wait-time on contention only.
/// Uses try_to_lock to detect contention: uncontended acquisitions have zero timing overhead.
/// Only increments the wait-event when the lock was actually contended (try_lock failed).
template <typename MutexType, typename LockType>
class TSA_SCOPED_LOCKABLE ProfiledLockBase
{
public:
    ProfiledLockBase(MutexType & mutex, ProfileEvents::Event wait_event_) TSA_ACQUIRE(mutex)
        : underlying_lock(mutex, std::try_to_lock), wait_event(wait_event_)
    {
        if (!underlying_lock.owns_lock())
        {
            Stopwatch wait_watch;
            underlying_lock.lock();
            ProfileEvents::increment(wait_event, wait_watch.elapsedMicroseconds());
        }
    }

    template <typename T>
    ProfiledLockBase(MutexType & mutex, ProfileEvents::Event wait_event_, T && lock_arg)
        : underlying_lock(mutex, std::forward<T>(lock_arg)), wait_event(wait_event_)
    {}

    ProfiledLockBase(const ProfiledLockBase &) = delete;
    ProfiledLockBase & operator=(const ProfiledLockBase &) = delete;
    ProfiledLockBase(ProfiledLockBase &&) = default;
    ProfiledLockBase & operator=(ProfiledLockBase &&) = default;

    ~ProfiledLockBase() TSA_RELEASE()
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

    bool owns_lock() const noexcept { return underlying_lock.owns_lock(); }
    explicit operator bool() const noexcept { return underlying_lock.owns_lock(); }

private:
    LockType underlying_lock;
    ProfileEvents::Event wait_event;
};

using ProfiledMutexLock = ProfiledLockBase<std::mutex, std::unique_lock<std::mutex>>;
using ProfiledTimedMutexLock = ProfiledLockBase<std::timed_mutex, std::unique_lock<std::timed_mutex>>;
using ProfiledExclusiveLock = ProfiledLockBase<SharedMutex, std::unique_lock<SharedMutex>>;
using ProfiledSharedLock = ProfiledLockBase<SharedMutex, std::shared_lock<SharedMutex>>;
}
