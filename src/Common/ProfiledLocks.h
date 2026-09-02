#pragma once

#include <Common/SharedMutex.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <base/defines.h>

#include <chrono>
#include <mutex>
#include <shared_mutex>
#include <utility>


namespace DB
{

/// Shared implementation without TSA annotations; public wrappers below own the acquire/release annotations.
template <typename MutexType, typename LockType>
class ProfiledLockBase : private LockType
{
protected:
    explicit ProfiledLockBase(MutexType & mutex, ProfileEvents::Event wait_event_)
        : LockType(mutex, std::try_to_lock)
        , wait_event(wait_event_)
    {
        if (!LockType::owns_lock())
        {
            lockAndProfileWait();
        }
    }

    template <typename LockArg>
    ProfiledLockBase(MutexType & mutex, ProfileEvents::Event wait_event_, LockArg && lock_arg)
        : LockType(mutex, std::forward<LockArg>(lock_arg))
        , wait_event(wait_event_)
    {
    }

    template <typename Rep, typename Period>
    ProfiledLockBase(MutexType & mutex, ProfileEvents::Event wait_event_, std::chrono::duration<Rep, Period> timeout)
        : LockType(mutex, std::try_to_lock)
        , wait_event(wait_event_)
    {
        if (!LockType::owns_lock())
        {
            tryLockAndProfileWait(timeout);
        }
    }

    ~ProfiledLockBase() = default;

    void unlockImpl() { LockType::unlock(); }

    void lockImpl()
    {
        if (LockType::try_lock())
            return;

        lockAndProfileWait();
    }

    void lockAndProfileWait()
    {
        Stopwatch wait_watch;
        LockType::lock();
        ProfileEvents::increment(wait_event, wait_watch.elapsedMicroseconds());
    }

    template <typename Rep, typename Period>
    void tryLockAndProfileWait(std::chrono::duration<Rep, Period> timeout)
    {
        Stopwatch watch;
        LockType::try_lock_for(timeout);
        ProfileEvents::increment(wait_event, watch.elapsedMicroseconds());
    }

public:
    ProfiledLockBase(const ProfiledLockBase &) = delete;
    ProfiledLockBase & operator=(const ProfiledLockBase &) = delete;
    ProfiledLockBase(ProfiledLockBase &&) = default;
    ProfiledLockBase & operator=(ProfiledLockBase &&) = default;

    using LockType::mutex;
    using LockType::operator bool;
    using LockType::owns_lock;

private:
    ProfileEvents::Event wait_event;
};

template <typename MutexType>
class TSA_SCOPED_LOCKABLE ProfiledExclusiveLock : private ProfiledLockBase<MutexType, std::unique_lock<MutexType>>
{
    using Base = ProfiledLockBase<MutexType, std::unique_lock<MutexType>>;

public:
    ProfiledExclusiveLock(MutexType & mutex, ProfileEvents::Event wait_event_) TSA_ACQUIRE(mutex)
        : Base(mutex, wait_event_)
    {
    }

    template <typename... Args>
    ProfiledExclusiveLock(MutexType & mutex, ProfileEvents::Event wait_event_, Args &&... args)
        : Base(mutex, wait_event_, std::forward<Args>(args)...)
    {
    }

    ProfiledExclusiveLock(const ProfiledExclusiveLock &) = delete;
    ProfiledExclusiveLock & operator=(const ProfiledExclusiveLock &) = delete;
    ProfiledExclusiveLock(ProfiledExclusiveLock &&) = default;
    ProfiledExclusiveLock & operator=(ProfiledExclusiveLock &&) = default;

    ~ProfiledExclusiveLock() TSA_RELEASE() = default;

    void unlock() TSA_RELEASE() { Base::unlockImpl(); }

    void lock() TSA_ACQUIRE() { Base::lockImpl(); }

    using Base::mutex;
    using Base::operator bool;
    using Base::owns_lock;
};

template <typename MutexType>
class TSA_SCOPED_LOCKABLE ProfiledSharedLock : private ProfiledLockBase<MutexType, std::shared_lock<MutexType>>
{
    using Base = ProfiledLockBase<MutexType, std::shared_lock<MutexType>>;

public:
    ProfiledSharedLock(MutexType & mutex, ProfileEvents::Event wait_event_) TSA_ACQUIRE_SHARED(mutex)
        : Base(mutex, wait_event_)
    {
    }

    template <typename... Args>
    ProfiledSharedLock(MutexType & mutex, ProfileEvents::Event wait_event_, Args &&... args)
        : Base(mutex, wait_event_, std::forward<Args>(args)...)
    {
    }

    ProfiledSharedLock(const ProfiledSharedLock &) = delete;
    ProfiledSharedLock & operator=(const ProfiledSharedLock &) = delete;
    ProfiledSharedLock(ProfiledSharedLock &&) = default;
    ProfiledSharedLock & operator=(ProfiledSharedLock &&) = default;

    ~ProfiledSharedLock() TSA_RELEASE() = default;

    void unlock() TSA_RELEASE() { Base::unlockImpl(); }

    void lock() TSA_ACQUIRE_SHARED() { Base::lockImpl(); }

    using Base::mutex;
    using Base::operator bool;
    using Base::owns_lock;
};

template <typename MutexType, typename... Args>
ProfiledExclusiveLock(MutexType &, ProfileEvents::Event, Args &&...) -> ProfiledExclusiveLock<MutexType>;

template <typename MutexType, typename... Args>
ProfiledSharedLock(MutexType &, ProfileEvents::Event, Args &&...) -> ProfiledSharedLock<MutexType>;

}
