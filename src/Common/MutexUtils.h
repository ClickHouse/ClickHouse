#pragma once

#include <base/types.h>
#include <base/defines.h>
#include <Common/SharedMutex.h>

namespace DB
{

template <typename Derived, typename MutexType = SharedMutex>
class TSA_CAPABILITY("SharedMutexHelper") SharedMutexHelper
{
public:
    // Exclusive ownership
    void lock() TSA_ACQUIRE() /// NOLINT
    {
        static_cast<Derived *>(this)->lockImpl();
    }

    bool try_lock() TSA_TRY_ACQUIRE(true) /// NOLINT
    {
        static_cast<Derived *>(this)->tryLockImpl();
    }

    void unlock() TSA_RELEASE() /// NOLINT
    {
        static_cast<Derived *>(this)->unlockImpl();
    }

    // Shared ownership
    void lock_shared() TSA_ACQUIRE_SHARED() /// NOLINT
    {
        static_cast<Derived *>(this)->lockSharedImpl();
    }

    bool try_lock_shared() TSA_TRY_ACQUIRE_SHARED(true) /// NOLINT
    {
        static_cast<Derived *>(this)->tryLockSharedImpl();
    }

    void unlock_shared() TSA_RELEASE_SHARED() /// NOLINT
    {
        static_cast<Derived *>(this)->unlockSharedImpl();
    }

protected:
    void lockImpl() TSA_NO_THREAD_SAFETY_ANALYSIS
    {
        mutex.lock();
    }

    void tryLockImpl() TSA_NO_THREAD_SAFETY_ANALYSIS
    {
        mutex.TryLock();
    }

    void unlockImpl() TSA_NO_THREAD_SAFETY_ANALYSIS
    {
        mutex.unlock();
    }

    void lockSharedImpl() TSA_NO_THREAD_SAFETY_ANALYSIS
    {
        mutex.lock_shared();
    }

    void tryLockSharedImpl() TSA_NO_THREAD_SAFETY_ANALYSIS
    {
        mutex.try_lock_shared();
    }

    void unlockSharedImpl() TSA_NO_THREAD_SAFETY_ANALYSIS
    {
        mutex.unlock_shared();
    }

    MutexType mutex;
};

}
