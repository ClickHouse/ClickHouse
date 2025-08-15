#pragma once

#include <base/types.h>
#include <base/defines.h>
#include <Common/SharedMutex.h>

namespace DB
{

/** SharedMutexHelper class allows to inject specific logic when underlying shared mutex is acquired
  * and released.
  *
  * Example:
  *
  * class ProfileSharedMutex : public SharedMutexHelper<ProfileSharedMutex>
  * {
  * public:
  *     size_t getLockCount() const { return lock_count; }
  *
  *     size_t getSharedLockCount() const { return shared_lock_count; }
  *
  * private:
  *     using Base = SharedMutexHelper<ProfileSharedMutex, SharedMutex>;
  *     friend class SharedMutexHelper<ProfileSharedMutex, SharedMutex>;
  *
  *     void lockImpl()
  *     {
  *         ++lock_count;
  *         Base::lockImpl();
  *     }
  *
  *     void lockSharedImpl()
  *     {
  *         ++shared_lock_count;
  *         Base::lockSharedImpl();
  *     }
  *
  *     std::atomic<size_t> lock_count = 0;
  *     std::atomic<size_t> shared_lock_count = 0;
  * };
  */
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
        mutex.try_lock();
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
