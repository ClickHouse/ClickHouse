#pragma once

#include <Common/OvercommitTracker.h>
#include <base/defines.h>

namespace DB
{

/** LockGuard provides RAII-style locking mechanism for a mutex.
 ** It's intended to be used like std::unique_ptr but with TSA annotations
  */
template <typename Mutex>
class TSA_SCOPED_LOCKABLE LockGuard
{
public:
    explicit LockGuard(Mutex & mutex_) TSA_ACQUIRE(mutex_) : mutex(mutex_) { mutex.lock(); }
    ~LockGuard() TSA_RELEASE() { mutex.unlock(); }

private:
    Mutex & mutex;
};

template <template<typename> typename TLockGuard, typename Mutex>
class TSA_SCOPED_LOCKABLE LockAndOverCommitTrackerBlocker
{
public:
    explicit LockAndOverCommitTrackerBlocker(Mutex & mutex_) TSA_ACQUIRE(mutex_) : lock(TLockGuard(mutex_)) {}
    ~LockAndOverCommitTrackerBlocker() TSA_RELEASE() = default;

    TLockGuard<Mutex> & getUnderlyingLock() { return lock; }

private:
    TLockGuard<Mutex> lock;
    OvercommitTrackerBlockerInThread blocker = {};
};

}
