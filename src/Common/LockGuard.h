#pragma once

#include <base/defines.h>
#include <Common/Exception.h>
#include <Common/OvercommitTracker.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
};

/** LockGuard provides RAII-style locking mechanism for a mutex.
 ** It's intended to be used like std::unique_lock but with TSA annotations
  */
template <typename Mutex>
class TSA_SCOPED_LOCKABLE LockGuard
{
public:
    explicit LockGuard(Mutex & mutex_) TSA_ACQUIRE(mutex_) : mutex(mutex_) { lock(); }
    ~LockGuard() TSA_RELEASE() { if (locked) unlock(); }

    void lock() TSA_ACQUIRE()
    {
        /// Don't allow recursive_mutex for now.
        if (locked)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't lock twice the same mutex");
        mutex.lock();
        locked = true;
    }

    void unlock() TSA_RELEASE()
    {
        if (!locked)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't unlock the mutex without locking it first");
        mutex.unlock();
        locked = false;
    }

private:
    Mutex & mutex;
    bool locked = false;
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
