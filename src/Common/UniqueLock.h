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

/** UniqueLock provides RAII-style locking mechanism for a mutex.
 ** It's intended to be used like std::unique_lock but with TSA annotations
  */
template <typename Mutex>
class TSA_SCOPED_LOCKABLE UniqueLock
{
public:
    explicit UniqueLock(Mutex & mutex_) TSA_ACQUIRE(mutex_) : unique_lock(mutex_) { }
    ~UniqueLock() TSA_RELEASE() = default;

    void lock() TSA_ACQUIRE()
    {
        /// Don't allow recursive_mutex for now.
        if (locked)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't lock twice the same mutex");
        unique_lock.lock();
        locked = true;
    }

    void unlock() TSA_RELEASE()
    {
        if (!locked)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't unlock the mutex without locking it first");
        unique_lock.unlock();
        locked = false;
    }

    std::unique_lock<Mutex> & getUnderlyingLock() { return unique_lock; }

private:
    std::unique_lock<Mutex> unique_lock;
    bool locked = true;
};
_LIBCPP_CTAD_SUPPORTED_FOR_TYPE(UniqueLock);

template <template<typename> typename TUniqueLock, typename Mutex>
class TSA_SCOPED_LOCKABLE LockAndOverCommitTrackerBlocker
{
public:
    explicit LockAndOverCommitTrackerBlocker(Mutex & mutex_) TSA_ACQUIRE(mutex_) : lock(TUniqueLock(mutex_)) {}
    ~LockAndOverCommitTrackerBlocker() TSA_RELEASE() = default;

    TUniqueLock<Mutex> & getUnderlyingLock() { return lock; }

private:
    TUniqueLock<Mutex> lock;
    OvercommitTrackerBlockerInThread blocker = {};
};

}
