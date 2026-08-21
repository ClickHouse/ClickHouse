#pragma once

#include <Common/Exception.h>

#include <base/defines.h>

#include <shared_mutex>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
};

/** SharedLockGuard provides RAII-style locking mechanism for acquiring shared ownership of the implementation
  * of the SharedLockable concept (for example SharedMutex or ContextSharedMutex) supplied as the
  * constructor argument. Think of it as std::lock_guard which locks shared.
  *
  * On construction it acquires shared ownership using `lock_shared` method.
  * On destruction shared ownership is released using `unlock_shared` method.
  */
template <typename Mutex>
class TSA_SCOPED_LOCKABLE SharedLockGuard
{
public:
    explicit SharedLockGuard(Mutex & mutex_) TSA_ACQUIRE_SHARED(mutex_) : shared_lock(mutex_) {}
    ~SharedLockGuard() TSA_RELEASE() = default;

    SharedLockGuard(SharedLockGuard && shared_lock_guard_) noexcept
        : shared_lock(std::move(shared_lock_guard_.shared_lock))
        , locked(shared_lock_guard_.locked)
    {
        shared_lock_guard_.locked = false;
    }

    void lock() TSA_ACQUIRE_SHARED()
    {
        if (locked)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't lock twice the same mutex");

        shared_lock.lock();
        locked = true;
    }

    void unlock() TSA_RELEASE()
    {
        if (!locked)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't unlock the mutex without locking it first");

        shared_lock.unlock();
        locked = false;
    }

private:
    std::shared_lock<Mutex> shared_lock;
    bool locked = true;
};
_LIBCPP_CTAD_SUPPORTED_FOR_TYPE(SharedLockGuard);

}
