#pragma once

#include <base/defines.h>

namespace DB
{

/** SharedLockGuard provides RAII-style locking mechanism for acquiring shared ownership of the implementation
  * of the SharedLockable concept (for example std::shared_mutex or ContextSharedMutex) supplied as the
  * constructor argument. Think of it as std::lock_guard which locks shared.
  *
  * On construction it acquires shared ownership using `lock_shared` method.
  * On destruction shared ownership is released using `unlock_shared` method.
  */
template <typename Mutex>
class TSA_SCOPED_LOCKABLE SharedLockGuard
{
public:
    explicit SharedLockGuard(Mutex & mutex_) TSA_ACQUIRE_SHARED(mutex_) : mutex(mutex_) { mutex_.lock_shared(); }

    ~SharedLockGuard() TSA_RELEASE() { mutex.unlock_shared(); }

private:
    Mutex & mutex;
};

}
