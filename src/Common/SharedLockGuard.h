#pragma once

#include <base/types.h>
#include <base/defines.h>

namespace DB
{

/// Same as std::lock_guard but for shared mutex
template <typename Mutex>
class TSA_SCOPED_LOCKABLE SharedLockGuard
{
public:
    explicit SharedLockGuard(Mutex & mutex_) TSA_ACQUIRE_SHARED(mutex_) : mutex(mutex_)
    {
        mutex_.lock_shared();
    }

    ~SharedLockGuard() TSA_RELEASE()
    {
        mutex.unlock_shared();
    }

private:
    Mutex & mutex;
};

}


