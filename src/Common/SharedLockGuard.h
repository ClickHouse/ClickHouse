#pragma once

#include <base/types.h>
#include <base/defines.h>

namespace DB
{

template <typename Mutex>
class __attribute__((scoped_lockable)) SharedLockGuard
{
public:
    explicit SharedLockGuard(Mutex & mutex_) TSA_ACQUIRE_SHARED(mutex_) : mutex(mutex_)
    {
        mutex_.lock();
    }

    ~SharedLockGuard() TSA_RELEASE()
    {
        mutex.unlock();
    }

private:
    Mutex & mutex;
};

}


