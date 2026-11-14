#pragma once

#include <base/defines.h>

#include <atomic>

namespace DB
{

/// A mutex of one byte, for objects that exist in large numbers. A waiter blocks in
/// `std::atomic_flag::wait` instead of spinning. Not recursive, not fair.
class TSA_CAPABILITY("ByteMutex") ByteMutex
{
public:
    void lock() TSA_ACQUIRE()
    {
        while (flag.test_and_set(std::memory_order_acquire))
            flag.wait(true, std::memory_order_relaxed);
    }

    void unlock() TSA_RELEASE()
    {
        flag.clear(std::memory_order_release);
        flag.notify_one();
    }

private:
    std::atomic_flag flag;
};

static_assert(sizeof(ByteMutex) == 1);

}
