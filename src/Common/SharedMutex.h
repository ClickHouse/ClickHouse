#pragma once

#ifdef OS_LINUX /// Because of futex

#include <base/types.h>
#include <atomic>
#include <shared_mutex> // for std::unique_lock and std::shared_lock

namespace DB
{

// Faster implementation of `std::shared_mutex` based on a pair of futexes
class SharedMutex
{
public:
    SharedMutex();
    ~SharedMutex() = default;
    SharedMutex(const SharedMutex &) = delete;
    SharedMutex & operator=(const SharedMutex &) = delete;

    // Exclusive ownership
    void lock();
    bool try_lock();
    void unlock();

    // Shared ownership
    void lock_shared();
    bool try_lock_shared();
    void unlock_shared();

private:
    static constexpr UInt64 readers = (1ull << 32ull) - 1ull; // Lower 32 bits of state
    static constexpr UInt64 writers = ~readers; // Upper 32 bits of state

    alignas(64) std::atomic<UInt64> state;
    std::atomic<UInt32> waiters;
};

}

#else

using SharedMutex = std::shared_mutex;

}

#endif
