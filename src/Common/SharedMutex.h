#pragma once

#include <shared_mutex>

#ifdef OS_LINUX /// Because of futex

#include <base/types.h>
#include <base/defines.h>
#include <atomic>

namespace DB
{

// Faster implementation of `std::shared_mutex` based on a pair of futexes
class TSA_CAPABILITY("SharedMutex") SharedMutex
{
public:
    SharedMutex();
    ~SharedMutex() = default;
    SharedMutex(const SharedMutex &) = delete;
    SharedMutex & operator=(const SharedMutex &) = delete;
    SharedMutex(SharedMutex &&) = delete;
    SharedMutex & operator=(SharedMutex &&) = delete;

    // Exclusive ownership
    void lock() TSA_ACQUIRE();
    bool try_lock() TSA_TRY_ACQUIRE(true);
    void unlock() TSA_RELEASE();

    // Shared ownership
    void lock_shared() TSA_ACQUIRE_SHARED();
    bool try_lock_shared() TSA_TRY_ACQUIRE_SHARED(true);
    void unlock_shared() TSA_RELEASE_SHARED();

private:
    static constexpr UInt64 readers = (1ull << 32ull) - 1ull; // Lower 32 bits of state
    static constexpr UInt64 writers = ~readers; // Upper 32 bits of state

    alignas(64) std::atomic<UInt64> state;
    std::atomic<UInt32> waiters;
    /// Is set while the lock is held (or is in the process of being acquired) in exclusive mode only to facilitate debugging
    std::atomic<UInt64> writer_thread_id;
};

}

#else

namespace DB
{

using SharedMutex = std::shared_mutex;

}

#endif
