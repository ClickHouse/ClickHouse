#pragma once

#include <shared_mutex>

#ifdef OS_LINUX /// Because of futex

#include <Common/CancelToken.h>
#include <base/types.h>
#include <base/defines.h>
#include <atomic>

namespace DB
{

// Reimplementation of `std::shared_mutex` that can interoperate with thread cancellation via `CancelToken::signal()`.
// It has cancellation point on waiting during `lock()` and `shared_lock()`.
// NOTE: It has NO cancellation points on fast code path, when locking does not require waiting.
class TSA_CAPABILITY("CancelableSharedMutex") CancelableSharedMutex
{
public:
    CancelableSharedMutex();
    ~CancelableSharedMutex() = default;
    CancelableSharedMutex(const CancelableSharedMutex &) = delete;
    CancelableSharedMutex & operator=(const CancelableSharedMutex &) = delete;

    // Exclusive ownership
    void lock() TSA_ACQUIRE();
    bool try_lock() TSA_TRY_ACQUIRE(true);
    void unlock() TSA_RELEASE();

    // Shared ownership
    void lock_shared() TSA_ACQUIRE_SHARED();
    bool try_lock_shared() TSA_TRY_ACQUIRE_SHARED(true);
    void unlock_shared() TSA_RELEASE_SHARED();

private:
    // State 64-bits layout:
    //    1b    -   31b   -    1b    -   31b
    // signaled - writers - signaled - readers
    // 63------------------------------------0
    // Two 32-bit words are used for cancelable waiting, so each has its own separate signaled bit
    static constexpr UInt64 readers = (1ull << 32ull) - 1ull - CancelToken::signaled;
    static constexpr UInt64 readers_signaled = CancelToken::signaled;
    static constexpr UInt64 writers = readers << 32ull;
    static constexpr UInt64 writers_signaled = readers_signaled << 32ull;

    alignas(64) std::atomic<UInt64> state;
    std::atomic<UInt32> waiters;
};

}

#else

// WARNING: We support cancelable synchronization primitives only on linux for now

namespace DB
{

using CancelableSharedMutex = std::shared_mutex;

}

#endif
