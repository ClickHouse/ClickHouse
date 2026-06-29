#pragma once

#include <base/defines.h>

#ifdef OS_LINUX /// Because of futex

#include <base/types.h>

#include <atomic>

namespace DB
{

// Faster implementation of STD shared_mutex based on a pair of futexes
// See https://github.com/ClickHouse/ClickHouse/issues/87060 for a comparison with absl::Mutex
// Or run `./src/unit_tests_dbms --gtest_filter=*SharedMutex*`
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

    std::atomic<UInt64> state;
    std::atomic<UInt32> waiters;
};

}

#else

#include <absl/synchronization/mutex.h>

namespace DB
{

class TSA_CAPABILITY("SharedMutex") SharedMutex final : absl::Mutex
{
    using absl::Mutex::Mutex;

public:
    SharedMutex(const SharedMutex &) = delete;
    SharedMutex & operator=(const SharedMutex &) = delete;
    SharedMutex(SharedMutex &&) = delete;
    SharedMutex & operator=(SharedMutex &&) = delete;

    // Exclusive ownership
    void lock() TSA_ACQUIRE() { absl::Mutex::lock(); }

    bool try_lock() TSA_TRY_ACQUIRE(true) { return absl::Mutex::try_lock(); }

    void unlock() TSA_RELEASE() { absl::Mutex::unlock(); }

    // Shared ownership
    void lock_shared() TSA_ACQUIRE_SHARED() { absl::Mutex::lock_shared(); }

    bool try_lock_shared() TSA_TRY_ACQUIRE_SHARED(true) { return absl::Mutex::try_lock_shared(); }

    void unlock_shared() TSA_RELEASE_SHARED() { absl::Mutex::unlock_shared(); }
};
}

#endif
