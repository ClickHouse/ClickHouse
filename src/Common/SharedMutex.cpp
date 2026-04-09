#ifdef OS_LINUX /// Because of futex

#include <Common/SharedMutex.h>
#include <Common/futex.h>

namespace DB
{

SharedMutex::SharedMutex()
    : state(0)
    , waiters(0)
{}

void SharedMutex::lock()
{
    /// Try fast acquire
    UInt64 value = 0;
    if (likely(state.compare_exchange_strong(value, writers, std::memory_order_acquire, std::memory_order_relaxed)))
        return;

    /// Drain writers and set up self as next writer
    value = state.load(std::memory_order_relaxed);
    while (true)
    {
        if (unlikely(value & writers))
        {
            waiters.fetch_add(1, std::memory_order_relaxed);
            futexWaitUpperFetch(state, value);
            waiters.fetch_sub(1, std::memory_order_relaxed);
        }
        else if (state.compare_exchange_strong(value, value | writers, std::memory_order_acquire, std::memory_order_relaxed))
            break;
    }

    /// Drain readers
    value |= writers;
    while (value & readers)
        futexWaitLowerFetch(state, value);
}

bool SharedMutex::try_lock()
{
    UInt64 value = 0;
    return state.compare_exchange_strong(value, writers, std::memory_order_acquire, std::memory_order_relaxed);
}

void SharedMutex::unlock()
{
    state.store(0, std::memory_order_release);
    if (waiters.load(std::memory_order_relaxed))
        futexWakeUpperAll(state);
}

void SharedMutex::lock_shared()
{
    UInt64 value = state.load(std::memory_order_relaxed);
    while (true)
    {
        if (unlikely(value & writers))
        {
            waiters.fetch_add(1, std::memory_order_relaxed);
            futexWaitUpperFetch(state, value);
            waiters.fetch_sub(1, std::memory_order_relaxed);
        }
        else if (state.compare_exchange_strong(value, value + 1, std::memory_order_acquire, std::memory_order_relaxed))
            break;
    }
}

bool SharedMutex::try_lock_shared()
{
    UInt64 value = state.load(std::memory_order_relaxed);
    while (true)
    {
        if (value & writers)
            return false;
        if (state.compare_exchange_strong(value, value + 1, std::memory_order_acquire, std::memory_order_relaxed))
            break;
        // Concurrent try_lock_shared() should not fail, so we have to retry CAS, but avoid blocking wait
    }
    return true;
}

void SharedMutex::unlock_shared()
{
    UInt64 value = state.fetch_sub(1, std::memory_order_acq_rel) - 1;
    if (value == writers)
        futexWakeLowerOne(state); // Wake writer
}

}

#endif
