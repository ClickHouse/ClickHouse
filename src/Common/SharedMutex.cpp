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
    if (likely(state.compare_exchange_strong(value, writers)))
        return;

    /// Drain writers and set up self as next writer
    value = state.load();
    while (true)
    {
        if (unlikely(value & writers))
        {
            waiters++;
            futexWaitUpperFetch(state, value);
            waiters--;
        }
        else if (state.compare_exchange_strong(value, value | writers))
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
    return state.compare_exchange_strong(value, writers);
}

void SharedMutex::unlock()
{
    state.store(0);
    if (waiters)
        futexWakeUpperAll(state);
}

void SharedMutex::lock_shared()
{
    /// Take a reader slot optimistically. Unlike a compare-exchange this cannot
    /// fail, so concurrent readers never retry against each other; the hardware
    /// queues the increments on the line instead. With many readers the retries
    /// were themselves the contention, so the read path used to get worse as
    /// readers were added rather than merely staying flat.
    UInt64 value = state.fetch_add(1);
    if (likely(!(value & writers)))
        return;

    /// A writer holds the lock or is waiting for readers to drain, so withdraw.
    /// Withdrawing can make this the last reader the writer was waiting for, so
    /// it has to be woken exactly as unlock_shared() would.
    value = state.fetch_sub(1) - 1;
    if (value == writers)
        futexWakeLowerOne(state);

    value = state.load();
    while (true)
    {
        if (unlikely(value & writers))
        {
            waiters++;
            futexWaitUpperFetch(state, value);
            waiters--;
        }
        else if (state.compare_exchange_strong(value, value + 1))
            break;
    }
}

bool SharedMutex::try_lock_shared()
{
    UInt64 value = state.load();
    while (true)
    {
        if (value & writers)
            return false;
        if (state.compare_exchange_strong(value, value + 1))
            break;
        // Concurrent try_lock_shared() should not fail, so we have to retry CAS, but avoid blocking wait
    }
    return true;
}

void SharedMutex::unlock_shared()
{
    UInt64 value = state.fetch_sub(1) - 1;
    if (value == writers)
        futexWakeLowerOne(state); // Wake writer
}

}

#endif
