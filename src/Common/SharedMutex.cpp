#include <Common/SharedMutex.h>

#ifdef OS_LINUX /// Because of futex

#include <bit>

#include <Common/futex.h>

namespace DB
{

SharedMutex::SharedMutex()
    : state(0)
    , waiters(0)
{}

void SharedMutex::lock()
{
    UInt64 value = state.load();
    while (true)
    {
        if (value & writers)
        {
            waiters++;
            futexWaitUpperFetch(state, value);
            waiters--;
        }
        else if (state.compare_exchange_strong(value, value | writers))
            break;
    }

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
    UInt64 value = state.load();
    while (true)
    {
        if (value & writers)
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
