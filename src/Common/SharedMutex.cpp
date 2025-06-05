#include <Common/SharedMutex.h>
#include <base/getThreadId.h>

#ifdef OS_LINUX /// Because of futex

#include <bit>

#include <Common/futex.h>

namespace DB
{

SharedMutex::SharedMutex()
    : state(0)
    , waiters(0)
    , writer_thread_id(0)
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

    /// The first step of acquiring the exclusive ownership is finished.
    /// Now we just wait until all readers release the shared ownership.
    writer_thread_id.store(getThreadId());

    value |= writers;
    while (value & readers)
        futexWaitLowerFetch(state, value);
}

bool SharedMutex::try_lock()
{
    UInt64 value = 0;
    bool success = state.compare_exchange_strong(value, writers);
    if (success)
        writer_thread_id.store(getThreadId());
    return success;
}

void SharedMutex::unlock()
{
    writer_thread_id.store(0);
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
