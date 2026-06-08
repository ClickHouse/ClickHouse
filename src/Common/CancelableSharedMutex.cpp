#include <Common/CancelableSharedMutex.h>

#ifdef OS_LINUX /// Because of futex

#include <Common/futex.h>

namespace DB
{

namespace
{
    inline bool cancelableWaitUpperFetch(std::atomic<UInt64> & address, UInt64 & value)
    {
        bool res = CancelToken::local().wait(upperHalfAddress(&address), upperHalf(value));
        value = address.load();
        return res;
    }

    inline bool cancelableWaitLowerFetch(std::atomic<UInt64> & address, UInt64 & value)
    {
        bool res = CancelToken::local().wait(lowerHalfAddress(&address), lowerHalf(value));
        value = address.load();
        return res;
    }
}

CancelableSharedMutex::CancelableSharedMutex()
    : state(0)
    , waiters(0)
{}

void CancelableSharedMutex::lock()
{
    UInt64 value = state.load();
    while (true)
    {
        if (value & writers)
        {
            waiters++;
            if (!cancelableWaitUpperFetch(state, value))
            {
                waiters--;
                CancelToken::local().raise();
            }
            else
                waiters--;
        }
        else if (state.compare_exchange_strong(value, value | writers))
            break;
    }

    value |= writers;
    while (value & readers)
    {
        if (!cancelableWaitLowerFetch(state, value))
        {
            state.fetch_and(~writers);
            futexWakeUpperAll(state);
            CancelToken::local().raise();
        }
    }
}

bool CancelableSharedMutex::try_lock()
{
    UInt64 value = state.load();
    return (value & (readers | writers)) == 0 && state.compare_exchange_strong(value, value | writers);
}

void CancelableSharedMutex::unlock()
{
    state.fetch_and(~writers);
    if (waiters)
        futexWakeUpperAll(state);
}

void CancelableSharedMutex::lock_shared()
{
    UInt64 value = state.load();
    while (true)
    {
        if (value & writers)
        {
            waiters++;
            if (!cancelableWaitUpperFetch(state, value))
            {
                waiters--;
                CancelToken::local().raise();
            }
            else
                waiters--;
        }
        else if (state.compare_exchange_strong(value, value + 1)) // overflow is not realistic
            break;
    }
}

bool CancelableSharedMutex::try_lock_shared()
{
    UInt64 value = state.load();
    while (true)
    {
        if (value & writers)
            return false;
        if (state.compare_exchange_strong(value, value + 1)) // overflow is not realistic
            break;
        // Concurrent try_lock_shared() should not fail, so we have to retry CAS, but avoid blocking wait
    }
    return true;
}

void CancelableSharedMutex::unlock_shared()
{
    UInt64 value = state.fetch_sub(1) - 1;
    if ((value & (writers | readers)) == writers) // If writer is waiting and no more readers
        futexWakeLowerOne(state); // Wake writer
}

}

#endif
