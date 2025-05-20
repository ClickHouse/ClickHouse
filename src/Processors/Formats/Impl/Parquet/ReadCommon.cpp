#include <Processors/Formats/Impl/Parquet/ReadCommon.h>

namespace DB::Parquet
{

#ifdef OS_LINUX

bool CompletionNotification::check() const
{
    return val.load(std::memory_order_acquire) == NOTIFIED;
}

void CompletionNotification::wait()
{
    UInt32 n = val.load(std::memory_order_acquire);
    if (n == NOTIFIED)
        return; // fast path
    if (n == EMPTY)
    {
        if (!val.compare_exchange_strong(n, WAITING))
        {
            if (n == NOTIFIED)
                return;
            chassert(n == WAITING);
        }
    }
    while (true)
    {
        futexWait(&val, WAITING);
        n = val.load();
        if (n == NOTIFIED)
            return;
        chassert(n == WAITING);
    }
}

void CompletionNotification::notify()
{
    UInt32 n = val.exchange(NOTIFIED);
    /// If there were no wait() calls before the notify() call, avoid the syscall.
    if (n == WAITING)
        futexWake(&val, INT32_MAX);
}

#else

bool CompletionNotification::check() const
{
    return notified.load();
}

void CompletionNotification::wait()
{
    if (!check())
        future.wait();
}

void CompletionNotification::notify()
{
    if (!notified.exchange(true))
        promise.set_value();
}

#endif

}
