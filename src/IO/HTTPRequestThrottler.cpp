#include <IO/HTTPRequestThrottler.h>
#include <Common/Stopwatch.h>

namespace DB
{

void HTTPRequestThrottler::throttleImpl(
        const ThrottlerPtr & throttler,
        ProfileEvents::Event blocked_event,
        ProfileEvents::Event disk_amount_event,
        ProfileEvents::Event disk_blocked_event,
        ProfileEvents::Event disk_sleep_event) const
{
    if (throttler)
    {
        Stopwatch sleep_watch;
        bool blocked = throttler->throttle(1); // Throttles and updates non Disk* profile events internally
        if (blocked && blocked_event != ProfileEvents::end())
            ProfileEvents::increment(blocked_event); // This is both for non-disk and disk
        if (disk_amount_event != ProfileEvents::end())
            ProfileEvents::increment(disk_amount_event);
        if (blocked && disk_blocked_event != ProfileEvents::end())
            ProfileEvents::increment(disk_blocked_event); // This is only for disk
        if (blocked && disk_sleep_event != ProfileEvents::end())
            ProfileEvents::increment(disk_sleep_event, sleep_watch.elapsedMicroseconds());
    }
}

}
