#include <IO/HTTPRequestThrottler.h>
#include <Common/Stopwatch.h>

namespace DB
{

void HTTPRequestThrottler::throttleImpl(const ThrottlerPtr & throttler, ProfileEvents::Event amount_event, ProfileEvents::Event sleep_event) const
{
    if (throttler)
    {
        Stopwatch sleep_watch;
        bool blocked = throttler->throttle(1); // Throttles and updates non Disk* profile events internally
        if (amount_event != ProfileEvents::end())
            ProfileEvents::increment(amount_event);
        if (blocked && sleep_event != ProfileEvents::end())
            ProfileEvents::increment(sleep_event, sleep_watch.elapsedMicroseconds());
    }
}

}
