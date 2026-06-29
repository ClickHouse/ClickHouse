#pragma once

#include <base/types.h>
#include <Common/ProfileEvents.h>
#include <Common/Throttler.h>


namespace DB
{

/// Helper struct encapsulating HTTP request throttling and profile event update logic.
struct HTTPRequestThrottler
{
    /// Throttles for different kinds of HTTP requests
    /// NOTE: Throttlers contain non Disk* profile events and update them internally.
    ThrottlerPtr get_throttler;
    ThrottlerPtr put_throttler;

    ProfileEvents::Event get_blocked{ProfileEvents::end()}; /// GET throttler blocks the request
    ProfileEvents::Event put_blocked{ProfileEvents::end()}; /// PUT throttler blocks the request

    ProfileEvents::Event disk_get_amount{ProfileEvents::end()}; /// GET throttler is used for disk (regardless of whether it sleeps or not)
    ProfileEvents::Event disk_get_blocked{ProfileEvents::end()}; /// GET throttler blocks the request for disk
    ProfileEvents::Event disk_get_sleep_us{ProfileEvents::end()}; /// GET throttler sleeps for disk

    ProfileEvents::Event disk_put_amount{ProfileEvents::end()}; /// PUT throttler is used for disk (regardless of whether it sleeps or not)
    ProfileEvents::Event disk_put_blocked{ProfileEvents::end()}; /// PUT throttler blocks the request for disk
    ProfileEvents::Event disk_put_sleep_us{ProfileEvents::end()}; /// PUT throttler sleeps for disk

    void throttleHTTPGet() const
    {
        throttleImpl(get_throttler, get_blocked, disk_get_amount, disk_get_blocked, disk_get_sleep_us);
    }

    void throttleHTTPPut() const
    {
        throttleImpl(put_throttler, put_blocked, disk_put_amount, disk_put_blocked, disk_put_sleep_us);
    }

private:
    void throttleImpl(
        const ThrottlerPtr & throttler,
        ProfileEvents::Event blocked_event,
        ProfileEvents::Event disk_amount_event,
        ProfileEvents::Event disk_blocked_event,
        ProfileEvents::Event disk_sleep_event) const;
};

}
