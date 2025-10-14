#pragma once

#include <base/types.h>
#include <Common/ProfileEvents.h>
#include <Common/Throttler.h>


namespace DB
{

/// Helper struct encapsulating HTTP request throttling logic.
struct HTTPRequestThrottler
{
    /// Throttles for different kinds of HTTP requests
    /// NOTE: Throttlers contain non Disk* profile events and update them internally.
    ThrottlerPtr get_throttler;
    ThrottlerPtr put_throttler;

    /// Event to increment when get throttler is used for disk (regardless of whether it sleeps or not)
    ProfileEvents::Event disk_get_amount{ProfileEvents::end()};

    /// Event to increment when get throttler sleeps for disk
    ProfileEvents::Event disk_get_sleep_us{ProfileEvents::end()};

    /// Event to increment when put throttler is used for disk (regardless of whether it sleeps or not)
    ProfileEvents::Event disk_put_amount{ProfileEvents::end()};

    /// Event to increment when put throttler sleeps for disk
    ProfileEvents::Event disk_put_sleep_us{ProfileEvents::end()};

    void throttleHTTPGet()
    {
        throttleImpl(get_throttler, disk_get_amount, disk_get_sleep_us);
    }

    void throttleHTTPPut()
    {
        throttleImpl(put_throttler, disk_put_amount, disk_put_sleep_us);
    }

private:
    void throttleImpl(const ThrottlerPtr & throttler, ProfileEvents::Event amount_event, ProfileEvents::Event sleep_event);
};

}
