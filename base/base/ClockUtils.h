#pragma once

#include <chrono>
#include <time.h>

#include <base/types.h>

namespace DB
{
template<typename Clock, typename TimeScale>
class ClockUtils
{
public:
    static inline Int64 now()
    {
        return std::chrono::duration_cast<TimeScale>(Clock::now().time_since_epoch()).count();
    }

    static inline Int64 count(std::chrono::time_point<Clock> timepoint)
    {
        return std::chrono::duration_cast<TimeScale>(timepoint.time_since_epoch()).count();
    }
};

template<typename TimeScale>
using UTCClock = ClockUtils<std::chrono::system_clock, TimeScale>;

template<typename TimeScale>
using MonotonicClock = ClockUtils<std::chrono::steady_clock, TimeScale>;

using UTCMinutes = UTCClock<std::chrono::minutes>;
using UTCSeconds = UTCClock<std::chrono::seconds>;
using UTCMilliseconds = UTCClock<std::chrono::milliseconds>;
using UTCMicroseconds = UTCClock<std::chrono::microseconds>;
using UTCNanoseconds = UTCClock<std::chrono::nanoseconds>;

using MonotonicMinutes = MonotonicClock<std::chrono::minutes>;
using MonotonicSeconds = MonotonicClock<std::chrono::seconds>;
using MonotonicMilliseconds = MonotonicClock<std::chrono::milliseconds>;
using MonotonicMicroseconds = MonotonicClock<std::chrono::microseconds>;
using MonotonicNanoseconds = MonotonicClock<std::chrono::nanoseconds>;

inline Int64 local_now_ms()
{
    timespec spec{};
    if (clock_gettime(CLOCK_REALTIME, &spec))
        return UTCMilliseconds::now();

    return spec.tv_sec * 1000 + spec.tv_nsec / 1000000;
}

}
