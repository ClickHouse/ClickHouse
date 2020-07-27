#pragma once

#include <chrono>
#include <string>
#include <common/DateLUT.h>


namespace ext
{
    template <typename Clock, typename Duration = typename Clock::duration>
    std::string to_string(const std::chrono::time_point<Clock, Duration> & tp)
    {
        return DateLUT::instance().timeToString(std::chrono::system_clock::to_time_t(tp));
    }

    template <typename Rep, typename Period = std::ratio<1>>
    std::string to_string(const std::chrono::duration<Rep, Period> & dur)
    {
        auto seconds_as_int = std::chrono::duration_cast<std::chrono::seconds>(dur);
        if (seconds_as_int == dur)
            return std::to_string(seconds_as_int.count()) + "s";
        auto seconds_as_double = std::chrono::duration_cast<std::chrono::duration<double>>(dur);
        return std::to_string(seconds_as_double.count()) + "s";
    }

    template <typename Clock, typename Duration = typename Clock::duration>
    std::ostream & operator<<(std::ostream & o, const std::chrono::time_point<Clock, Duration> & tp)
    {
        return o << to_string(tp);
    }

    template <typename Rep, typename Period = std::ratio<1>>
    std::ostream & operator<<(std::ostream & o, const std::chrono::duration<Rep, Period> & dur)
    {
        return o << to_string(dur);
    }
}
