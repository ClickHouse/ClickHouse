#pragma once

#include <chrono>
#include <ctime>
#include <string>
#include <iomanip>


namespace ext
{
    inline std::string to_string(const std::time_t & time)
    {
        std::stringstream ss;
        ss << std::put_time(std::localtime(&time), "%Y-%m-%d %X");
        return ss.str();
    }

    template <typename Clock, typename Duration = typename Clock::duration>
    std::string to_string(const std::chrono::time_point<Clock, Duration> & tp)
    {
        // Don't use DateLUT because it shows weird characters for
        // TimePoint::max(). I wish we could use C++20 format, but it's not
        // there yet.
        // return DateLUT::instance().timeToString(std::chrono::system_clock::to_time_t(tp));

        auto in_time_t = std::chrono::system_clock::to_time_t(tp);
        return to_string(in_time_t);
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
