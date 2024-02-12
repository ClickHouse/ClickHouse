#pragma once

#include <tuple>
#include <sys/time.h>

/// Wrapper class for the `timespec` structure that provides easy construction
/// from `time_t` and enables comparison operations.
struct TimeSpec
{
public:
    TimeSpec() = default;

    explicit TimeSpec(const timespec & ts_)
        : ts(ts_)
    {}
    explicit TimeSpec(const time_t & time)
    {
        ts.tv_sec = time;
        ts.tv_nsec = 0;
    }

    explicit operator timespec() const { return ts; }
    timespec getTimeSpec() const { return ts; }

    auto operator<=>(const TimeSpec & rhs) const
    {
        return std::tie(ts.tv_sec, ts.tv_nsec)
           <=> std::tie(rhs.ts.tv_sec, rhs.ts.tv_nsec);
    }

private:
    ::timespec ts{};
};
