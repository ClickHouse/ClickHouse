#pragma once
#if defined(OS_LINUX) || defined(OS_DARWIN)
#include <Poco/Timespan.h>

namespace DB
{

/// Wrapper over timerfd on Linux. On macOS it is backed by a dedicated kqueue with an
/// EVFILT_TIMER registration; the kqueue descriptor itself is pollable, so `getDescriptor()`
/// can still be added to an `Epoll` and becomes readable when the timer fires.
class TimerDescriptor
{
private:
    int timer_fd;

public:
    TimerDescriptor();
    ~TimerDescriptor();

    TimerDescriptor(const TimerDescriptor &) = delete;
    TimerDescriptor & operator=(const TimerDescriptor &) = delete;
    TimerDescriptor(TimerDescriptor && other) noexcept;
    TimerDescriptor & operator=(TimerDescriptor &&) noexcept;

    int getDescriptor() const { return timer_fd; }

    void reset() const;
    void drain() const;
    void setRelative(uint64_t usec) const;
    void setRelative(Poco::Timespan timespan) const;
};

}
#endif
