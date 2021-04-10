#pragma once
#if defined(OS_LINUX)
#include <Poco/Timespan.h>

namespace DB
{

/// Wrapper over timerfd.
class TimerDescriptor
{
private:
    int timer_fd;

public:
    explicit TimerDescriptor(int clockid, int flags);
    ~TimerDescriptor();

    TimerDescriptor(const TimerDescriptor &) = delete;
    TimerDescriptor & operator=(const TimerDescriptor &) = delete;
    TimerDescriptor(TimerDescriptor &&) = default;
    TimerDescriptor & operator=(TimerDescriptor &&) = default;

    int getDescriptor() const { return timer_fd; }

    void reset() const;
    void drain() const;
    void setRelative(const Poco::Timespan & timespan) const;
};

}
#endif
