#pragma once
#if defined(OS_LINUX)
#include <Poco/Timespan.h>

namespace DB
{

enum TimerTypes
{
    DEFAULT,
    RECEIVE_HELLO_TIMEOUT,
    RECEIVE_TABLES_STATUS_TIMEOUT,
    RECEIVE_DATA_TIMEOUT,
    RECEIVE_TIMEOUT,
};

/// Wrapper over timerfd.
class TimerDescriptor
{
private:
    int timer_fd;
    int type = TimerTypes::DEFAULT;

public:
    explicit TimerDescriptor(int clockid = CLOCK_MONOTONIC, int flags = 0);
    ~TimerDescriptor();

    TimerDescriptor(const TimerDescriptor &) = delete;
    TimerDescriptor & operator=(const TimerDescriptor &) = delete;
    TimerDescriptor(TimerDescriptor &&) = default;
    TimerDescriptor & operator=(TimerDescriptor &&) = default;

    int getDescriptor() const { return timer_fd; }
    int getType() const { return type; }

    void reset() const;
    void drain() const;
    void setRelative(const Poco::Timespan & timespan) const;
    void setType(int type_) { type = type_; }
};

using TimerDescriptorPtr = TimerDescriptor *;

}
#endif
