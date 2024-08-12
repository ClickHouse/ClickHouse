#if defined(OS_LINUX)

#include <Common/TimerDescriptor.h>
#include <Common/Exception.h>
#include <Common/Epoll.h>
#include <Common/logger_useful.h>

#include <sys/timerfd.h>
#include <unistd.h>
#include <fmt/format.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_CREATE_TIMER;
    extern const int CANNOT_SET_TIMER_PERIOD;
    extern const int SYSTEM_ERROR;
}

TimerDescriptor::TimerDescriptor()
{
    init();
}

TimerDescriptor::TimerDescriptor(TimerDescriptor && other) noexcept
    : timer_fd(other.timer_fd)
{
    other.timer_fd = -1;
}

TimerDescriptor & TimerDescriptor::operator=(DB::TimerDescriptor && other) noexcept
{
    std::swap(timer_fd, other.timer_fd);
    return *this;
}

TimerDescriptor::~TimerDescriptor()
{
    if (timer_fd != -1)
    {
        if (0 != ::close(timer_fd))
            std::terminate();
    }
}

void TimerDescriptor::reset()
{
    if (timer_fd == -1)
        return;

    if (0 != ::close(timer_fd))
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot close timer_fd descriptor");

    init();
}

void TimerDescriptor::init()
{
    timer_fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    if (timer_fd == -1)
        throw ErrnoException(ErrorCodes::CANNOT_CREATE_TIMER, "Cannot create timer_fd descriptor");
}

void TimerDescriptor::setRelative(uint64_t usec) const
{
    chassert(timer_fd >= 0);

    static constexpr uint32_t TIMER_PRECISION = 1e6;

    itimerspec spec;
    spec.it_interval.tv_nsec = 0;
    spec.it_interval.tv_sec = 0;
    spec.it_value.tv_sec = usec / TIMER_PRECISION;
    spec.it_value.tv_nsec = (usec % TIMER_PRECISION) * 1'000;

    if (-1 == timerfd_settime(timer_fd, 0 /*relative timer */, &spec, nullptr))
        throw ErrnoException(ErrorCodes::CANNOT_SET_TIMER_PERIOD, "Cannot set time for timer_fd");
}

void TimerDescriptor::setRelative(Poco::Timespan timespan) const
{
    setRelative(timespan.totalMicroseconds());
}

}

#endif
