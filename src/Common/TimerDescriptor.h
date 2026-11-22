#pragma once
#if defined(OS_LINUX) || defined(OS_DARWIN)
#include <Poco/Timespan.h>

namespace DB
{

/// Wrapper over timerfd on Linux. On macOS it is backed by a pipe whose read end is the pollable
/// descriptor; a single shared timer thread writes one byte to it when the timer expires, so
/// `getDescriptor()` returns a plain descriptor on both platforms.
///
/// The macOS timer deliberately avoids the obvious EVFILT_TIMER-on-its-own-kqueue implementation:
/// that makes the timer descriptor a kqueue, so every `Epoll` watching a timer nests one kqueue
/// inside another. The kernel rejects such graphs with EINVAL once they branch and get deep enough,
/// which is exactly the shape the hedged-connections hierarchy builds.
class TimerDescriptor
{
private:
    int timer_fd;
#if defined(OS_DARWIN)
    /// Write end of the pipe, and the key the shared timer thread uses for this timer.
    int wakeup_fd = -1;
#endif

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
