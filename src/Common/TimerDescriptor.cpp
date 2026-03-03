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
    extern const int CANNOT_READ_FROM_SOCKET;
}

TimerDescriptor::TimerDescriptor()
{
    timer_fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    if (timer_fd == -1)
        throw ErrnoException(ErrorCodes::CANNOT_CREATE_TIMER, "Cannot create timer_fd descriptor");
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

void TimerDescriptor::reset() const
{
    if (timer_fd == -1)
        return;

    itimerspec spec{};

    if (-1 == timerfd_settime(timer_fd, 0 /*relative timer */, &spec, nullptr))
        throw ErrnoException(ErrorCodes::CANNOT_SET_TIMER_PERIOD, "Cannot reset timer_fd");

    /// Drain socket.
    /// It may be possible that alarm happened and socket is readable.
    drain();
}

void TimerDescriptor::drain() const
{
    if (timer_fd == -1)
        return;

    /// It is expected that socket returns 8 bytes when readable.
    /// Read in loop anyway cause signal may interrupt read call.

    /// man timerfd_create:
    /// If the timer has already expired one or more times since its settings were last modified using timerfd_settime(),
    /// or since the last successful read(2), then the buffer given to read(2) returns an unsigned 8-byte integer (uint64_t)
    /// containing the number of expirations that have occurred.
    /// (The returned value is in host byte order—that is, the native byte order for integers on the host machine.)

    /// Due to a bug in Linux Kernel, reading from timerfd in non-blocking mode can be still blocking.
    /// Avoid it with polling.
    Epoll epoll;
    epoll.add(timer_fd);
    epoll_event event;
    event.data.fd = -1;
    size_t ready_count = epoll.getManyReady(1, &event, 0);
    if (!ready_count)
        return;

    uint64_t buf;
    while (true)
    {
        ssize_t res = ::read(timer_fd, &buf, sizeof(buf));

        if (res < 0)
        {
            /// man timerfd_create:
            /// If no timer expirations have occurred at the time of the read(2),
            /// then the call either blocks until the next timer expiration, or fails with the error EAGAIN
            /// if the file descriptor has been made nonblocking
            /// (via the use of the fcntl(2) F_SETFL operation to set the O_NONBLOCK flag).
            if (errno == EAGAIN)
                break;

            /// A signal happened, need to retry.
            if (errno == EINTR)
            {
                /** This is to help with debugging.
                  *
                  * Sometimes reading from timer_fd blocks, which should not happen, because we opened it in a non-blocking mode.
                  * But it could be possible if a rogue 3rd-party library closed our file descriptor by mistake
                  * (for example by double closing due to the lack of exception safety or if it is a crappy code in plain C)
                  * and then another file descriptor is opened in its place.
                  *
                  * Let's try to get a name of this file descriptor and log it.
                  */
                LoggerPtr log = getLogger("TimerDescriptor");

                static constexpr ssize_t max_link_path_length = 256;
                char link_path[max_link_path_length];
                ssize_t link_path_length = readlink(fmt::format("/proc/self/fd/{}", timer_fd).c_str(), link_path, max_link_path_length);
                if (-1 == link_path_length)
                    throw ErrnoException(ErrorCodes::CANNOT_READ_FROM_SOCKET, "Cannot readlink for a timer_fd {}", timer_fd);

                LOG_TRACE(log, "Received EINTR while trying to drain a TimerDescriptor, fd {}: {}", timer_fd, std::string_view(link_path, link_path_length));

                /// Check that it's actually a timerfd.
                chassert(std::string_view(link_path, link_path_length).contains("timerfd"));
                continue;
            }

            throw ErrnoException(ErrorCodes::CANNOT_READ_FROM_SOCKET, "Cannot drain timer_fd {}", timer_fd);
        }

        chassert(res == sizeof(buf));
    }
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
