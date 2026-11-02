#if defined(OS_LINUX) || defined(OS_DARWIN)

#include <Common/TimerDescriptor.h>
#include <Common/Exception.h>
#include <Common/ErrnoException.h>

#include <cerrno>
#include <utility>
#include <unistd.h>

#if defined(OS_LINUX)
#include <Common/Epoll.h>
#include <Common/logger_useful.h>
#include <sys/timerfd.h>
#include <fmt/format.h>
#else
#include <sys/event.h>
#include <sys/time.h>
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_CREATE_TIMER;
    extern const int CANNOT_SET_TIMER_PERIOD;
    extern const int CANNOT_READ_FROM_SOCKET;
}

/// Methods that do not depend on the underlying timer mechanism are shared between platforms.

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

void TimerDescriptor::setRelative(Poco::Timespan timespan) const
{
    setRelative(timespan.totalMicroseconds());
}

#if defined(OS_LINUX)

TimerDescriptor::TimerDescriptor()
{
    timer_fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    if (timer_fd == -1)
        throw ErrnoException(ErrorCodes::CANNOT_CREATE_TIMER, "Cannot create timer_fd descriptor");
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
    epoll_event event{};
    event.data.fd = -1;
    size_t ready_count = epoll.getManyReady(1, &event, 0);
    if (!ready_count)
        return;

    uint64_t buf = 0;
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

    itimerspec spec{};
    spec.it_interval.tv_nsec = 0;
    spec.it_interval.tv_sec = 0;
    spec.it_value.tv_sec = usec / TIMER_PRECISION;
    spec.it_value.tv_nsec = (usec % TIMER_PRECISION) * 1'000;

    if (-1 == timerfd_settime(timer_fd, 0 /*relative timer */, &spec, nullptr))
        throw ErrnoException(ErrorCodes::CANNOT_SET_TIMER_PERIOD, "Cannot set time for timer_fd");
}

#elif defined(OS_DARWIN)

/// macOS has no timerfd. Back the timer with a dedicated kqueue carrying a single EVFILT_TIMER:
/// the kqueue descriptor is itself pollable, so getDescriptor() can be added to an `Epoll` and
/// becomes readable when the timer fires.

namespace
{
    /// Fixed identifier for the single EVFILT_TIMER registered on the timer's own kqueue.
    constexpr uintptr_t TIMER_IDENT = 1;
}

TimerDescriptor::TimerDescriptor()
{
    timer_fd = kqueue();
    if (timer_fd == -1)
        throw ErrnoException(ErrorCodes::CANNOT_CREATE_TIMER, "Cannot create kqueue for timer");
}

void TimerDescriptor::reset() const
{
    if (timer_fd == -1)
        return;

    struct kevent change{};
    EV_SET(&change, TIMER_IDENT, EVFILT_TIMER, EV_DELETE, 0, 0, nullptr);
    /// ENOENT means the timer was not armed; that's fine.
    if (kevent(timer_fd, &change, 1, nullptr, 0, nullptr) == -1 && errno != ENOENT)
        throw ErrnoException(ErrorCodes::CANNOT_SET_TIMER_PERIOD, "Cannot reset kqueue timer");

    drain();
}

void TimerDescriptor::drain() const
{
    if (timer_fd == -1)
        return;

    /// Consume any pending timer expirations so the descriptor stops being readable.
    struct timespec zero{};
    struct kevent event{};
    while (kevent(timer_fd, nullptr, 0, &event, 1, &zero) > 0)
        ;
}

void TimerDescriptor::setRelative(uint64_t usec) const
{
    chassert(timer_fd >= 0);

    /// A zero timeout means "disarm" for the timerfd-based implementation; mirror that here.
    if (usec == 0)
    {
        reset();
        return;
    }

    struct kevent change{};
    /// EV_ONESHOT mirrors the single (non-periodic) expiration the timerfd path arms.
    EV_SET(&change, TIMER_IDENT, EVFILT_TIMER, EV_ADD | EV_ONESHOT, NOTE_USECONDS, static_cast<int64_t>(usec), nullptr);

    if (kevent(timer_fd, &change, 1, nullptr, 0, nullptr) == -1)
        throw ErrnoException(ErrorCodes::CANNOT_SET_TIMER_PERIOD, "Cannot set kqueue timer");
}

#endif

}

#endif
