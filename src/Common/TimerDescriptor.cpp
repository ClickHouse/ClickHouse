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
#include <Common/setThreadName.h>
#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <fcntl.h>
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_CREATE_TIMER;
    extern const int CANNOT_SET_TIMER_PERIOD;
    extern const int CANNOT_READ_FROM_SOCKET;
}

#if defined(OS_DARWIN)
namespace
{
    /// Defined together with the macOS timer implementation below.
    void disarmTimer(int fd);
}
#endif

/// Methods that do not depend on the underlying timer mechanism are shared between platforms.

TimerDescriptor::TimerDescriptor(TimerDescriptor && other) noexcept
    : timer_fd(other.timer_fd)
{
    other.timer_fd = -1;
#if defined(OS_DARWIN)
    std::swap(wakeup_fd, other.wakeup_fd);
#endif
}

TimerDescriptor & TimerDescriptor::operator=(DB::TimerDescriptor && other) noexcept
{
    std::swap(timer_fd, other.timer_fd);
#if defined(OS_DARWIN)
    std::swap(wakeup_fd, other.wakeup_fd);
#endif
    return *this;
}

TimerDescriptor::~TimerDescriptor()
{
#if defined(OS_DARWIN)
    if (wakeup_fd != -1)
    {
        /// Returns only once no expiration can still be written, so both ends are safe to close.
        disarmTimer(wakeup_fd);
        if (0 != ::close(wakeup_fd))
            std::terminate();
    }
#endif

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
    Epoll epoll{EpollNesting::Leaf};
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

/// macOS has no timerfd. Back the timer with a pipe: the read end is the pollable descriptor and a
/// single shared thread writes one byte to the write end when the timer expires. See the note in
/// TimerDescriptor.h for why the descriptor must not be a kqueue.
///
/// libdispatch would be less code, but its worker threads are kernel-managed and cannot be
/// signalled: `pthread_kill` returns ENOTSUP for them, so every `system.stack_trace` query fails
/// with CANNOT_SIGQUEUE. One ordinary thread for the whole process avoids that.

namespace
{
    using TimerClock = std::chrono::steady_clock;

    class TimerThread
    {
    public:
        static TimerThread & instance()
        {
            /// Intentionally leaked: the thread runs for the lifetime of the process, so there is no
            /// destruction order to get wrong at exit.
            static TimerThread * timer_thread = new TimerThread;
            return *timer_thread;
        }

        void arm(int fd, uint64_t usec)
        {
            std::lock_guard lock(mutex);
            deadlines[fd] = TimerClock::now() + std::chrono::microseconds(usec);
            wakeup.notify_all();
        }

        /// Returns only once no expiration for `fd` can still be written: the timer thread writes
        /// while holding the same mutex, so afterwards the descriptor cannot become readable on its own.
        void disarm(int fd)
        {
            std::lock_guard lock(mutex);
            deadlines.erase(fd);
        }

    private:
        TimerThread()
        {
            std::thread(&TimerThread::run, this).detach();
        }

        /// The lock is held across the whole loop and handed to the condition variable, which the
        /// analysis cannot follow around the back edge. `arm` and `disarm` stay checked.
        void run() TSA_NO_THREAD_SAFETY_ANALYSIS
        {
            setThreadName(ThreadName::TIMER_DESCRIPTOR);

            std::unique_lock<std::mutex> lock(mutex);
            while (true)
            {
                if (deadlines.empty())
                {
                    wakeup.wait(lock);
                }
                else
                {
                    auto earliest = std::min_element(
                        deadlines.begin(),
                        deadlines.end(),
                        [](const auto & lhs, const auto & rhs) { return lhs.second < rhs.second; })->second;
                    wakeup.wait_until(lock, earliest);
                }

                auto now = TimerClock::now();
                for (auto it = deadlines.begin(); it != deadlines.end();)
                {
                    if (it->second > now)
                    {
                        ++it;
                        continue;
                    }

                    char byte = 1;
                    ssize_t written = 0;
                    /// A signal (the query profiler, `system.stack_trace`) can interrupt the write
                    /// before anything is written. Dropping the expiration then would leave the timer
                    /// silent forever, so retry instead of losing it.
                    do
                    {
                        written = ::write(it->first, &byte, sizeof(byte));
                    } while (written < 0 && errno == EINTR);

                    /// The descriptor is non-blocking. A full pipe means an earlier expiration has not
                    /// been drained, so the timer already reads as expired and losing this byte changes
                    /// nothing.
                    it = deadlines.erase(it);
                }
            }
        }

        std::mutex mutex;
        std::condition_variable wakeup;
        std::unordered_map<int, TimerClock::time_point> deadlines TSA_GUARDED_BY(mutex);
    };

    void disarmTimer(int fd)
    {
        TimerThread::instance().disarm(fd);
    }
}

TimerDescriptor::TimerDescriptor()
{
    int fds[2];
    if (-1 == ::pipe(fds))
        throw ErrnoException(ErrorCodes::CANNOT_CREATE_TIMER, "Cannot create pipe for timer");

    timer_fd = fds[0];
    wakeup_fd = fds[1];

    for (int fd : fds)
    {
        int flags = ::fcntl(fd, F_GETFL, 0);
        if (-1 == flags || -1 == ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) || -1 == ::fcntl(fd, F_SETFD, FD_CLOEXEC))
        {
            /// Construction failed, so the destructor will not run and both ends have to be closed here.
            /// Nothing can be done about a failing close on this path, but errno has to survive it.
            int fcntl_errno = errno;
            [[maybe_unused]] int read_end_closed = ::close(timer_fd);
            [[maybe_unused]] int write_end_closed = ::close(wakeup_fd);
            timer_fd = -1;
            wakeup_fd = -1;
            ErrnoException::throwWithErrno(ErrorCodes::CANNOT_CREATE_TIMER, fcntl_errno, "Cannot configure timer pipe");
        }
    }
}

void TimerDescriptor::reset() const
{
    if (timer_fd == -1)
        return;

    disarmTimer(wakeup_fd);
    drain();
}

void TimerDescriptor::drain() const
{
    if (timer_fd == -1)
        return;

    char buf[16];
    while (true)
    {
        ssize_t res = ::read(timer_fd, buf, sizeof(buf));

        if (res > 0)
            continue;

        /// Nothing left to read.
        if (res == 0 || errno == EAGAIN)
            break;

        /// A signal happened, need to retry.
        if (errno == EINTR)
            continue;

        throw ErrnoException(ErrorCodes::CANNOT_READ_FROM_SOCKET, "Cannot drain timer pipe {}", timer_fd);
    }
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

    TimerThread::instance().arm(wakeup_fd, usec);
}

#endif

}

#endif
