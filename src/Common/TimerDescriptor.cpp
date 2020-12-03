#include <Common/TimerDescriptor.h>
#include <Common/Exception.h>

#include <sys/timerfd.h>
#include <sys/ioctl.h>
#include <unistd.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_CREATE_TIMER;
    extern const int CANNOT_SET_TIMER_PERIOD;
    extern const int CANNOT_FCNTL;
    extern const int CANNOT_READ_FROM_SOCKET;
}

TimerDescriptor::TimerDescriptor(int clockid, int flags)
{
    timer_fd = timerfd_create(clockid, flags);
    if (timer_fd == -1)
        throw Exception(ErrorCodes::CANNOT_CREATE_TIMER, "Cannot create timer_fd descriptor");
}

TimerDescriptor::~TimerDescriptor()
{
    /// Do not check for result cause cannot throw exception.
    close(timer_fd);
}

void TimerDescriptor::reset() const
{
    itimerspec spec;
    spec.it_interval.tv_nsec = 0;
    spec.it_interval.tv_sec = 0;
    spec.it_value.tv_sec = 0;
    spec.it_value.tv_nsec = 0;

    if (-1 == timerfd_settime(timer_fd, 0 /*relative timer */, &spec, nullptr))
        throwFromErrno("Cannot reset timer_fd", ErrorCodes::CANNOT_SET_TIMER_PERIOD);

    /// Drain socket.
    /// It may be possible that alarm happened and socket is readable.
    drain();
}

void TimerDescriptor::drain() const
{
    int num_bytes_to_read = 0;
    if (-1 == ioctl(timer_fd, FIONREAD, &num_bytes_to_read))
        throwFromErrno("Cannot get available bytes form timer_fd", ErrorCodes::CANNOT_FCNTL);

    if (num_bytes_to_read)
    {
        /// It is expected that socket returns 8 bytes when readable.
        /// Read in loop anyway cause signal may interrupt read call.
        uint64_t buf;
        while (num_bytes_to_read)
        {
            ssize_t res = ::read(timer_fd, &buf, sizeof(buf));
            if (res < 0 && errno != EINTR)
                throwFromErrno("Cannot drain timer_fd", ErrorCodes::CANNOT_READ_FROM_SOCKET);
            else
                num_bytes_to_read -= res;
        }
    }
}

void TimerDescriptor::setRelative(const Poco::Timespan & timespan) const
{
    itimerspec spec;
    spec.it_interval.tv_nsec = 0;
    spec.it_interval.tv_sec = 0;
    spec.it_value.tv_sec = timespan.totalSeconds();
    spec.it_value.tv_nsec = timespan.useconds();

    if (-1 == timerfd_settime(timer_fd, 0 /*relative timer */, &spec, nullptr))
        throwFromErrno("Cannot set time for timer_fd", ErrorCodes::CANNOT_SET_TIMER_PERIOD);
}

}
