#if defined(OS_LINUX)

#include "Epoll.h"
#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <base/defines.h>
#include <unistd.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int EPOLL_ERROR;
    extern const int LOGICAL_ERROR;
}

Epoll::Epoll() : events_count(0)
{
    epoll_fd = epoll_create1(0);
    if (epoll_fd == -1)
        throwFromErrno("Cannot open epoll descriptor", DB::ErrorCodes::EPOLL_ERROR);
}

Epoll::Epoll(Epoll && other) noexcept : epoll_fd(other.epoll_fd), events_count(other.events_count.load())
{
    other.epoll_fd = -1;
}

Epoll & Epoll::operator=(Epoll && other) noexcept
{
    epoll_fd = other.epoll_fd;
    other.epoll_fd = -1;
    events_count.store(other.events_count.load());
    return *this;
}

void Epoll::add(int fd, void * ptr, uint32_t events)
{
    epoll_event event;
    event.events = events | EPOLLPRI;
    if (ptr)
        event.data.ptr = ptr;
    else
        event.data.fd = fd;

    ++events_count;

    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &event) == -1)
        throwFromErrno("Cannot add new descriptor to epoll", DB::ErrorCodes::EPOLL_ERROR);
}

void Epoll::remove(int fd)
{
    --events_count;

    if (epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr) == -1)
        throwFromErrno("Cannot remove descriptor from epoll", DB::ErrorCodes::EPOLL_ERROR);
}

size_t Epoll::getManyReady(int max_events, epoll_event * events_out, int timeout) const
{
    if (events_count == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There are no events in epoll");

    Stopwatch watch;
    int ready_size;
    while (true)
    {
        ready_size = epoll_wait(epoll_fd, events_out, max_events, timeout);

        /// If `ready_size` = 0, it's timeout.
        if (ready_size < 0)
        {
            if (errno == EINTR)
            {
                if (timeout >= 0)
                {
                    timeout = std::max(0, static_cast<int>(timeout - watch.elapsedMilliseconds()));
                    watch.restart();
                }
                continue;
            }
            else
                throwFromErrno("Error in epoll_wait", DB::ErrorCodes::EPOLL_ERROR);
        }
        else
            break;
    }

    return ready_size;
}

Epoll::~Epoll()
{
    if (epoll_fd != -1)
    {
        int err = close(epoll_fd);
        chassert(!err || errno == EINTR);
    }
}

}
#endif
