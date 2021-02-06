#if defined(OS_LINUX)

#include "Epoll.h"
#include <Common/Exception.h>
#include <unistd.h>
#include <common/logger_useful.h>

namespace DB
{


/// TODO: add appropriate error codes
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

void Epoll::add(int fd, void * ptr)
{
    epoll_event event;
    event.events = EPOLLIN | EPOLLPRI;
    if (ptr)
        event.data.ptr = ptr;
    else
        event.data.fd = fd;

    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &event) == -1)
        throwFromErrno("Cannot add new descriptor to epoll", DB::ErrorCodes::EPOLL_ERROR);

    ++events_count;
}

void Epoll::remove(int fd)
{
    if (epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr) == -1)
        throwFromErrno("Cannot remove descriptor from epoll", DB::ErrorCodes::EPOLL_ERROR);

    --events_count;
}

epoll_event Epoll::getReady(bool blocking, AsyncCallback async_callback) const
{
    epoll_event event;
    event.data.fd = -1;
    size_t ready_events_count = getManyReady(1, &event, blocking, std::move(async_callback));
    if (ready_events_count > 1)
        throw Exception("Returned amount of events cannot be more than 1.", ErrorCodes::LOGICAL_ERROR);

    return event;
}

size_t Epoll::getManyReady(int max_events, epoll_event * events_out, bool blocking, AsyncCallback async_callback) const
{
    int ready_size = 0;
    int timeout = blocking && !async_callback ? -1 : 0;
    do
    {
        ready_size = epoll_wait(epoll_fd, events_out, max_events, timeout);

        if (ready_size == -1 && errno != EINTR)
            throwFromErrno("Error in epoll_wait", DB::ErrorCodes::EPOLL_ERROR);

        if (ready_size == 0 && blocking && async_callback)
            async_callback(epoll_fd, 0, "epoll");
    }
    while (ready_size <= 0 && (ready_size != 0 || blocking));

    return ready_size;
}

Epoll::~Epoll()
{
    close(epoll_fd);
}

}
#endif
