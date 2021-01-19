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

epoll_event Epoll::getReady(AsyncCallback async_callback) const
{
    std::vector<epoll_event> events = getManyReady(1, true, std::move(async_callback));
    if (events.empty())
        throw Exception("Vector of ready events is empty", ErrorCodes::LOGICAL_ERROR);

    return events[0];
}

std::vector<epoll_event> Epoll::getManyReady(int max_events, bool blocking, AsyncCallback async_callback) const
{
    std::vector<epoll_event> events(max_events);

    int ready_size = 0;
    int timeout = blocking && !async_callback ? -1 : 0;
    while (ready_size <= 0 && (ready_size != 0 || blocking))
    {
        ready_size = epoll_wait(epoll_fd, events.data(), max_events, timeout);

        if (ready_size == -1 && errno != EINTR)
            throwFromErrno("Error in epoll_wait", DB::ErrorCodes::EPOLL_ERROR);

        if (ready_size == 0 && blocking && async_callback)
            async_callback(epoll_fd, 0, "epoll");
    }

    events.resize(ready_size);
    return events;
}

Epoll::~Epoll()
{
    close(epoll_fd);
}

}
