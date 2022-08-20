#if defined(OS_LINUX)

#include "Epoll.h"
#include <Common/Exception.h>
#include <unistd.h>
#include <Common/logger_useful.h>

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

void Epoll::add(int fd, void * ptr)
{
    epoll_event event;
    event.events = EPOLLIN | EPOLLPRI;
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

size_t Epoll::getManyReady(int max_events, epoll_event * events_out, bool blocking) const
{
    if (events_count == 0)
        throw Exception("There are no events in epoll", ErrorCodes::LOGICAL_ERROR);

    int ready_size;
    int timeout = blocking ? -1 : 0;
    do
    {
        ready_size = epoll_wait(epoll_fd, events_out, max_events, timeout);

        if (ready_size == -1 && errno != EINTR)
            throwFromErrno("Error in epoll_wait", DB::ErrorCodes::EPOLL_ERROR);

        if (errno == EINTR)
            LOG_TEST(&Poco::Logger::get("Epoll"), "EINTR");
    }
    while (ready_size <= 0 && (ready_size != 0 || blocking));

    return ready_size;
}

Epoll::~Epoll()
{
    if (epoll_fd != -1)
        close(epoll_fd);
}

}
#endif
