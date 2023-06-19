#pragma once
#if defined(OS_LINUX)

#include <sys/epoll.h>
#include <vector>
#include <functional>
#include <boost/noncopyable.hpp>
#include <Poco/Logger.h>

namespace DB
{

class Epoll
{
public:
    Epoll();

    Epoll(const Epoll &) = delete;
    Epoll & operator=(const Epoll &) = delete;

    Epoll & operator=(Epoll && other) noexcept;
    Epoll(Epoll && other) noexcept;

    /// Add new file descriptor to epoll. If ptr set to nullptr, epoll_event.data.fd = fd,
    /// otherwise epoll_event.data.ptr = ptr.
    /// Default events are for reading from fd and for errors.
    void add(int fd, void * ptr = nullptr, uint32_t events = EPOLLIN | EPOLLERR);
    void add(int fd, uint32_t events) { add(fd, nullptr, events); }

    /// Remove file descriptor to epoll.
    void remove(int fd);

    /// Get events from epoll. Events are written in events_out, this function returns an amount of ready events.
    /// If blocking is false and there are no ready events,
    /// return empty vector, otherwise wait for ready events.
    size_t getManyReady(int max_events, epoll_event * events_out, bool blocking) const;

    int getFileDescriptor() const { return epoll_fd; }

    int size() const { return events_count; }

    bool empty() const { return events_count == 0; }

    const std::string & getDescription() const { return fd_description; }

    ~Epoll();

private:
    int epoll_fd;
    std::atomic<int> events_count;
    const std::string fd_description = "epoll";
};

}
#endif
