#pragma once
#if defined(OS_LINUX)

#include <sys/epoll.h>
#include <vector>
#include <boost/noncopyable.hpp>
#include <Poco/Logger.h>

namespace DB
{

using AsyncCallback = std::function<void(int, Poco::Timespan, const std::string &)>;

class Epoll
{
public:
    Epoll();

    Epoll(const Epoll &) = delete;
    Epoll & operator=(const Epoll &) = delete;

    Epoll & operator=(Epoll && other);
    Epoll(Epoll && other);

    /// Add new file descriptor to epoll. If ptr set to nullptr, epoll_event.data.fd = fd,
    /// otherwise epoll_event.data.ptr = ptr.
    void add(int fd, void * ptr = nullptr);

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
