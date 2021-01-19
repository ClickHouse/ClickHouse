#pragma once

#include <sys/epoll.h>
#include <vector>
#include <boost/noncopyable.hpp>
#include <Poco/Logger.h>

namespace DB
{

using AsyncCallback = std::function<void(int, const Poco::Timespan &, const std::string &)>;

class Epoll : boost::noncopyable
{
public:
    Epoll();

    /// Add new file descriptor to epoll.
    void add(int fd, void * ptr = nullptr);

    /// Remove file descriptor to epoll.
    void remove(int fd);

    /// Get events from epoll. If blocking is false and there are no ready events,
    /// return empty vector, otherwise wait for ready events. If blocking is true,
    /// async_callback is given and there is no ready events, async_callback is called
    /// with epoll file descriptor.
    std::vector<epoll_event> getManyReady(int max_events, bool blocking, AsyncCallback async_callback = {}) const;

    /// Get only one ready event, this function is always blocking.
    epoll_event getReady(AsyncCallback async_callback = {}) const;

    int getFileDescriptor() const { return epoll_fd; }

    int size() const { return events_count; }

    ~Epoll();

private:
    int epoll_fd;
    int events_count;
};

}
