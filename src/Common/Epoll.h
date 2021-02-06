#pragma once
#if defined(OS_LINUX)

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

    /// Add new file descriptor to epoll. If ptr set to nullptr, epoll_event.data.fd = fd,
    /// otherwise epoll_event.data.ptr = ptr.
    void add(int fd, void * ptr = nullptr);

    /// Remove file descriptor to epoll.
    void remove(int fd);

    /// Get events from epoll. Events are written in events_out, this function returns an amount of ready events.
    /// If blocking is false and there are no ready events,
    /// return empty vector, otherwise wait for ready events. If blocking is true,
    /// async_callback is given and there is no ready events, async_callback is called
    /// with epoll file descriptor.
    size_t getManyReady(int max_events, epoll_event * events_out, bool blocking, AsyncCallback async_callback = {}) const;

    /// Get only one ready event, if blocking is false and there is no ready events, epoll_event.data.fd will be set to -1.
    epoll_event getReady(bool blocking = true, AsyncCallback async_callback = {}) const;

    int getFileDescriptor() const { return epoll_fd; }

    int size() const { return events_count; }

    bool empty() const { return events_count == 0; }

    ~Epoll();

private:
    int epoll_fd;
    int events_count;
};

}
#endif
