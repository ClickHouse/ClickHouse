#if defined(OS_LINUX) || defined(OS_DARWIN)

#include <Common/Epoll.h>
#include <Common/Exception.h>
#include <Common/ErrnoException.h>
#include <Common/Stopwatch.h>
#include <base/defines.h>

#include <algorithm>
#include <cerrno>
#include <unistd.h>

#if defined(OS_DARWIN)
#include <vector>
#include <sys/event.h>
#include <sys/time.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int EPOLL_ERROR;
    extern const int LOGICAL_ERROR;
}

#if defined(OS_LINUX)

Epoll::Epoll() : events_count(0)
{
    epoll_fd = epoll_create1(0);
    if (epoll_fd == -1)
        throw ErrnoException(ErrorCodes::EPOLL_ERROR, "Cannot open epoll descriptor");
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
    epoll_event event{};
    event.events = events | EPOLLPRI;
    if (ptr)
        event.data.ptr = ptr;
    else
        event.data.fd = fd;

    ++events_count;

    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &event) == -1)
        throw ErrnoException(ErrorCodes::EPOLL_ERROR, "Cannot add new descriptor to epoll");
}

void Epoll::remove(int fd)
{
    --events_count;

    if (epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr) == -1)
        throw ErrnoException(ErrorCodes::EPOLL_ERROR, "Cannot remove descriptor from epoll");
}

size_t Epoll::getManyReady(int max_events, epoll_event * events_out, int timeout) const
{
    if (events_count == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There are no events in epoll");

    Stopwatch watch;
    int ready_size = 0;
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
            throw ErrnoException(ErrorCodes::EPOLL_ERROR, "Error in epoll_wait");
        }
        break;
    }

    return ready_size;
}

#elif defined(OS_DARWIN)

/// macOS implementation of the Epoll interface on top of kqueue.
/// EPOLLIN -> EVFILT_READ, EPOLLOUT -> EVFILT_WRITE; errors/EOF are reported via EV_ERROR/EV_EOF
/// on whichever filter is registered. The `epoll_event.data` union is round-tripped through the
/// kevent `udata` field, so callers read back the same `.fd`/`.ptr` they registered with.

Epoll::Epoll() : events_count(0)
{
    epoll_fd = kqueue();
    if (epoll_fd == -1)
        throw ErrnoException(ErrorCodes::EPOLL_ERROR, "Cannot create kqueue descriptor");
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
    epoll_data_t data{};
    if (ptr)
        data.ptr = ptr;
    else
        data.fd = fd;

    struct kevent changes[2];
    int n = 0;
    /// EV_ADD is idempotent and re-arms the filter; kqueue read/write filters are level-triggered like epoll.
    if (events & EPOLLIN)
        EV_SET(&changes[n++], fd, EVFILT_READ, EV_ADD, 0, 0, reinterpret_cast<void *>(data.u64));
    if (events & EPOLLOUT)
        EV_SET(&changes[n++], fd, EVFILT_WRITE, EV_ADD, 0, 0, reinterpret_cast<void *>(data.u64));
    /// If neither readability nor writability was requested (e.g. only EPOLLERR), watch for readability
    /// so that errors and EOF are still delivered.
    if (n == 0)
        EV_SET(&changes[n++], fd, EVFILT_READ, EV_ADD, 0, 0, reinterpret_cast<void *>(data.u64));

    ++events_count;

    if (kevent(epoll_fd, changes, n, nullptr, 0, nullptr) == -1)
        throw ErrnoException(ErrorCodes::EPOLL_ERROR, "Cannot add new descriptor to kqueue");
}

void Epoll::remove(int fd)
{
    --events_count;

    struct kevent changes[2];
    EV_SET(&changes[0], fd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
    EV_SET(&changes[1], fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
    /// A given filter may not have been registered for this fd; ENOENT is expected then.
    for (auto & change : changes)
    {
        if (kevent(epoll_fd, &change, 1, nullptr, 0, nullptr) == -1 && errno != ENOENT)
            throw ErrnoException(ErrorCodes::EPOLL_ERROR, "Cannot remove descriptor from kqueue");
    }
}

size_t Epoll::getManyReady(int max_events, epoll_event * events_out, int timeout) const
{
    if (events_count == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There are no events in epoll");

    /// kqueue may report up to two native events per fd (read + write); over-allocate and coalesce below.
    std::vector<struct kevent> received(static_cast<size_t>(max_events) * 2);

    Stopwatch watch;
    int ready_size = 0;
    while (true)
    {
        struct timespec ts{};
        struct timespec * ts_ptr = nullptr;
        if (timeout >= 0)
        {
            ts.tv_sec = timeout / 1000;
            ts.tv_nsec = static_cast<long>(timeout % 1000) * 1'000'000;
            ts_ptr = &ts;
        }

        ready_size = kevent(epoll_fd, nullptr, 0, received.data(), static_cast<int>(received.size()), ts_ptr);

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
            throw ErrnoException(ErrorCodes::EPOLL_ERROR, "Error in kevent");
        }
        break;
    }

    /// epoll reports a single entry per descriptor; coalesce the native read/write events accordingly.
    size_t out_count = 0;
    for (int i = 0; i < ready_size; ++i)
    {
        const struct kevent & ev = received[i];
        epoll_data_t data{};
        data.u64 = reinterpret_cast<uint64_t>(ev.udata);

        uint32_t e = 0;
        if (ev.filter == EVFILT_READ)
            e |= EPOLLIN;
        if (ev.filter == EVFILT_WRITE)
            e |= EPOLLOUT;
        if (ev.flags & EV_ERROR)
            e |= EPOLLERR;
        if (ev.flags & EV_EOF)
            e |= EPOLLHUP;

        bool merged = false;
        for (size_t j = 0; j < out_count; ++j)
        {
            if (events_out[j].data.u64 == data.u64)
            {
                events_out[j].events |= e;
                merged = true;
                break;
            }
        }

        if (!merged && out_count < static_cast<size_t>(max_events))
        {
            events_out[out_count].events = e;
            events_out[out_count].data = data;
            ++out_count;
        }
    }

    return out_count;
}

#endif

Epoll::~Epoll()
{
    if (epoll_fd != -1)
    {
        [[maybe_unused]] int err = close(epoll_fd);
        chassert(!err || errno == EINTR);
    }
}

}
#endif
