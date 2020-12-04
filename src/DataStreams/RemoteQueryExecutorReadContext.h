#pragma once
#include <sys/epoll.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_FROM_SOCKET;
    extern const int CANNOT_OPEN_FILE;
    extern const int SOCKET_TIMEOUT;
}

class RemoteQueryExecutorReadContext
{
public:
    using Self = RemoteQueryExecutorReadContext;

    bool is_read_in_progress = false;
    Packet packet;

    std::exception_ptr exception;
    FiberStack<> stack;
    std::mutex fiber_mutex;
    boost::context::fiber fiber;

    Poco::Timespan receive_timeout;
    MultiplexedConnections & connections;

    TimerDescriptor timer{CLOCK_MONOTONIC, 0};
    int socket_fd = -1;
    int epoll_fd;

    explicit RemoteQueryExecutorReadContext(MultiplexedConnections & connections_) : connections(connections_)
    {
        epoll_fd = epoll_create(2);
        if (-1 == epoll_fd)
            throwFromErrno("Cannot create epoll descriptor", ErrorCodes::CANNOT_OPEN_FILE);

        {
            epoll_event timer_event;
            timer_event.events = EPOLLIN | EPOLLPRI;
            timer_event.data.fd = timer.getDescriptor();

            if (-1 == epoll_ctl(epoll_fd, EPOLL_CTL_ADD, timer_event.data.fd, &timer_event))
                throwFromErrno("Cannot add timer descriptor to epoll", ErrorCodes::CANNOT_OPEN_FILE);
        }

        fiber = boost::context::fiber(std::allocator_arg_t(), stack, Routine{connections, *this});
    }

    void setSocket(Poco::Net::Socket & socket)
    {
        int fd = socket.impl()->sockfd();
        if (fd == socket_fd)
            return;

        epoll_event socket_event;
        socket_event.events = EPOLLIN | EPOLLPRI;
        socket_event.data.fd = fd;

        if (socket_fd != -1)
        {
            if (-1 == epoll_ctl(epoll_fd, EPOLL_CTL_DEL, socket_fd, &socket_event))
                throwFromErrno("Cannot remove socket descriptor to epoll", ErrorCodes::CANNOT_OPEN_FILE);
        }

        socket_fd = fd;

        if (-1 == epoll_ctl(epoll_fd, EPOLL_CTL_ADD, socket_fd, &socket_event))
            throwFromErrno("Cannot add socket descriptor to epoll", ErrorCodes::CANNOT_OPEN_FILE);

        receive_timeout = socket.impl()->getReceiveTimeout();
    }

    void checkTimeout() const
    {
        try
        {
            checkTimeoutImpl();
        }
        catch (DB::Exception & e)
        {
            e.addMessage(" while reading from socket ({})", connections.getSocket().peerAddress().toString());
            throw;
        }
    }

    void checkTimeoutImpl() const
    {
        epoll_event events[2];

        /// Wait for epoll_fd will not block if it was polled externally.
        int num_events = epoll_wait(epoll_fd, events, 2, 0);
        if (num_events == -1)
            throwFromErrno("Failed to epoll_wait", ErrorCodes::CANNOT_READ_FROM_SOCKET);

        bool is_socket_ready = false;
        bool has_timer_alarm = false;

        for (int i = 0; i < num_events; ++i)
        {
            if (events[i].data.fd == socket_fd)
                is_socket_ready = true;
            if (events[i].data.fd == timer.getDescriptor())
                has_timer_alarm = true;
        }

        if (has_timer_alarm && !is_socket_ready)
        {
            /// Socket receive timeout. Drain it in case or error, or it may be hide by timeout exception.
            timer.drain();
            throw NetException("Timeout exceeded", ErrorCodes::SOCKET_TIMEOUT);
        }
    }

    void setTimer() const
    {
        /// Did not get packet yet. Init timeout for the next async reading.
        timer.reset();

        if (receive_timeout.totalMicroseconds())
            timer.setRelative(receive_timeout);
    }

    bool resumeRoutine()
    {
        if (is_read_in_progress)
            checkTimeout();

        {
            std::lock_guard guard(fiber_mutex);
            if (!fiber)
                return false;

            fiber = std::move(fiber).resume();
        }

        if (exception)
            std::rethrow_exception(std::move(exception));

        if (is_read_in_progress)
        {
            auto & socket = connections.getSocket();
            try
            {
                setSocket(socket);
            }
            catch (DB::Exception & e)
            {
                e.addMessage(" while reading from socket ({})", socket.peerAddress().toString());
                throw;
            }
        }

        return true;
    }

    void cancel()
    {
        std::lock_guard guard(fiber_mutex);
        boost::context::fiber to_destroy = std::move(fiber);
    }

    ~RemoteQueryExecutorReadContext()
    {
        /// socket_fd is closed by Poco::Net::Socket
        /// timer_fd is closed by TimerDescriptor
        close(epoll_fd);
    }

    struct Routine
    {
        MultiplexedConnections & connections;
        Self & read_context;

        boost::context::fiber operator()(boost::context::fiber && sink) const
        {
            try
            {
                while (true)
                {
                    connections.setFiber(&sink);

                    read_context.is_read_in_progress = true;
                    read_context.packet = connections.receivePacket();
                    read_context.is_read_in_progress = false;

                    sink = std::move(sink).resume();
                }
            }
            catch (const boost::context::detail::forced_unwind &)
            {
                throw;
            }
            catch (...)
            {
                read_context.exception = std::current_exception();
            }

            return std::move(sink);
        }
    };
};

}
