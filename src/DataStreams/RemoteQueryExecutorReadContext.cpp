#if defined(OS_LINUX)

#include <DataStreams/RemoteQueryExecutorReadContext.h>
#include <Common/Exception.h>
#include <Common/NetException.h>
#include <Client/IConnections.h>
#include <sys/epoll.h>

namespace DB
{

struct RemoteQueryExecutorRoutine
{
    IConnections & connections;
    RemoteQueryExecutorReadContext & read_context;

    struct ReadCallback
    {
        RemoteQueryExecutorReadContext & read_context;
        Fiber & fiber;

        void operator()(int fd, const Poco::Timespan & timeout = 0, const std::string fd_description = "")
        {
            try
            {
                read_context.setConnectionFD(fd, timeout, fd_description);
            }
            catch (DB::Exception & e)
            {
                e.addMessage(" while reading from {}", fd_description);
                throw;
            }

            read_context.is_read_in_progress.store(true, std::memory_order_relaxed);
            fiber = std::move(fiber).resume();
            read_context.is_read_in_progress.store(false, std::memory_order_relaxed);
        }
    };

    Fiber operator()(Fiber && sink) const
    {
        try
        {
            while (true)
            {
                read_context.packet = connections.receivePacketUnlocked(ReadCallback{read_context, sink});
                sink = std::move(sink).resume();
            }
        }
        catch (const boost::context::detail::forced_unwind &)
        {
            /// This exception is thrown by fiber implementation in case if fiber is being deleted but hasn't exited
            /// It should not be caught or it will segfault.
            /// Other exceptions must be caught
            throw;
        }
        catch (...)
        {
            read_context.exception = std::current_exception();
        }

        return std::move(sink);
    }
};

namespace ErrorCodes
{
    extern const int CANNOT_READ_FROM_SOCKET;
    extern const int CANNOT_OPEN_FILE;
    extern const int SOCKET_TIMEOUT;
}

RemoteQueryExecutorReadContext::RemoteQueryExecutorReadContext(IConnections & connections_)
    : connections(connections_)
{

    if (-1 == pipe2(pipe_fd, O_NONBLOCK))
        throwFromErrno("Cannot create pipe", ErrorCodes::CANNOT_OPEN_FILE);

    {
        epoll.add(pipe_fd[0]);
    }

    {
        epoll.add(timer.getDescriptor());
    }

    auto routine = RemoteQueryExecutorRoutine{connections, *this};
    fiber = boost::context::fiber(std::allocator_arg_t(), stack, std::move(routine));
}

void RemoteQueryExecutorReadContext::setConnectionFD(int fd, const Poco::Timespan & timeout, const std::string & fd_description)
{
    if (fd == connection_fd)
        return;

    if (connection_fd != -1)
        epoll.remove(connection_fd);

    connection_fd = fd;
    epoll.add(connection_fd);

    receive_timeout = timeout;
    connection_fd_description = fd_description;
}

bool RemoteQueryExecutorReadContext::checkTimeout(bool blocking) const
{
    try
    {
        return checkTimeoutImpl(blocking);
    }
    catch (DB::Exception & e)
    {
        if (last_used_socket)
            e.addMessage(" while reading from socket ({})", last_used_socket->peerAddress().toString());
        throw;
    }
}

bool RemoteQueryExecutorReadContext::checkTimeoutImpl(bool blocking) const
{
    /// Wait for epoll will not block if it was polled externally.
    epoll_event events[3];
    events[0].data.fd = events[1].data.fd = events[2].data.fd = -1;

    int num_events = epoll.getManyReady(3, events, blocking);

    bool is_socket_ready = false;
    bool is_pipe_alarmed = false;
    bool has_timer_alarm = false;

    for (int i = 0; i < num_events; ++i)
    {
        if (events[i].data.fd == connection_fd)
            is_socket_ready = true;
        if (events[i].data.fd == timer.getDescriptor())
            has_timer_alarm = true;
        if (events[i].data.fd == pipe_fd[0])
            is_pipe_alarmed = true;
    }

    if (is_pipe_alarmed)
        return false;

    if (has_timer_alarm && !is_socket_ready)
    {
        /// Socket receive timeout. Drain it in case or error, or it may be hide by timeout exception.
        timer.drain();
        throw NetException("Timeout exceeded", ErrorCodes::SOCKET_TIMEOUT);
    }

    return true;
}

void RemoteQueryExecutorReadContext::setTimer() const
{
    /// Did not get packet yet. Init timeout for the next async reading.
    timer.reset();

    if (receive_timeout.totalMicroseconds())
        timer.setRelative(receive_timeout);
}

bool RemoteQueryExecutorReadContext::resumeRoutine()
{
    if (is_read_in_progress.load(std::memory_order_relaxed) && !checkTimeout())
        return false;

    {
        std::lock_guard guard(fiber_lock);
        if (!fiber)
            return false;

        fiber = std::move(fiber).resume();
    }

    if (exception)
        std::rethrow_exception(std::move(exception));

    return true;
}

void RemoteQueryExecutorReadContext::cancel()
{
    std::lock_guard guard(fiber_lock);

    /// It is safe to just destroy fiber - we are not in the process of reading from socket.
    boost::context::fiber to_destroy = std::move(fiber);

    while (is_read_in_progress.load(std::memory_order_relaxed))
    {
        checkTimeout(/* blocking= */ true);
        to_destroy = std::move(to_destroy).resume();
    }

    /// Send something to pipe to cancel executor waiting.
    uint64_t buf = 0;
    while (-1 == write(pipe_fd[1], &buf, sizeof(buf)))
    {
        if (errno == EAGAIN)
            break;

        if (errno != EINTR)
            throwFromErrno("Cannot write to pipe", ErrorCodes::CANNOT_READ_FROM_SOCKET);
    }
}

RemoteQueryExecutorReadContext::~RemoteQueryExecutorReadContext()
{
    /// connection_fd is closed by Poco::Net::Socket or Epoll
    if (pipe_fd[0] != -1)
        close(pipe_fd[0]);
    if (pipe_fd[1] != -1)
        close(pipe_fd[1]);
}

}
#endif
