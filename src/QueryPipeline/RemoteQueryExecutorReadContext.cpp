#if defined(OS_LINUX)

#include <QueryPipeline/RemoteQueryExecutorReadContext.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <base/defines.h>
#include <Common/Exception.h>
#include <Common/NetException.h>
#include <Client/IConnections.h>
#include <Common/AsyncTaskExecutor.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_FROM_SOCKET;
    extern const int CANNOT_OPEN_FILE;
    extern const int SOCKET_TIMEOUT;
}

RemoteQueryExecutorReadContext::RemoteQueryExecutorReadContext(RemoteQueryExecutor & executor_, bool suspend_when_query_sent_)
    : AsyncTaskExecutor(std::make_unique<Task>(*this)), executor(executor_), suspend_when_query_sent(suspend_when_query_sent_)
{
    if (-1 == pipe2(pipe_fd, O_NONBLOCK))
        throw ErrnoException(ErrorCodes::CANNOT_OPEN_FILE, "Cannot create pipe");

    epoll.add(pipe_fd[0]);
    epoll.add(timer.getDescriptor());
}

bool RemoteQueryExecutorReadContext::checkBeforeTaskResume()
{
    return !is_in_progress.load(std::memory_order_relaxed) || checkTimeout();
}


void RemoteQueryExecutorReadContext::Task::run(AsyncCallback async_callback, SuspendCallback suspend_callback)
{
    read_context.executor.sendQueryUnlocked(ClientInfo::QueryKind::SECONDARY_QUERY, async_callback);
    read_context.is_query_sent = true;

    if (read_context.suspend_when_query_sent)
        suspend_callback();

    if (read_context.executor.needToSkipUnavailableShard())
        return;

    while (true)
    {
        read_context.packet = read_context.executor.getConnections().receivePacketUnlocked(async_callback);
        suspend_callback();
    }
}

void RemoteQueryExecutorReadContext::processAsyncEvent(int fd, Poco::Timespan socket_timeout, AsyncEventTimeoutType type, const std::string & description, uint32_t events)
{
    connection_fd = fd;
    epoll.add(connection_fd, events);
    timeout = socket_timeout;
    timer.setRelative(socket_timeout);
    timeout_type = type;
    connection_fd_description = description;
    is_in_progress.store(true);
}

void RemoteQueryExecutorReadContext::clearAsyncEvent()
{
    epoll.remove(connection_fd);
    timer.reset();
    is_in_progress.store(false);
}

bool RemoteQueryExecutorReadContext::checkTimeout(bool blocking)
{
    /// Wait for epoll will not block if it was polled externally.
    epoll_event events[3];
    events[0].data.fd = events[1].data.fd = events[2].data.fd = -1;

    size_t num_events = epoll.getManyReady(3, events, blocking ? -1 : 0);

    bool is_socket_ready = false;

    for (size_t i = 0; i < num_events; ++i)
    {
        if (events[i].data.fd == connection_fd)
            is_socket_ready = true;
        if (events[i].data.fd == timer.getDescriptor())
            is_timer_alarmed = true;
        if (events[i].data.fd == pipe_fd[0])
            is_pipe_alarmed = true;
    }

    if (is_pipe_alarmed)
        return false;

    if (is_timer_alarmed && !is_socket_ready)
    {
        /// Socket timeout. Drain it in case of error, or it may be hide by timeout exception.
        timer.drain();
        const String exception_message = getSocketTimeoutExceededMessageByTimeoutType(timeout_type, timeout, connection_fd_description);
        throw NetException(ErrorCodes::SOCKET_TIMEOUT, exception_message);
    }

    return true;
}

void RemoteQueryExecutorReadContext::cancelBefore()
{
    /// One should not try to wait for the current packet here in case of
    /// timeout because this will exceed the timeout.
    /// Anyway if the timeout is exceeded, then the connection will be shutdown
    /// (disconnected), so it will not left in an unsynchronised state.
    if (!is_timer_alarmed)
    {
        /// If query wasn't sent, just complete sending it.
        if (!is_query_sent)
            suspend_when_query_sent = true;

        /// Wait for current pending packet, to avoid leaving connection in unsynchronised state.
        while (is_in_progress.load(std::memory_order_relaxed))
        {
            checkTimeout(/* blocking= */ true);
            resumeUnlocked();
        }
    }

    /// Send something to pipe to cancel executor waiting.
    uint64_t buf = 0;
    while (-1 == write(pipe_fd[1], &buf, sizeof(buf)))
    {
        if (errno == EAGAIN)
            break;

        if (errno != EINTR)
            throw ErrnoException(ErrorCodes::CANNOT_READ_FROM_SOCKET, "Cannot write to pipe");
    }
}

RemoteQueryExecutorReadContext::~RemoteQueryExecutorReadContext()
{
    /// connection_fd is closed by Poco::Net::Socket or Epoll
    if (pipe_fd[0] != -1)
    {
        [[maybe_unused]] int err = close(pipe_fd[0]);
        chassert(!err || errno == EINTR);
    }
    if (pipe_fd[1] != -1)
    {
        [[maybe_unused]] int err = close(pipe_fd[1]);
        chassert(!err || errno == EINTR);
    }
}

}
#endif
