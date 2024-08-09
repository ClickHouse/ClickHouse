#pragma once

#if defined(OS_LINUX)

#include <atomic>
#include <Common/Fiber.h>
#include <Common/TimerDescriptor.h>
#include <Common/Epoll.h>
#include <Common/AsyncTaskExecutor.h>
#include <Client/Connection.h>
#include <Client/IConnections.h>
#include <Poco/Timespan.h>

namespace Poco::Net
{
class Socket;
}

namespace DB
{

class MultiplexedConnections;
class RemoteQueryExecutor;

class RemoteQueryExecutorReadContext : public AsyncTaskExecutor
{
public:
    explicit RemoteQueryExecutorReadContext(RemoteQueryExecutor & executor_, bool suspend_when_query_sent_ = false);

    ~RemoteQueryExecutorReadContext() override;

    bool isInProgress() const { return is_in_progress.load(std::memory_order_relaxed); }

    bool isCancelled() const { return AsyncTaskExecutor::isCancelled() || is_pipe_alarmed; }

    bool isQuerySent() const { return is_query_sent;  }

    int getFileDescriptor() const { return epoll.getFileDescriptor(); }

    Packet getPacket() { return std::move(packet); }

    UInt64 getPacketType() const { return packet.type; }

private:
    bool checkTimeout(bool blocking = false);

    bool checkBeforeTaskResume() override;
    void afterTaskResume() override {}

    void processAsyncEvent(int fd, Poco::Timespan socket_timeout, AsyncEventTimeoutType type, const std::string & description, uint32_t events) override;
    void clearAsyncEvent() override;

    void cancelBefore() override;

    struct Task : public AsyncTask
    {
        explicit Task(RemoteQueryExecutorReadContext & read_context_) : read_context(read_context_) {}

        RemoteQueryExecutorReadContext & read_context;

        void run(AsyncCallback async_callback, SuspendCallback suspend_callback) override;
    };

    std::atomic_bool is_in_progress = false;
    Packet packet;

    RemoteQueryExecutor & executor;

    /// Here we have three descriptors we are going to wait:
    /// * connection_fd is a descriptor of connection. It may be changed in case of reading from several replicas.
    /// * timer is a timerfd descriptor to manually check socket timeout
    /// * pipe_fd is a pipe we use to cancel query and socket polling by executor.
    /// We put those descriptors into our own epoll which is used by external executor.
    TimerDescriptor timer;
    Poco::Timespan timeout;
    AsyncEventTimeoutType timeout_type;
    std::atomic_bool is_timer_alarmed = false;
    int connection_fd = -1;
    int pipe_fd[2] = { -1, -1 };
    std::atomic_bool is_pipe_alarmed = false;

    Epoll epoll;

    std::string connection_fd_description;
    bool suspend_when_query_sent = false;
    bool is_query_sent = false;
};

}

#else

namespace DB
{
class RemoteQueryExecutorReadContext
{
public:
    void cancel() {}
    void setTimer() {}
};

}
#endif
