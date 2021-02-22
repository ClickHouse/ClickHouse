#pragma once

#if defined(OS_LINUX)

#include <mutex>
#include <atomic>
#include <Common/Fiber.h>
#include <Common/FiberStack.h>
#include <Common/TimerDescriptor.h>
#include <Client/Connection.h>
#include <Poco/Timespan.h>

namespace Poco::Net
{
class Socket;
}

namespace DB
{

class MultiplexedConnections;

class RemoteQueryExecutorReadContext
{
public:
    std::atomic_bool is_read_in_progress = false;
    Packet packet;

    std::exception_ptr exception;
    FiberStack stack;
    boost::context::fiber fiber;
    /// This mutex for fiber is needed because fiber could be destroyed in cancel method from another thread.
    std::mutex fiber_lock;

    Poco::Timespan receive_timeout;
    MultiplexedConnections & connections;
    Poco::Net::Socket * last_used_socket = nullptr;

    /// Here we have three descriptors we are going to wait:
    /// * socket_fd is a descriptor of connection. It may be changed in case of reading from several replicas.
    /// * timer is a timerfd descriptor to manually check socket timeout
    /// * pipe_fd is a pipe we use to cancel query and socket polling by executor.
    /// We put those descriptors into our own epoll_fd which is used by external executor.
    TimerDescriptor timer{CLOCK_MONOTONIC, 0};
    int socket_fd = -1;
    int epoll_fd = -1;
    int pipe_fd[2] = { -1, -1 };

    explicit RemoteQueryExecutorReadContext(MultiplexedConnections & connections_);
    ~RemoteQueryExecutorReadContext();

    bool checkTimeout() const;
    bool checkTimeoutImpl() const;

    void setSocket(Poco::Net::Socket & socket);
    void setTimer() const;

    bool resumeRoutine();
    void cancel();
};

}

#else

namespace DB
{
class RemoteQueryExecutorReadContext
{
public:
    void cancel() {}
};

}
#endif
