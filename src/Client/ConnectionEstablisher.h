#pragma once

#include <Common/Epoll.h>
#include <Common/Fiber.h>
#include <Common/FiberStack.h>
#include <Common/TimerDescriptor.h>
#include <Common/PoolWithFailoverBase.h>
#include <Client/ConnectionPool.h>

namespace DB
{

/// Class for nonblocking establishing connection to the replica.
/// It runs establishing connection process in fiber and sets special
/// read callback which is called when reading from socket blocks.
/// When read callback is called, socket and receive timeout are added in epoll
/// and execution returns to the main program.
/// So, you can poll this epoll file descriptor to determine when to resume.
class ConnectionEstablisher
{
public:
    using TryResult = PoolWithFailoverBase<IConnectionPool>::TryResult;

    ConnectionEstablisher(IConnectionPool * pool_,
                          const ConnectionTimeouts * timeouts_,
                          const Settings * settings_,
                          const QualifiedTableName * table_to_check = nullptr);

    /// Establish connection with replica, call async_callbeck when
    /// reading from socket blocks.
    void establishConnection(AsyncCallback async_callback = {});

    /// In the first call create fiber with establishConnection function,
    /// in the next - check timeout and resume fiber.
    void resume();

    /// Cancel establishing connections. Fiber will be destroyed,
    /// class will be set in initial stage.
    void cancel();

    bool isInProcess() const { return stage == Stage::IN_PROCESS; }

    bool isFinished() const { return stage == Stage::FINISHED; }

    bool isFailed() const { return stage == Stage::FAILED; }

    int getFileDescriptor() const
    {
        int fd = -1;
#if defined(OS_LINUX)
        fd = epoll.getFileDescriptor();
#endif
        return fd;
    }

    const std::string & getFailMessage() const { return fail_message; }

    TryResult getResult() { return result; }

    Connection * getConnection() { return &*result.entry; }


private:
    void processReceiveTimeout();

    enum class Stage
    {
        INITIAL,
        IN_PROCESS,
        FINISHED,
        FAILED,
    };

    struct Routine
    {
        ConnectionEstablisher & connection_establisher;

        struct ReadCallback
        {
            ConnectionEstablisher & connection_establisher;
            Fiber & fiber;

            void operator()(int fd, const Poco::Timespan & timeout, const std::string &);
        };

        Fiber operator()(Fiber && sink);
    };

    void resetResult();

    void reset();

    void destroyFiber();

    void resumeFiber();

    IConnectionPool * pool;
    const ConnectionTimeouts * timeouts;
    std::string fail_message;
    const Settings * settings;
    const QualifiedTableName * table_to_check;
    TryResult result;
    Stage stage;
    Poco::Logger * log;
    Fiber fiber;
    FiberStack fiber_stack;
    std::exception_ptr exception;
    int socket_fd = -1;
    bool fiber_created = false;
#if defined(OS_LINUX)
    TimerDescriptor receive_timeout;
    Epoll epoll;
#endif
};

}
