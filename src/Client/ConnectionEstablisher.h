#pragma once

#include <variant>

#include <Common/Epoll.h>
#include <Common/Fiber.h>
#include <Common/FiberStack.h>
#include <Common/TimerDescriptor.h>
#include <Common/PoolWithFailoverBase.h>
#include <Client/ConnectionPool.h>

namespace DB
{

/// Class for establishing connection to the replica. It supports setting up
/// an async callback that will be called when reading from socket blocks.
class ConnectionEstablisher
{
public:
    using TryResult = PoolWithFailoverBase<IConnectionPool>::TryResult;

    ConnectionEstablisher(IConnectionPool * pool_,
                          const ConnectionTimeouts * timeouts_,
                          const Settings * settings_,
                          Poco::Logger * log,
                          const QualifiedTableName * table_to_check = nullptr);

    /// Establish connection and save it in result, write possible exception message in fail_message.
    void run(TryResult & result, std::string & fail_message);

    /// Set async callback that will be called when reading from socket blocks.
    void setAsyncCallback(AsyncCallback async_callback_) { async_callback = std::move(async_callback_); }

    bool isFinished() const { return is_finished; }

private:
    IConnectionPool * pool;
    const ConnectionTimeouts * timeouts;
    const Settings * settings;
    Poco::Logger * log;
    const QualifiedTableName * table_to_check;

    bool is_finished;
    AsyncCallback async_callback = {};

};

#if defined(OS_LINUX)

/// Class for nonblocking establishing connection to the replica.
/// It runs establishing connection process in fiber and sets special
/// read callback which is called when reading from socket blocks.
/// When read callback is called, socket and receive timeout are added in epoll
/// and execution returns to the main program.
/// So, you can poll this epoll file descriptor to determine when to resume.
class ConnectionEstablisherAsync
{
public:
    using TryResult = PoolWithFailoverBase<IConnectionPool>::TryResult;

    ConnectionEstablisherAsync(IConnectionPool * pool_,
                          const ConnectionTimeouts * timeouts_,
                          const Settings * settings_,
                          Poco::Logger * log_,
                          const QualifiedTableName * table_to_check = nullptr);

    /// Resume establishing connection. If the process was not finished,
    /// return file descriptor (you can add it in epoll and poll it,
    /// when this fd become ready, call resume again),
    /// if the process was failed or finished, return it's result,
    std::variant<int, TryResult> resume();

    /// Cancel establishing connections. Fiber will be destroyed,
    /// class will be set in initial stage.
    void cancel();

    TryResult getResult() const { return result; }

    const std::string & getFailMessage() const { return fail_message; }

private:
    /// When epoll file descriptor is ready, check if it's an expired timeout.
    /// Return false if receive timeout expired and socket is not ready, return true otherwise.
    bool checkReceiveTimeout();

    struct Routine
    {
        ConnectionEstablisherAsync & connection_establisher_async;

        struct ReadCallback
        {
            ConnectionEstablisherAsync & connection_establisher_async;
            Fiber & fiber;

            void operator()(int fd, Poco::Timespan timeout, const std::string &);
        };

        Fiber operator()(Fiber && sink);
    };

    void reset();

    void resetResult();

    void destroyFiber();

    ConnectionEstablisher connection_establisher;
    TryResult result;
    std::string fail_message;

    Fiber fiber;
    FiberStack fiber_stack;

    /// We use timer descriptor for checking socket receive timeout.
    TimerDescriptor receive_timeout;

    /// In read callback we add socket file descriptor and timer descriptor with receive timeout
    /// in epoll, so we can return epoll file descriptor outside for polling.
    Epoll epoll;
    int socket_fd = -1;
    std::string socket_description;

    /// If and exception occurred in fiber resume, we save it and rethrow.
    std::exception_ptr exception;

    bool fiber_created = false;
};

#endif

}
