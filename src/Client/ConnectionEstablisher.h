#pragma once

#include <variant>

#include <Common/AsyncTaskExecutor.h>
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
                          const Settings & settings_,
                          LoggerPtr log,
                          const QualifiedTableName * table_to_check = nullptr);

    /// Establish connection and save it in result, write possible exception message in fail_message.
    void run(TryResult & result, std::string & fail_message);

    /// Set async callback that will be called when reading from socket blocks.
    void setAsyncCallback(AsyncCallback async_callback_) { async_callback = std::move(async_callback_); }

    bool isFinished() const { return is_finished; }

private:
    IConnectionPool * pool;
    const ConnectionTimeouts * timeouts;
    const Settings & settings;
    LoggerPtr log;
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
class ConnectionEstablisherAsync : public AsyncTaskExecutor
{
public:
    using TryResult = PoolWithFailoverBase<IConnectionPool>::TryResult;

    ConnectionEstablisherAsync(IConnectionPool * pool_,
                               const ConnectionTimeouts * timeouts_,
                               const Settings & settings_,
                               LoggerPtr log_,
                               const QualifiedTableName * table_to_check_ = nullptr);

    /// Get file descriptor that can be added in epoll and be polled,
    /// when this fd becomes ready, you call resume establishing connection.
    int getFileDescriptor() { return epoll.getFileDescriptor(); }

    /// Check if the process of connection establishing was finished.
    /// The process is considered finished if connection is ready,
    /// some exception occurred or timeout exceeded.
    bool isFinished() const { return is_finished; }
    TryResult getResult() const { return result; }

    const std::string & getFailMessage() const { return fail_message; }

private:
    bool checkBeforeTaskResume() override;

    void afterTaskResume() override;

    void processAsyncEvent(int fd, Poco::Timespan socket_timeout, AsyncEventTimeoutType type, const std::string & description, uint32_t events) override;
    void clearAsyncEvent() override;

    struct Task : public AsyncTask
    {
        explicit Task(ConnectionEstablisherAsync & connection_establisher_async_)
            : connection_establisher_async(connection_establisher_async_)
        {
        }

        ConnectionEstablisherAsync & connection_establisher_async;

        void run(AsyncCallback async_callback, SuspendCallback suspend_callback) override;
    };

    void cancelAfter() override;

    /// When epoll file descriptor is ready, check if it's an expired timeout.
    /// Return false if receive timeout expired and socket is not ready, return true otherwise.
    bool checkTimeout();

    void reset();

    void resetResult();

    bool haveMoreAddressesToConnect();

    ConnectionEstablisher connection_establisher;
    TryResult result;
    std::string fail_message;

    /// We use timer descriptor for checking socket receive timeout.
    TimerDescriptor timeout_descriptor;
    Poco::Timespan timeout;
    AsyncEventTimeoutType timeout_type;

    /// In read callback we add socket file descriptor and timer descriptor with receive timeout
    /// in epoll, so we can return epoll file descriptor outside for polling.
    Epoll epoll;
    int socket_fd = -1;
    std::string socket_description;

    bool is_finished = false;
    bool restarted = false;
};

#endif

}
