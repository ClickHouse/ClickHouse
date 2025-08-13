#pragma once

#if defined(OS_LINUX)

#include <variant>

#include <Client/IConnections.h>
#include <Common/Fiber.h>
#include <Common/Epoll.h>
#include <Common/TimerDescriptor.h>
#include <Common/AsyncTaskExecutor.h>

namespace DB
{

/// Class for nonblocking packet receiving. It runs connection->receivePacket
/// in fiber and sets special read callback which is called when
/// reading from socket blocks. When read callback is called,
/// socket and receive timeout are added in epoll and execution returns to the main program.
/// So, you can poll this epoll file descriptor to determine when to resume
/// packet receiving.
class PacketReceiver : public AsyncTaskExecutor
{
public:
    explicit PacketReceiver(Connection * connection_);

    bool isPacketReady() const { return !is_read_in_process && !is_timeout_expired && !exception; }
    Packet getPacket() { return std::move(packet); }

    bool hasException() const { return exception.operator bool(); }
    std::exception_ptr getException() const { return exception; }

    bool isTimeoutExpired() const { return is_timeout_expired; }
    Poco::Timespan getTimeout() const { return timeout; }

    void setTimeout(const Poco::Timespan & timeout_)
    {
        timeout_descriptor.setRelative(timeout_);
        timeout = timeout_;
    }

    int getFileDescriptor() const { return epoll.getFileDescriptor(); }

private:
    bool checkBeforeTaskResume() override;
    void afterTaskResume() override {}

    void processAsyncEvent(int fd, Poco::Timespan socket_timeout, AsyncEventTimeoutType, const std::string &, uint32_t) override;
    void clearAsyncEvent() override;

    void processException(std::exception_ptr e) override { exception = e; }

    struct Task : public AsyncTask
    {
        explicit Task(PacketReceiver & receiver_) : receiver(receiver_) {}

        PacketReceiver & receiver;

        void run(AsyncCallback async_callback, SuspendCallback suspend_callback) override;
    };

    /// When epoll file descriptor is ready, check if it's an expired timeout.
    /// Return false if receive timeout expired and socket is not ready, return true otherwise.
    bool checkTimeout();

    Connection * connection;
    int socket_fd = -1;
    Packet packet;

    /// We use timer descriptor for checking socket timeouts.
    TimerDescriptor timeout_descriptor;
    Poco::Timespan timeout;
    bool is_timeout_expired = false;

    /// In read callback we add socket file descriptor and timer descriptor with receive timeout
    /// in epoll, so we can return epoll file descriptor outside for polling.
    Epoll epoll;

    /// If and exception occurred in fiber resume, we save it and rethrow.
    std::exception_ptr exception;

    bool is_read_in_process = false;
};

}
#endif
