#pragma once

#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBuffer.h>
#include <Common/AsyncTaskExecutor.h>
#include <Common/Stopwatch.h>
#include <Common/ProfileEvents.h>
#include <Poco/Net/Socket.h>

#include <optional>

namespace DB
{

/// Works with the ready Poco::Net::Socket. Blocking operations.
class ReadBufferFromPocoSocketBase : public BufferWithOwnMemory<ReadBuffer>
{
protected:
    Poco::Net::Socket & socket;

    /** For error messages. It is necessary to receive this address in advance, because,
      *  for example, if the connection is broken, the address will not be received anymore
      *  (getpeername will return an error).
      */
    Poco::Net::SocketAddress peer_address;

    ProfileEvents::Event read_event;

    bool nextImpl() override;

public:
    explicit ReadBufferFromPocoSocketBase(Poco::Net::Socket & socket_, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE);
    explicit ReadBufferFromPocoSocketBase(Poco::Net::Socket & socket_, const ProfileEvents::Event & read_event_, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE);

    bool poll(size_t timeout_microseconds) override;

    void setAsyncCallback(AsyncCallback async_callback_);

    ssize_t socketReceiveBytesImpl(char * ptr, size_t size);

    /// 0 does nothing, rather than clearing the socket's timeout.
    void setReceiveTimeout(size_t receive_timeout_microseconds);

    /// Bound the whole handshake phase, so an unauthenticated connection cannot hold a thread,
    /// whether it trickles bytes or goes silent inside a read. 0 disables.
    void setHandshakeTimeout(size_t timeout_milliseconds);
    void clearHandshakeTimeout();
    /// What is left of the deadline, for a buffer that replaces this one mid-handshake to adopt.
    /// 0 only when none is armed, so adopting it never disables the deadline by accident.
    UInt64 handshakeMillisecondsLeft() const;

private:
    void clampReceiveTimeoutToHandshakeDeadline(UInt64 milliseconds_left);

    AsyncCallback async_callback;
    std::string socket_description;

    size_t handshake_timeout_milliseconds = 0;
    Stopwatch handshake_stopwatch = Stopwatch(STOPWATCH_DEFAULT_CLOCK, 0, /* is running */ false);
    std::optional<Poco::Timespan> receive_timeout_before_handshake;
};

class ReadBufferFromPocoSocket : public ReadBufferFromPocoSocketBase
{
public:
    explicit ReadBufferFromPocoSocket(Poco::Net::Socket & socket_, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE)
        : ReadBufferFromPocoSocketBase(socket_, buf_size)
    {}
    explicit ReadBufferFromPocoSocket(Poco::Net::Socket & socket_, const ProfileEvents::Event & read_event_, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE)
        : ReadBufferFromPocoSocketBase(socket_, read_event_, buf_size)
    {}
};

}
