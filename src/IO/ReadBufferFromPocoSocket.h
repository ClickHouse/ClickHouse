#pragma once

#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBuffer.h>
#include <Common/AsyncTaskExecutor.h>
#include <Poco/Net/Socket.h>

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

    bool poll(size_t timeout_microseconds) const;

    void setAsyncCallback(AsyncCallback async_callback_) { async_callback = std::move(async_callback_); }

    ssize_t socketReceiveBytesImpl(char * ptr, size_t size);

private:
    AsyncCallback async_callback;
    std::string socket_description;
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
