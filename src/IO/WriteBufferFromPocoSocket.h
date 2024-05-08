#pragma once

#include <Poco/Net/Socket.h>

#include <IO/WriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <Common/AsyncTaskExecutor.h>

namespace DB
{

using AsyncCallback = std::function<void(int, Poco::Timespan, AsyncEventTimeoutType, const std::string &, uint32_t)>;

/** Works with the ready Poco::Net::Socket. Blocking operations.
  */
class WriteBufferFromPocoSocket : public BufferWithOwnMemory<WriteBuffer>
{
public:
    explicit WriteBufferFromPocoSocket(Poco::Net::Socket & socket_, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE);

    ~WriteBufferFromPocoSocket() override;

    void setAsyncCallback(AsyncCallback async_callback_) { async_callback = std::move(async_callback_); }

protected:
    void nextImpl() override;

    Poco::Net::Socket & socket;

    /** For error messages. It is necessary to receive this address in advance, because,
      *  for example, if the connection is broken, the address will not be received anymore
      *  (getpeername will return an error).
      */
    Poco::Net::SocketAddress peer_address;
    Poco::Net::SocketAddress our_address;

private:
    AsyncCallback async_callback;
    std::string socket_description;
};

}
