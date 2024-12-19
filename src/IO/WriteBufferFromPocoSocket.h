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
    explicit WriteBufferFromPocoSocket(Poco::Net::Socket & socket_, const ProfileEvents::Event & write_event_, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE);

    ~WriteBufferFromPocoSocket() override;

    void setAsyncCallback(AsyncCallback async_callback_) { async_callback = std::move(async_callback_); }

    using WriteBuffer::write;
    void write(const std::string & str) { WriteBuffer::write(str.c_str(), str.size()); }
    void write(std::string_view str) { WriteBuffer::write(str.data(), str.size()); }
    void write(const char * str) { WriteBuffer::write(str, strlen(str)); }
    void writeln(const std::string & str) { write(str); WriteBuffer::write("\n", 1); }
    void writeln(std::string_view str) { write(str); WriteBuffer::write("\n", 1); }
    void writeln(const char * str) { write(str); WriteBuffer::write("\n", 1); }

protected:
    void nextImpl() override;

    void socketSendBytes(const char * ptr, size_t size);
    void socketSendStr(const std::string & str)
    {
        socketSendBytes(str.data(), str.size());
    }
    void socketSendStr(const char * ptr)
    {
        socketSendBytes(ptr, strlen(ptr));
    }

    Poco::Net::Socket & socket;

    /** For error messages. It is necessary to receive this address in advance, because,
      *  for example, if the connection is broken, the address will not be received anymore
      *  (getpeername will return an error).
      */
    Poco::Net::SocketAddress peer_address;
    Poco::Net::SocketAddress our_address;

    ProfileEvents::Event write_event;

private:
    AsyncCallback async_callback;
    std::string socket_description;

    ssize_t socketSendBytesImpl(const char * ptr, size_t size);
};

}
