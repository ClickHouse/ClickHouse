#pragma once

#include <Poco/Net/Socket.h>

#include <IO/WriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <Common/AsyncTaskExecutor.h>
#include <Common/ProfileEvents.h>

namespace DB
{

using AsyncCallback = std::function<void(int, Poco::Timespan, AsyncEventTimeoutType, const std::string &, uint32_t)>;

/** Works with the ready Poco::Net::Socket. Blocking operations.
  */
class WriteBufferFromPocoSocket : public BufferWithOwnMemory<WriteBuffer>
{
public:
    explicit WriteBufferFromPocoSocket(Poco::Net::Socket & socket_, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE);
    /// If non-nullptr 'existing_memory' is passed, the buffer uses it without ownership and does not allocate.
    WriteBufferFromPocoSocket(Poco::Net::Socket & socket_, size_t buf_size, char * existing_memory);
    explicit WriteBufferFromPocoSocket(Poco::Net::Socket & socket_, const ProfileEvents::Event & write_event_, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE);

    void setAsyncCallback(AsyncCallback async_callback_);

    /// For a TLS connection that another thread reads at the same time. A TLS socket keeps its lock while it waits for the
    /// peer, so a write that has to wait keeps the reader out, also when the peer waits for that reader. With this, every
    /// write, even a small one, first waits without the lock until the socket is writable (on Linux, until a third of its
    /// buffer is free). A write of more than 4 KiB is limited to what normally fits then. If the wait takes longer than the
    /// send timeout, this and the next writes are made as usual, until the socket is writable again. A zero send timeout
    /// means no waiting.
    /// Must be called before another thread uses the buffer. Has no effect with an async callback.
    void setWaitBeforeSend() { wait_before_send = true; }

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
    bool wait_before_send = false;
    bool wait_failed = false;

    ssize_t socketSendBytesImpl(const char * ptr, size_t size);
    size_t waitBeforeSend(size_t size);
};

}
