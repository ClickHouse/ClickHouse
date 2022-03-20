#pragma once

#include <Poco/Net/Socket.h>

#include <IO/WriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>


namespace DB
{

/** Works with the ready Poco::Net::Socket. Blocking operations.
  */
class WriteBufferFromPocoSocket : public BufferWithOwnMemory<WriteBuffer>
{
public:
    explicit WriteBufferFromPocoSocket(Poco::Net::Socket & socket_, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE);

    ~WriteBufferFromPocoSocket() override;

protected:
    void nextImpl() override;

    Poco::Net::Socket & socket;

    /** For error messages. It is necessary to receive this address in advance, because,
      *  for example, if the connection is broken, the address will not be received anymore
      *  (getpeername will return an error).
      */
    Poco::Net::SocketAddress peer_address;
};

}
