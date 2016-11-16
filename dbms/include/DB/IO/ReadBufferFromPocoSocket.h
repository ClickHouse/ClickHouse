#pragma once

#include <Poco/Net/Socket.h>

#include <DB/IO/ReadBuffer.h>
#include <DB/IO/BufferWithOwnMemory.h>


namespace DB
{

/** Работает с готовым Poco::Net::Socket. Операции блокирующие.
  */
class ReadBufferFromPocoSocket : public BufferWithOwnMemory<ReadBuffer>
{
protected:
	Poco::Net::Socket & socket;

	/** Для сообщений об ошибках. Нужно получать этот адрес заранее, так как,
	  *  например, если соединение разорвано, то адрес уже будет получить нельзя
	  *  (getpeername вернёт ошибку).
	  */
	Poco::Net::SocketAddress peer_address;

	bool nextImpl() override;

public:
	ReadBufferFromPocoSocket(Poco::Net::Socket & socket_, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE);

	bool poll(size_t timeout_microseconds);
};

}
