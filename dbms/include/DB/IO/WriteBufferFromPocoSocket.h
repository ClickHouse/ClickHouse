#pragma once

#include <Poco/Net/Socket.h>

#include <DB/IO/WriteBuffer.h>
#include <DB/IO/BufferWithOwnMemory.h>


namespace DB
{

/** Работает с готовым Poco::Net::Socket. Операции блокирующие.
  */
class WriteBufferFromPocoSocket : public BufferWithOwnMemory<WriteBuffer>
{
protected:
	Poco::Net::Socket & socket;

	/** Для сообщений об ошибках. Нужно получать этот адрес заранее, так как,
	  *  например, если соединение разорвано, то адрес уже будет получить нельзя
	  *  (getpeername вернёт ошибку).
	  */
	Poco::Net::SocketAddress peer_address;


	void nextImpl() override;

public:
	WriteBufferFromPocoSocket(Poco::Net::Socket & socket_, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE);

    ~WriteBufferFromPocoSocket();
};

}
