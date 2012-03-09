#pragma once

#include <Poco/Net/Socket.h>

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

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
	
	bool nextImpl()
	{
		ssize_t bytes_read = socket.impl()->receiveBytes(internal_buffer.begin(), internal_buffer.size());
		if (bytes_read < 0)
			throw Exception("Cannot read from socket", ErrorCodes::CANNOT_READ_FROM_SOCKET);

		if (bytes_read)
			working_buffer.resize(bytes_read);
		else
			return false;

		return true;
	}

public:
	ReadBufferFromPocoSocket(Poco::Net::Socket & socket_, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE)
		: BufferWithOwnMemory<ReadBuffer>(buf_size), socket(socket_) {}
};

}
