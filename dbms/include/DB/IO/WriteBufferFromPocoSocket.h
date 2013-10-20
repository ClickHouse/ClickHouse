#pragma once

#include <Poco/Net/Socket.h>
#include <Poco/Net/NetException.h>

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

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
	
	void nextImpl()
	{
		if (!offset())
			return;

		size_t bytes_written = 0;
		while (bytes_written != offset())
		{
			ssize_t res = 0;

			/// Добавляем в эксепшены более подробную информацию.
			try
			{
				res = socket.impl()->sendBytes(working_buffer.begin() + bytes_written, offset() - bytes_written);
			}
			catch (Poco::Net::NetException & e)
			{
				throw Exception(e.displayText() + " while writing to socket (" + socket.address().toString() + ")", ErrorCodes::NETWORK_ERROR);
			}
			catch (Poco::TimeoutException & e)
			{
				throw Exception("Timeout exceeded while writing to socket (" + socket.address().toString() + ")", ErrorCodes::SOCKET_TIMEOUT);
			}
			
			if (res < 0)
				throw Exception("Cannot write to socket", ErrorCodes::CANNOT_WRITE_TO_SOCKET);
			bytes_written += res;
		}
	}

public:
	WriteBufferFromPocoSocket(Poco::Net::Socket & socket_, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE)
		: BufferWithOwnMemory<WriteBuffer>(buf_size), socket(socket_) {}

    ~WriteBufferFromPocoSocket()
	{
		if (!std::uncaught_exception())
			next();
	}
};

}
