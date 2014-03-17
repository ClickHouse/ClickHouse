#include <Poco/Net/NetException.h>

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#include <DB/IO/WriteBufferFromPocoSocket.h>


namespace DB
{

void WriteBufferFromPocoSocket::nextImpl()
{
	if (!offset())
		return;

	size_t bytes_written = 0;
	while (bytes_written < offset())
	{
		ssize_t res = 0;

		/// Добавляем в эксепшены более подробную информацию.
		try
		{
			res = socket.impl()->sendBytes(working_buffer.begin() + bytes_written, offset() - bytes_written);
		}
		catch (const Poco::Net::NetException & e)
		{
			throw Exception(e.displayText() + " while writing to socket (" + peer_address.toString() + ")", ErrorCodes::NETWORK_ERROR);
		}
		catch (const Poco::TimeoutException & e)
		{
			throw Exception("Timeout exceeded while writing to socket (" + peer_address.toString() + ")", ErrorCodes::SOCKET_TIMEOUT);
		}
		catch (const Poco::IOException & e)
		{
			throw Exception(e.displayText(), " while reading from socket (" + peer_address.toString() + ")", ErrorCodes::NETWORK_ERROR);
		}

		if (res < 0)
			throw Exception("Cannot write to socket (" + peer_address.toString() + ")", ErrorCodes::CANNOT_WRITE_TO_SOCKET);
		bytes_written += res;
	}
}

WriteBufferFromPocoSocket::WriteBufferFromPocoSocket(Poco::Net::Socket & socket_, size_t buf_size)
	: BufferWithOwnMemory<WriteBuffer>(buf_size), socket(socket_), peer_address(socket.peerAddress())
{
}

WriteBufferFromPocoSocket::~WriteBufferFromPocoSocket()
{
	try
	{
		next();
	}
	catch (...)
	{
		tryLogCurrentException(__PRETTY_FUNCTION__);
	}
}

}
