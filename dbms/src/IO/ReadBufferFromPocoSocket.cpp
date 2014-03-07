#include <Poco/Net/NetException.h>

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#include <DB/IO/ReadBufferFromPocoSocket.h>


namespace DB
{

bool ReadBufferFromPocoSocket::nextImpl()
{
	ssize_t bytes_read = 0;

	/// Добавляем в эксепшены более подробную информацию.
	try
	{
		bytes_read = socket.impl()->receiveBytes(internal_buffer.begin(), internal_buffer.size());
	}
	catch (const Poco::Net::NetException & e)
	{
		throw Exception(e.displayText(), "while reading from socket (" + peer_address.toString() + ")", ErrorCodes::NETWORK_ERROR);
	}
	catch (const Poco::TimeoutException & e)
	{
		throw Exception("Timeout exceeded while reading from socket (" + peer_address.toString() + ")", ErrorCodes::SOCKET_TIMEOUT);
	}
	catch (const Poco::IOException & e)
	{
		throw Exception(e.displayText(), "while reading from socket (" + peer_address.toString() + ")", ErrorCodes::NETWORK_ERROR);
	}

	if (bytes_read < 0)
		throw Exception("Cannot read from socket (" + peer_address.toString() + ")", ErrorCodes::CANNOT_READ_FROM_SOCKET);

	if (bytes_read)
		working_buffer.resize(bytes_read);
	else
		return false;

	return true;
}

ReadBufferFromPocoSocket::ReadBufferFromPocoSocket(Poco::Net::Socket & socket_, size_t buf_size)
	: BufferWithOwnMemory<ReadBuffer>(buf_size), socket(socket_), peer_address(socket.peerAddress())
{
}

bool ReadBufferFromPocoSocket::poll(size_t timeout_microseconds)
{
	return offset() != buffer().size() || socket.poll(timeout_microseconds, Poco::Net::Socket::SELECT_READ | Poco::Net::Socket::SELECT_ERROR);
}

}
