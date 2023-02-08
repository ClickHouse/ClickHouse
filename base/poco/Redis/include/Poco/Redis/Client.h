//
// Client.h
//
// Library: Redis
// Package: Redis
// Module:  Client
//
// Definition of the Client class.
//
// Copyright (c) 2015, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Redis_Client_INCLUDED
#define Redis_Client_INCLUDED


#include "Poco/Redis/Redis.h"
#include "Poco/Redis/Array.h"
#include "Poco/Redis/Error.h"
#include "Poco/Redis/RedisStream.h"
#include "Poco/Net/SocketAddress.h"
#include "Poco/Timespan.h"


namespace Poco {
namespace Redis {


class Redis_API Client
	/// Represents a connection to a Redis server.
	///
	/// A command is always made from an Array and a reply can be a signed 64
	/// bit integer, a simple string, a bulk string, an array or an error. The
	/// first element of the command array is the Redis command. A simple string
	/// is a string that cannot contain a CR or LF character. A bulk string is
	/// implemented as a typedef for Poco::Nullable<std::string>. This is
	/// because a bulk string can represent a Null value.
	///
	///     BulkString bs = client.execute<BulkString>(...);
	///     if ( bs.isNull() )
	///     {
	///        // We have a Null value
	///     }
	///     else
	///     {
	///        // We have a string value
	///     }
	///
	/// To create Redis commands, the factory methods of the Command class can
	/// be used or the Array class can be used directly.
	///
	///     Command llen = Command::llen("list");
	///
	/// is the same as:
	///
	///     Array command;
	///     command.add("LLEN").add("list");
	///
	/// or:
	///
	///     Array command;
	///     command << "LLEN" << "list";
	///
	///	or even:
	///
	///     Command command("LLEN");
	///     command << "list";
{
public:
	typedef SharedPtr<Client> Ptr;

	Client();
		/// Creates an unconnected Client.
		/// Use this when you want to connect later on.

	Client(const std::string& hostAndPort);
		/// Constructor which connects to the given Redis host/port.
		/// The host and port must be separated with a colon.

	Client(const std::string& host, int port);
		/// Constructor which connects to the given Redis host/port.

	Client(const Net::SocketAddress& addrs);
		/// Constructor which connects to the given Redis host/port.

	virtual ~Client();
		/// Destroys the Client.

	Net::SocketAddress address() const;
		/// Returns the address of the Redis connection.

	void connect(const std::string& hostAndPort);
		/// Connects to the given Redis server. The host and port must be
		/// separated with a colon.

	void connect(const std::string& host, int port);
		/// Connects to the given Redis server.

	void connect(const Net::SocketAddress& addrs);
		/// Connects to the given Redis server.

	void connect(const std::string& hostAndPort, const Timespan& timeout);
		/// Connects to the given Redis server. The host and port must be
		/// separated with a colon.

	void connect(const std::string& host, int port, const Timespan& timeout);
		/// Connects to the given Redis server.

	void connect(const Net::SocketAddress& addrs, const Timespan& timeout);
		/// Connects to the given Redis server.

	void disconnect();
		/// Disconnects from the Redis server.

	bool isConnected() const;
		/// Returns true iff the Client is connected to a Redis server.

	template<typename T>
	T execute(const Array& command)
		/// Sends the Redis Command to the server. It gets the reply
		/// and tries to convert it to the given template type.
		///
		/// A specialization exists for type void, which doesn't read
		/// the reply. If the server sends a reply, it is your
		/// responsibility to read it. Use this for pipelining.
		///
		/// A Poco::BadCastException will be thrown when the reply couldn't be
		/// converted. Supported types are Int64, std::string, BulkString,
		/// Array and void. When the reply is an Error, it will throw
		/// a RedisException.
	{
		T result = T();
		writeCommand(command, true);
		readReply(result);
		return result;
	}

	void flush();
		/// Flush the output buffer to Redis. Use this when commands
		/// are stored in the buffer to send them all at once to Redis.

	RedisType::Ptr sendCommand(const Array& command);
		/// Sends a Redis command to the server and returns the reply.
		/// Use this when the type of the reply isn't known.

	RedisType::Ptr readReply();
		/// Read a reply from the Redis server.

	template<typename T>
	void readReply(T& result)
		/// Read a reply from the Redis server and tries to convert that reply
		/// to the template type. When the reply is a Redis error, it will
		/// throw a RedisException. A BadCastException will be thrown, when
		/// the reply is not of the given type.
	{
		RedisType::Ptr redisResult = readReply();
		if (redisResult->type() == RedisTypeTraits<Error>::TypeId)
		{
			Type<Error>* error = dynamic_cast<Type<Error>*>(redisResult.get());
			throw RedisException(error->value().getMessage());
		}

		if (redisResult->type() == RedisTypeTraits<T>::TypeId)
		{
			Type<T>* type = dynamic_cast<Type<T>*>(redisResult.get());
			if (type != NULL) result = type->value();
		}
		else throw BadCastException();
	}

	Array sendCommands(const std::vector<Array>& commands);
		/// Sends all commands (pipelining) to the Redis server before
		/// getting all replies.

	void setReceiveTimeout(const Timespan& timeout);
		/// Sets a receive timeout.

private:
	Client(const Client&);
	Client& operator = (const Client&);

	void connect();
		/// Connects to the Redis server

	void connect(const Timespan& timeout);
		/// Connects to the Redis server and sets a timeout.

	void writeCommand(const Array& command, bool flush);
		/// Sends a request to the Redis server. Use readReply to get the
		/// answer. Can also be used for pipelining commands. Make sure you
		/// call readReply as many times as you called writeCommand, even when
		/// an error occurred on a command.

	Net::SocketAddress _address;
	Net::StreamSocket _socket;
	RedisInputStream* _input;
	RedisOutputStream* _output;
};


//
// inlines
//


inline Net::SocketAddress Client::address() const
{
	return _address;
}


template<> inline
void Client::execute<void>(const Array& command)
{
	writeCommand(command, false);
}


inline void Client::flush()
{
	poco_assert(_output);
	_output->flush();
}


inline void Client::setReceiveTimeout(const Timespan& timeout)
{
	_socket.setReceiveTimeout(timeout);
}


} } // namespace Poco::Redis


#endif // Redis_Client_INCLUDED
