//
// RedisStream.h
//
// Library: Redis
// Package: Redis
// Module:  RedisStream
//
// Definition of the RedisStream class.
//
// Copyright (c) 2015, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Redis_RedisStream_INCLUDED
#define Redis_RedisStream_INCLUDED


#include "Poco/Redis/Redis.h"
#include "Poco/BufferedStreamBuf.h"
#include "Poco/Net/StreamSocket.h"
#include <istream>
#include <ostream>


namespace Poco {
namespace Redis {


class RedisStreamBuf: public BufferedStreamBuf
	/// BufferedStreamBuf for Redis.
{
public:
	RedisStreamBuf(Net::StreamSocket& redis);
		/// Constructor

	~RedisStreamBuf();
		/// Destructor

	std::string readLine();
		/// Reads a line from Redis (until \r\n is encountered).

protected:
	int readFromDevice(char* buffer, std::streamsize length);
	int writeToDevice(const char* buffer, std::streamsize length);

private:
	enum
	{
		STREAM_BUFFER_SIZE = 1024
	};

	Net::StreamSocket& _redis;
};


class RedisIOS: public virtual std::ios
{
public:
	RedisIOS(Net::StreamSocket& redis);
		/// Creates the RedisIOS with the given socket.

	~RedisIOS();
		/// Destroys the RedisIOS.
		///
		/// Flushes the buffer, but does not close the socket.

	RedisStreamBuf* rdbuf();
		/// Returns a pointer to the internal RedisStreamBuf.

	void close();
		/// Flushes the stream.

protected:
	RedisStreamBuf _buf;
};


class Redis_API RedisOutputStream: public RedisIOS, public std::ostream
	/// An output stream for writing to a Redis server.
{
public:
	RedisOutputStream(Net::StreamSocket& redis);
		/// Creates the RedisOutputStream with the given socket.

	~RedisOutputStream();
		/// Destroys the RedisOutputStream.
		///
		/// Flushes the buffer.
};


class Redis_API RedisInputStream: public RedisIOS, public std::istream
	/// An input stream for reading from a Redis server.
{
public:
	RedisInputStream(Net::StreamSocket& redis);
		/// Creates the RedisInputStream with the given socket.

	~RedisInputStream();
		/// Destroys the RedisInputStream.

	std::string getline();
		/// Redis uses \r\n as delimiter. This getline version removes
		/// the \r from the result.
};


} } // namespace Poco::Redis


#endif // Redis_RedisStream_INCLUDED
