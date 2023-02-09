//
// AsyncReader.h
//
// Library: Redis
// Package: Redis
// Module:  AsyncReader
//
// Definition of the AsyncReader class.
//
// Copyright (c) 2015, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Redis_AsyncReader_INCLUDED
#define Redis_AsyncReader_INCLUDED


#include "Poco/Redis/Redis.h"
#include "Poco/Redis/Client.h"
#include "Poco/Redis/RedisEventArgs.h"
#include "Poco/Activity.h"


namespace Poco {
namespace Redis {


class Redis_API AsyncReader
	/// Wrapper around a Redis client to read messages asynchronously. Use this
	/// for publish/subscribe. The redisResponse event is used to notify that
	/// a message is received. When a reader is started for a Redis server,
	/// you should use execute<void>, because this class is responsible for
	/// reading all replies.
{
public:
	BasicEvent<RedisEventArgs> redisResponse;
		/// Event that is fired when a message is received.

	BasicEvent<RedisEventArgs> redisException;
		/// Event that is fired when an error occurred.

	AsyncReader(Client& client);
		/// Creates the AsyncReader using the given Client.

	virtual ~AsyncReader();
		/// Destroys the AsyncReader.

	bool isStopped();
		/// Returns true if the activity is not running, false when it is.

	void start();
		/// Starts the activity to read replies from the Redis server.

	void stop();
		/// Stops the read activity.

protected:
	void runActivity();

private:
	AsyncReader(const AsyncReader&);
	AsyncReader& operator = (const AsyncReader&);

	Client& _client;
	Activity<AsyncReader> _activity;
};


//
// inlines
//


inline bool AsyncReader::isStopped()
{
	return _activity.isStopped();
}


inline void AsyncReader::start()
{
	_activity.start();
}


inline void AsyncReader::stop()
{
	_activity.stop();
}


} } // namespace Poco::Redis


#endif //Redis_AsyncReader_INCLUDED
