//
// RedisEventArgs.h
//
// Library: Redis
// Package: Redis
// Module:  RedisEventArgs
//
// Definition of the RedisEventArgs class.
//
// Copyright (c) 2015, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Redis_RedisEventArgs_INCLUDED
#define Redis_RedisEventArgs_INCLUDED


#include "Poco/Redis/Type.h"


namespace Poco {
namespace Redis {


class Redis_API RedisEventArgs
	/// Event arguments for AsyncReader events.
{
public:
	RedisEventArgs(RedisType::Ptr message);
		/// Creates the RedisEventArgs from the given message.

	RedisEventArgs(Exception* e);
		/// Creates the RedisEventArgs from the given Redis Exception.

	~RedisEventArgs();
		/// Destroys the RedisEventArgs.

	RedisType::Ptr message() const;
		/// Returns the message retrieved from the Redis server.
		/// This can be a NULL pointer when this event is about an exception.

	const Exception* exception() const;
		/// Returns the exception if any, otherwise it returns null pointer.

	void stop();
		/// When called, the AsyncReader will stop.
		///
		/// Note: The AsyncReader will always stop when this is an exception
		/// event. Use this for example for pub/sub when there are no
		/// subcribers anymore.

	bool isStopped() const;
		/// Returns true when the AsyncReader will stop.

private:
	RedisType::Ptr _message;

	Exception* _exception;

	bool _stop;
};


//
// inlines
//


inline RedisType::Ptr RedisEventArgs::message() const
{
	return _message;
}


inline const Exception* RedisEventArgs::exception() const
{
	return _exception;
}


inline bool RedisEventArgs::isStopped() const
{
	return _stop;
}


inline void RedisEventArgs::stop()
{
	_stop = true;
}


} } // namespace Poco::Redis


#endif // Redis_RedisEventArgs_INCLUDED
