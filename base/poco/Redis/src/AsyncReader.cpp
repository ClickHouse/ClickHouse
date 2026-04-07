//
// AsyncReader.cpp
//
// Library: Redis
// Package: Redis
// Module:  AsyncReader
//
// Implementation of the AsyncReader class.
//
// Copyright (c) 2015, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Redis/AsyncReader.h"


namespace Poco {
namespace Redis {


AsyncReader::AsyncReader(Client& client): 
	_client(client),
	_activity(this, &AsyncReader::runActivity)
{
}


AsyncReader::~AsyncReader()
{
	stop();
}


void AsyncReader::runActivity()
{
	while (!_activity.isStopped())
	{
		try
		{
			RedisType::Ptr reply = _client.readReply();

			RedisEventArgs args(reply);
			redisResponse.notify(this, args);

			if ( args.isStopped() ) stop();
		}
		catch (Exception& e)
		{
			RedisEventArgs args(&e);
			redisException.notify(this, args);
			stop();
		}
		if (!_activity.isStopped()) Thread::trySleep(100);
	}
}


} } // namespace Poco::Redis
