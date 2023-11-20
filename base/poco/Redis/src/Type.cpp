//
// Type.h
//
// Library: Redis
// Package: Redis
// Module:  Type
//
// Implementation of the Type class.
//
// Copyright (c) 2015, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Redis/Type.h"
#include "Poco/Redis/Error.h"
#include "Poco/Redis/Array.h"


namespace Poco {
namespace Redis {


RedisType::RedisType()
{
}


RedisType::~RedisType()
{
}


RedisType::Ptr RedisType::createRedisType(char marker)
{
	RedisType::Ptr result;

	switch(marker)
	{
	case RedisTypeTraits<Int64>::marker :
		result = new Type<Int64>();
		break;
	case RedisTypeTraits<std::string>::marker :
		result = new Type<std::string>();
		break;
	case RedisTypeTraits<BulkString>::marker :
		result = new Type<BulkString>();
		break;
	case RedisTypeTraits<Array>::marker :
		result = new Type<Array>();
		break;
	case RedisTypeTraits<Error>::marker :
		result = new Type<Error>();
		break;
	}
	return result;
}


} } // namespace Poco::Redis
