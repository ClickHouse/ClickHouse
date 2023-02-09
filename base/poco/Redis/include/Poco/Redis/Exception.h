//
// Exception.h
//
// Library: Redis
// Package: Redis
// Module:  Exception
//
// Definition of the Exception class.
//
// Copyright (c) 2015, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Redis_Exception_INCLUDED
#define Redis_Exception_INCLUDED


#include "Poco/Redis/Redis.h"
#include "Poco/Exception.h"
#include <typeinfo>


namespace Poco {
namespace Redis {


POCO_DECLARE_EXCEPTION(Redis_API, RedisException, Exception)


} } // namespace Poco::Redis


#endif // Redis_Exception_INCLUDED
