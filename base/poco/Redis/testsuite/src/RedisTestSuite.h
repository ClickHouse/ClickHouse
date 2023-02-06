//
// RedisTestSuite.h
//
// Definition of the RedisTestSuite class.
//
// Copyright (c) 2015, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef RedisTestSuite_INCLUDED
#define RedisTestSuite_INCLUDED


#include "CppUnit/TestSuite.h"


class RedisTestSuite
{
public:
	static CppUnit::Test* suite();
};


#endif // RedisTestSuite_INCLUDED
