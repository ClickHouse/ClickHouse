//
// RedisTestSuite.cpp
//
// Copyright (c) 2015, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "RedisTestSuite.h"
#include "RedisTest.h"


CppUnit::Test* RedisTestSuite::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("RedisTestSuite");

	pSuite->addTest(RedisTest::suite());

	return pSuite;
}
