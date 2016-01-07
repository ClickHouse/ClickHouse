//
// ObjectPoolTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/ObjectPoolTest.h#1 $
//
// Definition of the ObjectPoolTest class.
//
// Copyright (c) 2010-2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef ObjectPoolTest_INCLUDED
#define ObjectPoolTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class ObjectPoolTest: public CppUnit::TestCase
{
public:
	ObjectPoolTest(const std::string& name);
	~ObjectPoolTest();

	void testObjectPool();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // ObjectPoolTest_INCLUDED
