//
// AutoReleasePoolTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/AutoReleasePoolTest.h#1 $
//
// Definition of the AutoReleasePoolTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef AutoReleasePoolTest_INCLUDED
#define AutoReleasePoolTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class AutoReleasePoolTest: public CppUnit::TestCase
{
public:
	AutoReleasePoolTest(const std::string& name);
	~AutoReleasePoolTest();

	void testAutoReleasePool();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // AutoReleasePoolTest_INCLUDED
