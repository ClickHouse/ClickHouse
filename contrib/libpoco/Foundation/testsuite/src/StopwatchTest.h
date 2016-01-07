//
// StopwatchTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/StopwatchTest.h#1 $
//
// Definition of the StopwatchTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef StopwatchTest_INCLUDED
#define StopwatchTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class StopwatchTest: public CppUnit::TestCase
{
public:
	StopwatchTest(const std::string& name);
	~StopwatchTest();

	void testStopwatch();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // StopwatchTest_INCLUDED
