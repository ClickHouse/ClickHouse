//
// ProcessTest.h
//
// Definition of the ProcessTest class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef ProcessTest_INCLUDED
#define ProcessTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class ProcessTest: public CppUnit::TestCase
{
public:
	ProcessTest(const std::string& name);
	~ProcessTest();

	void testLaunch();
	void testLaunchRedirectIn();
	void testLaunchRedirectOut();
	void testLaunchEnv();
	void testLaunchArgs();
	void testIsRunning();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // ProcessTest_INCLUDED
