//
// LoggerTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/LoggerTest.h#1 $
//
// Definition of the LoggerTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef LoggerTest_INCLUDED
#define LoggerTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class LoggerTest: public CppUnit::TestCase
{
public:
	LoggerTest(const std::string& name);
	~LoggerTest();

	void testLogger();
	void testFormat();
	void testFormatAny();
	void testDump();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // LoggerTest_INCLUDED
