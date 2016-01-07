//
// LoggingFactoryTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/LoggingFactoryTest.h#1 $
//
// Definition of the LoggingFactoryTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef LoggingFactoryTest_INCLUDED
#define LoggingFactoryTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class LoggingFactoryTest: public CppUnit::TestCase
{
public:
	LoggingFactoryTest(const std::string& name);
	~LoggingFactoryTest();

	void testBuiltins();
	void testCustom();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // LoggingFactoryTest_INCLUDED
