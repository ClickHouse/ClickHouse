//
// LoggingRegistryTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/LoggingRegistryTest.h#1 $
//
// Definition of the LoggingRegistryTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef LoggingRegistryTest_INCLUDED
#define LoggingRegistryTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class LoggingRegistryTest: public CppUnit::TestCase
{
public:
	LoggingRegistryTest(const std::string& name);
	~LoggingRegistryTest();

	void testRegister();
	void testReregister();
	void testUnregister();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // LoggingRegistryTest_INCLUDED
