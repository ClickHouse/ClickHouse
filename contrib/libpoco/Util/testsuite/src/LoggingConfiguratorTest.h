//
// LoggingConfiguratorTest.h
//
// $Id: //poco/1.4/Util/testsuite/src/LoggingConfiguratorTest.h#1 $
//
// Definition of the LoggingConfiguratorTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef LoggingConfiguratorTest_INCLUDED
#define LoggingConfiguratorTest_INCLUDED


#include "Poco/Util/Util.h"
#include "CppUnit/TestCase.h"


class LoggingConfiguratorTest: public CppUnit::TestCase
{
public:
	LoggingConfiguratorTest(const std::string& name);
	~LoggingConfiguratorTest();

	void testConfigurator();
	void testBadConfiguration1();
	void testBadConfiguration2();
	void testBadConfiguration3();
	void testBadConfiguration4();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // LoggingConfiguratorTest_INCLUDED
