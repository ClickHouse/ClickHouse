//
// TimezoneTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/TimezoneTest.h#1 $
//
// Definition of the TimezoneTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef TimezoneTest_INCLUDED
#define TimezoneTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class TimezoneTest: public CppUnit::TestCase
{
public:
	TimezoneTest(const std::string& name);
	~TimezoneTest();

	void testTimezone();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // TimezoneTest_INCLUDED
