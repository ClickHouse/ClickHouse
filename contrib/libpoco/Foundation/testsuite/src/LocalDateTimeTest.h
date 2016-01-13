//
// LocalDateTimeTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/LocalDateTimeTest.h#1 $
//
// Definition of the LocalDateTimeTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef LocalDateTimeTest_INCLUDED
#define LocalDateTimeTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"


class LocalDateTimeTest: public CppUnit::TestCase
{
public:
	LocalDateTimeTest(const std::string& name);
	~LocalDateTimeTest();

	void testGregorian1();
	void testGregorian2();
	void testConversions();
	void testCalcs();
	void testAMPM();
	void testRelational1();
	void testRelational2();
	void testArithmetics1();
	void testArithmetics2();
	void testSwap();
	void testTimezone();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

private:
};


#endif // LocalDateTimeTest_INCLUDED
