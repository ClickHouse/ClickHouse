//
// DateTimeTestSuite.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/DateTimeTestSuite.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DateTimeTestSuite.h"
#include "TimestampTest.h"
#include "ClockTest.h"
#include "TimespanTest.h"
#include "TimezoneTest.h"
#include "DateTimeTest.h"
#include "LocalDateTimeTest.h"
#include "DateTimeFormatterTest.h"
#include "DateTimeParserTest.h"


CppUnit::Test* DateTimeTestSuite::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("DateTimeTestSuite");

	pSuite->addTest(TimestampTest::suite());
	pSuite->addTest(ClockTest::suite());
	pSuite->addTest(TimespanTest::suite());
	pSuite->addTest(TimezoneTest::suite());
	pSuite->addTest(DateTimeTest::suite());
	pSuite->addTest(LocalDateTimeTest::suite());
	pSuite->addTest(DateTimeFormatterTest::suite());
	pSuite->addTest(DateTimeParserTest::suite());

	return pSuite;
}
