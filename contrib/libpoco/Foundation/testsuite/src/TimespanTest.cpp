//
// TimespanTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/TimespanTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "TimespanTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Timespan.h"


using Poco::Timespan;


TimespanTest::TimespanTest(const std::string& name): CppUnit::TestCase(name)
{
}


TimespanTest::~TimespanTest()
{
}


void TimespanTest::testConversions()
{
	Timespan ts;
	assert (ts.totalMicroseconds() == 0);
	ts = Timespan::DAYS;
	assert (ts.totalMicroseconds() == Timespan::DAYS);
	assert (ts.totalMilliseconds() == 86400000);
	assert (ts.totalSeconds() == 86400);
	assert (ts.totalMinutes() == 60*24);
	assert (ts.totalHours() == 24);
	assert (ts.days() == 1);
	
	assert (ts.microseconds() == 0);
	assert (ts.milliseconds() == 0);
	assert (ts.seconds() == 0);
	assert (ts.minutes() == 0);
	assert (ts.hours() == 0);
	
	ts.assign(2, 12, 30, 10, 123456);
	assert (ts.microseconds() == 456);
	assert (ts.milliseconds() == 123);
	assert (ts.seconds() == 10);
	assert (ts.minutes() == 30);
	assert (ts.hours() == 12);
	assert (ts.days() == 2);

	ts.assign(0, 36, 30, 10, 123456);
	assert (ts.microseconds() == 456);
	assert (ts.milliseconds() == 123);
	assert (ts.useconds() == 123456);
	assert (ts.seconds() == 10);
	assert (ts.minutes() == 30);
	assert (ts.hours() == 12);
	assert (ts.days() == 1);

	ts.assign(0, 0, 2190, 10, 123456);
	assert (ts.microseconds() == 456);
	assert (ts.milliseconds() == 123);
	assert (ts.useconds() == 123456);
	assert (ts.seconds() == 10);
	assert (ts.minutes() == 30);
	assert (ts.hours() == 12);
	assert (ts.days() == 1);
}


void TimespanTest::testComparisons()
{
	Timespan ts1(10000000);
	Timespan ts2(20000000);
	Timespan ts3(20000000);
	
	assert (ts1 != ts2);
	assert (!(ts1 == ts2));
	assert (ts1 <= ts2);
	assert (ts1 < ts2);
	assert (ts2 > ts1);
	assert (ts2 >= ts1);
	
	assert (ts2 == ts3);
	assert (!(ts2 != ts3));
	assert (ts2 >= ts3);
	assert (ts2 <= ts3);
	assert (!(ts2 > ts3));
	assert (!(ts2 < ts3));
	
	assert (ts1 == 10000000);
	assert (ts1 != 20000000);
	assert (ts1 <= 10000000);
	assert (ts1 <= 20000000);
	assert (ts1 >= 10000000);
	assert (ts1 >= 5000000);
	assert (ts1 < 20000000);
	assert (ts1 > 5000000);
}


void TimespanTest::testArithmetics()
{
	Timespan ts1(100000000);
	Timespan ts2(50000000);
	Timespan ts3;
	ts3 = ts1 + ts2;
	assert (ts3 == 150000000);
	ts3 = ts1 + 30000000;
	assert (ts3 == 130000000);
	ts3 = ts1 - ts2;
	assert (ts3 == 50000000);
	ts3 = ts1 - 20000000;
	assert (ts3 == 80000000);
	ts3 += 20000000;
	assert (ts3 == ts1);
	ts3 -= ts2;
	assert (ts3 == ts2);
}


void TimespanTest::testSwap()
{
	Timespan ts1(10000000);
	Timespan ts2(50000000);
	
	assert (ts1 < ts2);
	ts1.swap(ts2);
	assert (ts2 < ts1);
}


void TimespanTest::setUp()
{
}


void TimespanTest::tearDown()
{
}


CppUnit::Test* TimespanTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("TimespanTest");

	CppUnit_addTest(pSuite, TimespanTest, testConversions);
	CppUnit_addTest(pSuite, TimespanTest, testComparisons);
	CppUnit_addTest(pSuite, TimespanTest, testArithmetics);
	CppUnit_addTest(pSuite, TimespanTest, testSwap);

	return pSuite;
}
