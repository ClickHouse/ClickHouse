//
// TimestampTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/TimestampTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "TimestampTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Timestamp.h"
#include "Poco/Thread.h"


using Poco::Timestamp;
using Poco::Thread;


TimestampTest::TimestampTest(const std::string& name): CppUnit::TestCase(name)
{
}


TimestampTest::~TimestampTest()
{
}


void TimestampTest::testTimestamp()
{
	Timestamp t1;
	Thread::sleep(200);
	Timestamp t2;
	Timestamp t3 = t2;
	assert (t1 != t2);
	assert (!(t1 == t2));
	assert (t2 > t1);
	assert (t2 >= t1);
	assert (!(t1 > t2));
	assert (!(t1 >= t2));
	assert (t2 == t3);
	assert (!(t2 != t3));
	assert (t2 >= t3);
	assert (t2 <= t3);
	Timestamp::TimeDiff d = (t2 - t1);
	assert (d >= 180000 && d <= 300000);
	
	t1.swap(t2);
	assert (t1 > t2);
	t2.swap(t1);
	
	Timestamp::UtcTimeVal tv = t1.utcTime();
	Timestamp t4 = Timestamp::fromUtcTime(tv);
	assert (t1 == t4);
	
	Timestamp epoch(0);
	tv = epoch.utcTime();
	assert (tv >> 32 == 0x01B21DD2);
	assert ((tv & 0xFFFFFFFF) == 0x13814000);
	
	Timestamp now;
	Thread::sleep(201);
	assert (now.elapsed() >= 200000);
	assert (now.isElapsed(200000));
	assert (!now.isElapsed(2000000));
	
#if defined(_WIN32)
	{
		Timestamp now;
		Poco::UInt32 low;
		Poco::UInt32 high;
		now.toFileTimeNP(low, high);
		Timestamp ts = Timestamp::fromFileTimeNP(low, high);
		assert (now == ts);
	}
#endif
}


void TimestampTest::setUp()
{
}


void TimestampTest::tearDown()
{
}


CppUnit::Test* TimestampTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("TimestampTest");

	CppUnit_addTest(pSuite, TimestampTest, testTimestamp);

	return pSuite;
}
