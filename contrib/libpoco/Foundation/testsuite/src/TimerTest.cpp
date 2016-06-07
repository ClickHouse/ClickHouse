//
// TimerTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/TimerTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "TimerTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Thread.h"
#include "Poco/Stopwatch.h"


using Poco::Timer;
using Poco::TimerCallback;
using Poco::Thread;
using Poco::Stopwatch;


TimerTest::TimerTest(const std::string& name): CppUnit::TestCase(name)
{
}


TimerTest::~TimerTest()
{
}


void TimerTest::testTimer()
{
	Timer t(100, 200);
	assert (t.getStartInterval() == 100);
	assert (t.getPeriodicInterval() == 200);

	Stopwatch sw;
	TimerCallback<TimerTest> tc(*this, &TimerTest::onTimer);
	sw.start();
	t.start(tc);
	_event.wait();
	sw.stop();
	assert (sw.elapsed() >= 80000 && sw.elapsed() < 120000);
	sw.restart();
	_event.wait();
	sw.stop();
	assert (sw.elapsed() >= 180000 && sw.elapsed() < 250000);
	sw.restart();
	_event.wait();
	sw.stop();
	assert (sw.elapsed() >= 180000 && sw.elapsed() < 250000);
	t.stop();	
}


void TimerTest::testDuplicateStop()
{
	Timer t(100, 200);
	t.stop();
	t.stop();

	TimerCallback<TimerTest> tc(*this, &TimerTest::onTimer);
	t.start(tc);
	_event.wait();
	t.stop();
	t.stop();
}

void TimerTest::setUp()
{
}


void TimerTest::tearDown()
{
}


void TimerTest::onTimer(Timer& timer)
{
	_event.set();
}


CppUnit::Test* TimerTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("TimerTest");

	CppUnit_addTest(pSuite, TimerTest, testTimer);
	CppUnit_addTest(pSuite, TimerTest, testDuplicateStop);

	return pSuite;
}
