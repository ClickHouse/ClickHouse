//
// ThreadingTestSuite.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/ThreadingTestSuite.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "ThreadingTestSuite.h"
#include "ThreadTest.h"
#include "SemaphoreTest.h"
#include "RWLockTest.h"
#include "ThreadPoolTest.h"
#include "TimerTest.h"
#include "ThreadLocalTest.h"
#include "ActivityTest.h"
#include "ActiveMethodTest.h"
#include "ActiveDispatcherTest.h"
#include "ConditionTest.h"


CppUnit::Test* ThreadingTestSuite::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("ThreadingTestSuite");

	pSuite->addTest(ThreadTest::suite());
	pSuite->addTest(SemaphoreTest::suite());
	pSuite->addTest(RWLockTest::suite());
	pSuite->addTest(ThreadPoolTest::suite());
	pSuite->addTest(TimerTest::suite());
	pSuite->addTest(ThreadLocalTest::suite());
	pSuite->addTest(ActivityTest::suite());
	pSuite->addTest(ActiveMethodTest::suite());
	pSuite->addTest(ActiveDispatcherTest::suite());
	pSuite->addTest(ConditionTest::suite());

	return pSuite;
}
