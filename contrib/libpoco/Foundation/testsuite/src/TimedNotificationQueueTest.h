//
// TimedNotificationQueueTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/TimedNotificationQueueTest.h#1 $
//
// Definition of the TimedNotificationQueueTest class.
//
// Copyright (c) 2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef TimedNotificationQueueTest_INCLUDED
#define TimedNotificationQueueTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"
#include "Poco/TimedNotificationQueue.h"
#include "Poco/Mutex.h"
#include <set>


class TimedNotificationQueueTest: public CppUnit::TestCase
{
public:
	TimedNotificationQueueTest(const std::string& name);
	~TimedNotificationQueueTest();

	void testDequeue();
	void testWaitDequeue();
	void testWaitDequeueTimeout();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

protected:
	void work();

private:
};


#endif // TimedNotificationQueueTest_INCLUDED
