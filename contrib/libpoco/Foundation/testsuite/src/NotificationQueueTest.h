//
// NotificationQueueTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/NotificationQueueTest.h#1 $
//
// Definition of the NotificationQueueTest class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef NotificationQueueTest_INCLUDED
#define NotificationQueueTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"
#include "Poco/NotificationQueue.h"
#include "Poco/Mutex.h"
#include <set>


class NotificationQueueTest: public CppUnit::TestCase
{
public:
	NotificationQueueTest(const std::string& name);
	~NotificationQueueTest();

	void testQueueDequeue();
	void testQueueDequeueUrgent();
	void testWaitDequeue();
	void testThreads();
	void testDefaultQueue();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

protected:
	void work();

private:
	Poco::NotificationQueue    _queue;
	std::multiset<std::string> _handled;
	Poco::FastMutex            _mutex;
};


#endif // NotificationQueueTest_INCLUDED
