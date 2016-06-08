//
// PriorityNotificationQueueTest.h
//
// $Id: //poco/1.4/Foundation/testsuite/src/PriorityNotificationQueueTest.h#1 $
//
// Definition of the PriorityNotificationQueueTest class.
//
// Copyright (c) 2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef PriorityNotificationQueueTest_INCLUDED
#define PriorityNotificationQueueTest_INCLUDED


#include "Poco/Foundation.h"
#include "CppUnit/TestCase.h"
#include "Poco/PriorityNotificationQueue.h"
#include "Poco/Mutex.h"
#include <set>


class PriorityNotificationQueueTest: public CppUnit::TestCase
{
public:
	PriorityNotificationQueueTest(const std::string& name);
	~PriorityNotificationQueueTest();

	void testQueueDequeue();
	void testWaitDequeue();
	void testThreads();
	void testDefaultQueue();

	void setUp();
	void tearDown();

	static CppUnit::Test* suite();

protected:
	void work();

private:
	Poco::PriorityNotificationQueue    _queue;
	std::multiset<std::string> _handled;
	Poco::FastMutex            _mutex;
};


#endif // PriorityNotificationQueueTest_INCLUDED
