//
// PriorityNotificationQueueTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/PriorityNotificationQueueTest.cpp#1 $
//
// Copyright (c) 2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "PriorityNotificationQueueTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/PriorityNotificationQueue.h"
#include "Poco/Notification.h"
#include "Poco/Thread.h"
#include "Poco/Runnable.h"
#include "Poco/RunnableAdapter.h"
#include "Poco/Random.h"


using Poco::PriorityNotificationQueue;
using Poco::Notification;
using Poco::Thread;
using Poco::RunnableAdapter;


namespace 
{
	class QTestNotification: public Notification
	{
	public:
		QTestNotification(const std::string& data): _data(data)
		{
		}
		~QTestNotification()
		{
		}
		const std::string& data() const
		{
			return _data;
		}

	private:
		std::string _data;
	};
}


PriorityNotificationQueueTest::PriorityNotificationQueueTest(const std::string& name): CppUnit::TestCase(name)
{
}


PriorityNotificationQueueTest::~PriorityNotificationQueueTest()
{
}


void PriorityNotificationQueueTest::testQueueDequeue()
{
	PriorityNotificationQueue queue;
	assert (queue.empty());
	assert (queue.size() == 0);
	Notification* pNf = queue.dequeueNotification();
	assertNullPtr(pNf);
	queue.enqueueNotification(new Notification, 1);
	assert (!queue.empty());
	assert (queue.size() == 1);
	pNf = queue.dequeueNotification();
	assertNotNullPtr(pNf);
	assert (queue.empty());
	assert (queue.size() == 0);
	pNf->release();
	
	queue.enqueueNotification(new QTestNotification("first"), 1);
	queue.enqueueNotification(new QTestNotification("fourth"), 4);
	queue.enqueueNotification(new QTestNotification("third"), 3);
	queue.enqueueNotification(new QTestNotification("second"), 2);
	assert (!queue.empty());
	assert (queue.size() == 4);
	QTestNotification* pTNf = dynamic_cast<QTestNotification*>(queue.dequeueNotification());
	assertNotNullPtr(pTNf);
	assert (pTNf->data() == "first");
	pTNf->release();
	assert (!queue.empty());
	assert (queue.size() == 3);
	pTNf = dynamic_cast<QTestNotification*>(queue.dequeueNotification());
	assertNotNullPtr(pTNf);
	assert (pTNf->data() == "second");
	pTNf->release();
	assert (!queue.empty());
	assert (queue.size() == 2);
	pTNf = dynamic_cast<QTestNotification*>(queue.dequeueNotification());
	assertNotNullPtr(pTNf);
	assert (pTNf->data() == "third");
	pTNf->release();
	assert (!queue.empty());
	assert (queue.size() == 1);
	pTNf = dynamic_cast<QTestNotification*>(queue.dequeueNotification());
	assertNotNullPtr(pTNf);
	assert (pTNf->data() == "fourth");
	pTNf->release();
	assert (queue.empty());
	assert (queue.size() == 0);

	pNf = queue.dequeueNotification();
	assertNullPtr(pNf);
}


void PriorityNotificationQueueTest::testWaitDequeue()
{
	PriorityNotificationQueue queue;
	queue.enqueueNotification(new QTestNotification("third"), 3);
	queue.enqueueNotification(new QTestNotification("fourth"), 4);
	assert (!queue.empty());
	assert (queue.size() == 2);
	QTestNotification* pTNf = dynamic_cast<QTestNotification*>(queue.waitDequeueNotification(10));
	assertNotNullPtr(pTNf);
	assert (pTNf->data() == "third");
	pTNf->release();
	assert (!queue.empty());
	assert (queue.size() == 1);
	pTNf = dynamic_cast<QTestNotification*>(queue.waitDequeueNotification(10));
	assertNotNullPtr(pTNf);
	assert (pTNf->data() == "fourth");
	pTNf->release();
	assert (queue.empty());
	assert (queue.size() == 0);

	Notification* pNf = queue.waitDequeueNotification(10);
	assertNullPtr(pNf);
}


void PriorityNotificationQueueTest::testThreads()
{
	const int NOTIFICATION_COUNT = 5000;
	
	Thread t1("thread1");
	Thread t2("thread2");
	Thread t3("thread3");
	
	RunnableAdapter<PriorityNotificationQueueTest> ra(*this, &PriorityNotificationQueueTest::work);
	t1.start(ra);
	t2.start(ra);
	t3.start(ra);
	for (int i = 0; i < NOTIFICATION_COUNT; ++i)
	{
		_queue.enqueueNotification(new Notification, 1);
	}
	while (!_queue.empty()) Thread::sleep(50);
	Thread::sleep(20);
	_queue.wakeUpAll();
	t1.join();
	t2.join();
	t3.join();
	assert (_handled.size() == NOTIFICATION_COUNT);
	assert (_handled.count("thread1") > 0);
	assert (_handled.count("thread2") > 0);
	assert (_handled.count("thread3") > 0);
}


void PriorityNotificationQueueTest::testDefaultQueue()
{
	PriorityNotificationQueue& queue = PriorityNotificationQueue::defaultQueue();
	assert (queue.empty());
	assert (queue.size() == 0);
}


void PriorityNotificationQueueTest::setUp()
{
	_handled.clear();
}


void PriorityNotificationQueueTest::tearDown()
{
}


void PriorityNotificationQueueTest::work()
{
	Poco::Random rnd;
	Thread::sleep(50);
	Notification* pNf = _queue.waitDequeueNotification();
	while (pNf)
	{
		pNf->release();
		_mutex.lock();
		_handled.insert(Thread::current()->name());
		_mutex.unlock();
		Thread::sleep(rnd.next(5));
		pNf = _queue.waitDequeueNotification();
	}
}


CppUnit::Test* PriorityNotificationQueueTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("PriorityNotificationQueueTest");

	CppUnit_addTest(pSuite, PriorityNotificationQueueTest, testQueueDequeue);
	CppUnit_addTest(pSuite, PriorityNotificationQueueTest, testWaitDequeue);
	CppUnit_addTest(pSuite, PriorityNotificationQueueTest, testThreads);
	CppUnit_addTest(pSuite, PriorityNotificationQueueTest, testDefaultQueue);

	return pSuite;
}
