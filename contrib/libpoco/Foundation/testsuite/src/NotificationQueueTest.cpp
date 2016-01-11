//
// NotificationQueueTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/NotificationQueueTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "NotificationQueueTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/NotificationQueue.h"
#include "Poco/Notification.h"
#include "Poco/Thread.h"
#include "Poco/Runnable.h"
#include "Poco/RunnableAdapter.h"
#include "Poco/Random.h"


using Poco::NotificationQueue;
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


NotificationQueueTest::NotificationQueueTest(const std::string& name): CppUnit::TestCase(name)
{
}


NotificationQueueTest::~NotificationQueueTest()
{
}


void NotificationQueueTest::testQueueDequeue()
{
	NotificationQueue queue;
	assert (queue.empty());
	assert (queue.size() == 0);
	Notification* pNf = queue.dequeueNotification();
	assertNullPtr(pNf);
	queue.enqueueNotification(new Notification);
	assert (!queue.empty());
	assert (queue.size() == 1);
	pNf = queue.dequeueNotification();
	assertNotNullPtr(pNf);
	assert (queue.empty());
	assert (queue.size() == 0);
	pNf->release();
	
	queue.enqueueNotification(new QTestNotification("first"));
	queue.enqueueNotification(new QTestNotification("second"));
	assert (!queue.empty());
	assert (queue.size() == 2);
	QTestNotification* pTNf = dynamic_cast<QTestNotification*>(queue.dequeueNotification());
	assertNotNullPtr(pTNf);
	assert (pTNf->data() == "first");
	pTNf->release();
	assert (!queue.empty());
	assert (queue.size() == 1);
	pTNf = dynamic_cast<QTestNotification*>(queue.dequeueNotification());
	assertNotNullPtr(pTNf);
	assert (pTNf->data() == "second");
	pTNf->release();
	assert (queue.empty());
	assert (queue.size() == 0);

	pNf = queue.dequeueNotification();
	assertNullPtr(pNf);
}


void NotificationQueueTest::testQueueDequeueUrgent()
{
	NotificationQueue queue;	
	queue.enqueueNotification(new QTestNotification("first"));
	queue.enqueueNotification(new QTestNotification("second"));
	queue.enqueueUrgentNotification(new QTestNotification("third"));
	assert (!queue.empty());
	assert (queue.size() == 3);
	QTestNotification* pTNf = dynamic_cast<QTestNotification*>(queue.dequeueNotification());
	assertNotNullPtr(pTNf);
	assert (pTNf->data() == "third");
	pTNf->release();
	assert (!queue.empty());
	assert (queue.size() == 2);
	pTNf = dynamic_cast<QTestNotification*>(queue.dequeueNotification());
	assert (pTNf->data() == "first");
	pTNf->release();
	assert (!queue.empty());
	assert (queue.size() == 1);
	pTNf = dynamic_cast<QTestNotification*>(queue.dequeueNotification());
	assertNotNullPtr(pTNf);
	assert (pTNf->data() == "second");
	pTNf->release();
	assert (queue.empty());
	assert (queue.size() == 0);

	Notification* pNf = queue.dequeueNotification();
	assertNullPtr(pNf);
}


void NotificationQueueTest::testWaitDequeue()
{
	NotificationQueue queue;
	queue.enqueueNotification(new QTestNotification("third"));
	queue.enqueueNotification(new QTestNotification("fourth"));
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


void NotificationQueueTest::testThreads()
{
	const int NOTIFICATION_COUNT = 5000;

	Thread t1("thread1");
	Thread t2("thread2");
	Thread t3("thread3");
	
	RunnableAdapter<NotificationQueueTest> ra(*this, &NotificationQueueTest::work);
	t1.start(ra);
	t2.start(ra);
	t3.start(ra);
	for (int i = 0; i < NOTIFICATION_COUNT; ++i)
	{
		_queue.enqueueNotification(new Notification);
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


void NotificationQueueTest::testDefaultQueue()
{
	NotificationQueue& queue = NotificationQueue::defaultQueue();
	assert (queue.empty());
	assert (queue.size() == 0);
}


void NotificationQueueTest::setUp()
{
	_handled.clear();
}


void NotificationQueueTest::tearDown()
{
}


void NotificationQueueTest::work()
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


CppUnit::Test* NotificationQueueTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("NotificationQueueTest");

	CppUnit_addTest(pSuite, NotificationQueueTest, testQueueDequeue);
	CppUnit_addTest(pSuite, NotificationQueueTest, testQueueDequeueUrgent);
	CppUnit_addTest(pSuite, NotificationQueueTest, testWaitDequeue);
	CppUnit_addTest(pSuite, NotificationQueueTest, testThreads);
	CppUnit_addTest(pSuite, NotificationQueueTest, testDefaultQueue);

	return pSuite;
}
