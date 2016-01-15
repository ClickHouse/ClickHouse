//
// TimedNotificationQueueTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/TimedNotificationQueueTest.cpp#1 $
//
// Copyright (c) 2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "TimedNotificationQueueTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/TimedNotificationQueue.h"
#include "Poco/Notification.h"
#include "Poco/Timestamp.h"


using Poco::TimedNotificationQueue;
using Poco::Notification;
using Poco::Timestamp;


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


TimedNotificationQueueTest::TimedNotificationQueueTest(const std::string& name): CppUnit::TestCase(name)
{
}


TimedNotificationQueueTest::~TimedNotificationQueueTest()
{
}


void TimedNotificationQueueTest::testDequeue()
{
	TimedNotificationQueue queue;
	assert (queue.empty());
	assert (queue.size() == 0);
	Notification* pNf = queue.dequeueNotification();
	assertNullPtr(pNf);
	queue.enqueueNotification(new Notification, Timestamp());
	assert (!queue.empty());
	assert (queue.size() == 1);
	pNf = queue.dequeueNotification();
	assertNotNullPtr(pNf);
	assert (queue.empty());
	assert (queue.size() == 0);
	pNf->release();
	
	Poco::Timestamp ts1;
	ts1 += 100000;
	Poco::Timestamp ts2;
	ts2 += 200000;
	Poco::Timestamp ts3;
	ts3 += 300000;
	Poco::Timestamp ts4;
	ts4 += 400000;
	
	queue.enqueueNotification(new QTestNotification("first"), ts1);
	queue.enqueueNotification(new QTestNotification("fourth"), ts4);
	queue.enqueueNotification(new QTestNotification("third"), ts3);
	queue.enqueueNotification(new QTestNotification("second"), ts2);
	assert (!queue.empty());
	assert (queue.size() == 4);
	QTestNotification* pTNf = 0;
	while (!pTNf) 
	{
		pTNf = dynamic_cast<QTestNotification*>(queue.dequeueNotification());
	}
	assertNotNullPtr(pTNf);
	assert (pTNf->data() == "first");
	pTNf->release();
	assert (ts1.elapsed() >= 0);
	assert (!queue.empty());
	assert (queue.size() == 3);
	
	pTNf = 0;
	while (!pTNf) 
	{
		pTNf = dynamic_cast<QTestNotification*>(queue.dequeueNotification());
	}
	assertNotNullPtr(pTNf);
	assert (pTNf->data() == "second");
	pTNf->release();
	assert (ts2.elapsed() >= 0);
	assert (!queue.empty());
	assert (queue.size() == 2);
	
	pTNf = 0;
	while (!pTNf) 
	{
		pTNf = dynamic_cast<QTestNotification*>(queue.dequeueNotification());
	}
	assertNotNullPtr(pTNf);
	assert (pTNf->data() == "third");
	pTNf->release();
	assert (ts3.elapsed() >= 0);
	assert (!queue.empty());
	assert (queue.size() == 1);
	
	pTNf = 0;
	while (!pTNf) 
	{
		pTNf = dynamic_cast<QTestNotification*>(queue.dequeueNotification());
	}
	assertNotNullPtr(pTNf);
	assert (pTNf->data() == "fourth");
	pTNf->release();
	assert (ts4.elapsed() >= 0);
	assert (queue.empty());
	assert (queue.size() == 0);

	pNf = queue.dequeueNotification();
	assertNullPtr(pNf);
}


void TimedNotificationQueueTest::testWaitDequeue()
{
	TimedNotificationQueue queue;
	
	Poco::Timestamp ts1;
	ts1 += 100000;
	Poco::Timestamp ts2;
	ts2 += 200000;
	Poco::Timestamp ts3;
	ts3 += 300000;
	Poco::Timestamp ts4;
	ts4 += 400000;
	
	queue.enqueueNotification(new QTestNotification("first"), ts1);
	queue.enqueueNotification(new QTestNotification("fourth"), ts4);
	queue.enqueueNotification(new QTestNotification("third"), ts3);
	queue.enqueueNotification(new QTestNotification("second"), ts2);
	assert (!queue.empty());
	assert (queue.size() == 4);
	QTestNotification* pTNf = dynamic_cast<QTestNotification*>(queue.waitDequeueNotification());
	assertNotNullPtr(pTNf);
	assert (pTNf->data() == "first");
	pTNf->release();
	assert (ts1.elapsed() >= 0);
	assert (!queue.empty());
	assert (queue.size() == 3);
	
	pTNf = dynamic_cast<QTestNotification*>(queue.waitDequeueNotification());
	assertNotNullPtr(pTNf);
	assert (pTNf->data() == "second");
	pTNf->release();
	assert (ts2.elapsed() >= 0);
	assert (!queue.empty());
	assert (queue.size() == 2);
	
	pTNf = dynamic_cast<QTestNotification*>(queue.waitDequeueNotification());
	assertNotNullPtr(pTNf);
	assert (pTNf->data() == "third");
	pTNf->release();
	assert (ts3.elapsed() >= 0);
	assert (!queue.empty());
	assert (queue.size() == 1);
	
	pTNf = dynamic_cast<QTestNotification*>(queue.waitDequeueNotification());
	assertNotNullPtr(pTNf);
	assert (pTNf->data() == "fourth");
	pTNf->release();
	assert (ts4.elapsed() >= 0);
	assert (queue.empty());
	assert (queue.size() == 0);
}


void TimedNotificationQueueTest::testWaitDequeueTimeout()
{
	TimedNotificationQueue queue;
	
	Poco::Timestamp ts1;
	ts1 += 200000;
	Poco::Timestamp ts2;
	ts2 += 400000;
	Poco::Timestamp ts3;
	ts3 += 600000;
	Poco::Timestamp ts4;
	ts4 += 800000;
	
	queue.enqueueNotification(new QTestNotification("first"), ts1);
	queue.enqueueNotification(new QTestNotification("fourth"), ts4);
	queue.enqueueNotification(new QTestNotification("third"), ts3);
	queue.enqueueNotification(new QTestNotification("second"), ts2);
	assert (!queue.empty());
	assert (queue.size() == 4);
	QTestNotification* pTNf = dynamic_cast<QTestNotification*>(queue.waitDequeueNotification(10));
	assertNullPtr(pTNf);
	pTNf = dynamic_cast<QTestNotification*>(queue.waitDequeueNotification(20));
	assertNullPtr(pTNf);
	pTNf = dynamic_cast<QTestNotification*>(queue.waitDequeueNotification(200));
	assertNotNullPtr(pTNf);
	assert (pTNf->data() == "first");
	pTNf->release();
	assert (ts1.elapsed() >= 0);
	assert (!queue.empty());
	assert (queue.size() == 3);
	
	pTNf = dynamic_cast<QTestNotification*>(queue.waitDequeueNotification(220));
	assertNotNullPtr(pTNf);
	assert (pTNf->data() == "second");
	pTNf->release();
	assert (ts2.elapsed() >= 0);
	assert (!queue.empty());
	assert (queue.size() == 2);
	
	pTNf = dynamic_cast<QTestNotification*>(queue.waitDequeueNotification(220));
	assertNotNullPtr(pTNf);
	assert (pTNf->data() == "third");
	pTNf->release();
	assert (ts3.elapsed() >= 0);
	assert (!queue.empty());
	assert (queue.size() == 1);
	
	pTNf = dynamic_cast<QTestNotification*>(queue.waitDequeueNotification(220));
	assertNotNullPtr(pTNf);
	assert (pTNf->data() == "fourth");
	pTNf->release();
	assert (ts1.elapsed() >= 0);
	assert (queue.empty());
	assert (queue.size() == 0);
}


void TimedNotificationQueueTest::setUp()
{
}


void TimedNotificationQueueTest::tearDown()
{
}


CppUnit::Test* TimedNotificationQueueTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("TimedNotificationQueueTest");

	CppUnit_addTest(pSuite, TimedNotificationQueueTest, testDequeue);
	CppUnit_addTest(pSuite, TimedNotificationQueueTest, testWaitDequeue);
	CppUnit_addTest(pSuite, TimedNotificationQueueTest, testWaitDequeueTimeout);

	return pSuite;
}
