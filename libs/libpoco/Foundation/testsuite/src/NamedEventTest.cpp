//
// NamedEventTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/NamedEventTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "NamedEventTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/NamedEvent.h"
#include "Poco/Thread.h"
#include "Poco/Runnable.h"
#include "Poco/Timestamp.h"


using Poco::NamedEvent;
using Poco::Thread;
using Poco::Runnable;
using Poco::Timestamp;


static NamedEvent testEvent("TestEvent");


namespace
{
	class TestEvent: public Runnable
	{
	public:
		void run()
		{
		
			testEvent.wait();
			_timestamp.update();
		}

		const Timestamp& timestamp() const
		{
			return _timestamp;
		}

	private:
		Timestamp _timestamp;
	};
}


NamedEventTest::NamedEventTest(const std::string& name): CppUnit::TestCase(name)
{
}


NamedEventTest::~NamedEventTest()
{
}


void NamedEventTest::testNamedEvent()
{
	Thread thr1;
	TestEvent te;
	thr1.start(te);
	Timestamp now;
	Thread::sleep(2000);
	testEvent.set();
	thr1.join();
	assert (te.timestamp() > now);

	Thread thr2;
	thr2.start(te);
	now.update();
	Thread::sleep(2000);
	testEvent.set();
	thr2.join();
	assert (te.timestamp() > now);
}


void NamedEventTest::setUp()
{
}


void NamedEventTest::tearDown()
{
}


CppUnit::Test* NamedEventTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("NamedEventTest");

	CppUnit_addTest(pSuite, NamedEventTest, testNamedEvent);

	return pSuite;
}
