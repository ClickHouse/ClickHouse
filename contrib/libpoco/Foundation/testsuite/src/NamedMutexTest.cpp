//
// NamedMutexTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/NamedMutexTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "NamedMutexTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/NamedMutex.h"
#include "Poco/Thread.h"
#include "Poco/Runnable.h"
#include "Poco/Timestamp.h"


using Poco::NamedMutex;
using Poco::Thread;
using Poco::Runnable;
using Poco::Timestamp;


static NamedMutex testMutex("TestMutex");


namespace
{
	class TestLock: public Runnable
	{
	public:
		void run()
		{
		
			testMutex.lock();
			_timestamp.update();
			testMutex.unlock();
		}

		const Timestamp& timestamp() const
		{
			return _timestamp;
		}

	private:
		Timestamp _timestamp;
	};

	class TestTryLock: public Runnable
	{
	public:
		TestTryLock(): _locked(false)
		{
		}
		
		void run()
		{
			if (testMutex.tryLock())
			{
				_locked = true;
				testMutex.unlock();
			}
		}

		bool locked() const
		{
			return _locked;
		}

	private:
		bool _locked;
	};
}


NamedMutexTest::NamedMutexTest(const std::string& name): CppUnit::TestCase(name)
{
}


NamedMutexTest::~NamedMutexTest()
{
}


void NamedMutexTest::testLock()
{
	testMutex.lock();
	Thread thr;
	TestLock tl;
	thr.start(tl);
	Timestamp now;
	Thread::sleep(2000);
	testMutex.unlock();
	thr.join();
	assert (tl.timestamp() > now);
}


void NamedMutexTest::testTryLock()
{
	Thread thr1;
	TestTryLock ttl1;
	thr1.start(ttl1);
	thr1.join();
	assert (ttl1.locked());
	
	testMutex.lock();
	Thread thr2;
	TestTryLock ttl2;
	thr2.start(ttl2);
	thr2.join();
	testMutex.unlock();
	assert (!ttl2.locked());
}


void NamedMutexTest::setUp()
{
}


void NamedMutexTest::tearDown()
{
}


CppUnit::Test* NamedMutexTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("NamedMutexTest");

	CppUnit_addTest(pSuite, NamedMutexTest, testLock);
	CppUnit_addTest(pSuite, NamedMutexTest, testTryLock);

	return pSuite;
}
