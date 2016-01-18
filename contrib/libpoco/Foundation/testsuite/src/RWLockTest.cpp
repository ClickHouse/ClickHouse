//
// RWLockTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/RWLockTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "RWLockTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/RWLock.h"
#include "Poco/Thread.h"
#include "Poco/Runnable.h"


using Poco::RWLock;
using Poco::Thread;
using Poco::Runnable;


class RWLockRunnable: public Runnable
{
public:
	RWLockRunnable(RWLock& lock, volatile int& counter): _lock(lock), _counter(counter), _ok(true)
	{
	}
	
	void run()
	{
		int lastCount = 0;
		for (int i = 0; i < 10000; ++i)
		{
			_lock.readLock();
			lastCount = _counter;
			for (int k = 0; k < 100; ++k)
			{
				if (_counter != lastCount) _ok = false;
				Thread::yield();
			}
			_lock.unlock();
			_lock.writeLock();
			for (int k = 0; k < 100; ++k)
			{
				--_counter;
				Thread::yield();
			}
			for (int k = 0; k < 100; ++k)
			{
				++_counter;
				Thread::yield();
			}
			++_counter;
			if (_counter <= lastCount) _ok = false;
			_lock.unlock();
		}
	}
	
	bool ok() const
	{
		return _ok;
	}
	
private:
	RWLock& _lock;
	volatile int& _counter;
	bool _ok;
};


class RWTryLockRunnable: public Runnable
{
public:
	RWTryLockRunnable(RWLock& lock, volatile int& counter): _lock(lock), _counter(counter), _ok(true)
	{
	}
	
	void run()
	{
		int lastCount = 0;
		for (int i = 0; i < 10000; ++i)
		{
			while (!_lock.tryReadLock()) Thread::yield();
			lastCount = _counter;
			for (int k = 0; k < 100; ++k)
			{
				if (_counter != lastCount) _ok = false;
				Thread::yield();
			}
			_lock.unlock();
			while (!_lock.tryWriteLock()) Thread::yield();
			for (int k = 0; k < 100; ++k)
			{
				--_counter;
				Thread::yield();
			}
			for (int k = 0; k < 100; ++k)
			{
				++_counter;
				Thread::yield();
			}
			++_counter;
			if (_counter <= lastCount) _ok = false;
			_lock.unlock();
		}
	}
	
	bool ok() const
	{
		return _ok;
	}
	
private:
	RWLock& _lock;
	volatile int& _counter;
	bool _ok;
};


RWLockTest::RWLockTest(const std::string& name): CppUnit::TestCase(name)
{
}


RWLockTest::~RWLockTest()
{
}


void RWLockTest::testLock()
{
	RWLock lock;
	int counter = 0;
	RWLockRunnable r1(lock, counter);
	RWLockRunnable r2(lock, counter);
	RWLockRunnable r3(lock, counter);
	RWLockRunnable r4(lock, counter);
	RWLockRunnable r5(lock, counter);
	Thread t1;
	Thread t2;
	Thread t3;
	Thread t4;
	Thread t5;
	t1.start(r1);
	t2.start(r2);
	t3.start(r3);
	t4.start(r4);
	t5.start(r5);
	t1.join();
	t2.join();
	t3.join();
	t4.join();
	t5.join();
	assert (counter == 50000);
	assert (r1.ok());
	assert (r2.ok());
	assert (r3.ok());
	assert (r4.ok());
	assert (r5.ok());
}


void RWLockTest::testTryLock()
{
	RWLock lock;
	int counter = 0;
	RWTryLockRunnable r1(lock, counter);
	RWTryLockRunnable r2(lock, counter);
	RWTryLockRunnable r3(lock, counter);
	RWTryLockRunnable r4(lock, counter);
	RWTryLockRunnable r5(lock, counter);
	Thread t1;
	Thread t2;
	Thread t3;
	Thread t4;
	Thread t5;
	t1.start(r1);
	t2.start(r2);
	t3.start(r3);
	t4.start(r4);
	t5.start(r5);
	t1.join();
	t2.join();
	t3.join();
	t4.join();
	t5.join();
	assert (counter == 50000);
	assert (r1.ok());
	assert (r2.ok());
	assert (r3.ok());
	assert (r4.ok());
	assert (r5.ok());
}


void RWLockTest::setUp()
{
}


void RWLockTest::tearDown()
{
}


CppUnit::Test* RWLockTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("RWLockTest");

	CppUnit_addTest(pSuite, RWLockTest, testLock);
	CppUnit_addTest(pSuite, RWLockTest, testTryLock);

	return pSuite;
}
