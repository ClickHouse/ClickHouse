//
// ConditionTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/ConditionTest.cpp#1 $
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "ConditionTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Thread.h"
#include "Poco/Runnable.h"
#include "Poco/Condition.h"
#include "Poco/Mutex.h"
#include "Poco/Exception.h"


using Poco::Thread;
using Poco::Runnable;
using Poco::Condition;
using Poco::Mutex;
using Poco::TimeoutException;


namespace
{
	class WaitRunnable: public Runnable
	{
	public:
		WaitRunnable(Condition& cond, Mutex& mutex):
			_ran(false),
			_cond(cond),
			_mutex(mutex)
		{
		}
		
		void run()
		{
			_mutex.lock();
			_cond.wait(_mutex);
			_mutex.unlock();
			_ran = true;
		}
		
		bool ran() const
		{
			return _ran;
		}
			
	private:
		bool _ran;
		Condition& _cond;
		Mutex& _mutex;
	};
	
	class TryWaitRunnable: public Runnable
	{
	public:
		TryWaitRunnable(Condition& cond, Mutex& mutex):
			_ran(false),
			_cond(cond),
			_mutex(mutex)
		{
		}
		
		void run()
		{
			_mutex.lock();
			if (_cond.tryWait(_mutex, 10000))
			{
				_ran = true;
			}
			_mutex.unlock();
		}
		
		bool ran() const
		{
			return _ran;
		}
			
	private:
		bool _ran;
		Condition& _cond;
		Mutex& _mutex;
	};

}


ConditionTest::ConditionTest(const std::string& name): CppUnit::TestCase(name)
{
}


ConditionTest::~ConditionTest()
{
}


void ConditionTest::testSignal()
{
	Condition cond;
	Mutex mtx;
	WaitRunnable r1(cond, mtx);
	WaitRunnable r2(cond, mtx);
	
	Thread t1;
	Thread t2;
	
	t1.start(r1);
	Thread::sleep(200);
	t2.start(r2);
	
	assert (!r1.ran());
	assert (!r2.ran());
	
	cond.signal();
	
	t1.join();
	assert (r1.ran());
	
	assert (!t2.tryJoin(200));
	
	cond.signal();
	
	t2.join();

	assert (r2.ran());
}


void ConditionTest::testBroadcast()
{
	Condition cond;
	Mutex mtx;
	WaitRunnable r1(cond, mtx);
	WaitRunnable r2(cond, mtx);
	TryWaitRunnable r3(cond, mtx);
	
	Thread t1;
	Thread t2;
	Thread t3;
	
	t1.start(r1);
	Thread::sleep(200);
	t2.start(r2);
	Thread::sleep(200);
	t3.start(r3);
	
	assert (!r1.ran());
	assert (!r2.ran());
	assert (!r3.ran());
	
	cond.signal();
	t1.join();
	
	assert (r1.ran());
	assert (!t2.tryJoin(500));
	assert (!t3.tryJoin(500));
	
	cond.broadcast();
	
	t2.join();
	t3.join();
	
	assert (r2.ran());
	assert (r3.ran());
}


void ConditionTest::setUp()
{
}


void ConditionTest::tearDown()
{
}


CppUnit::Test* ConditionTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("ConditionTest");

	CppUnit_addTest(pSuite, ConditionTest, testSignal);
	CppUnit_addTest(pSuite, ConditionTest, testBroadcast);

	return pSuite;
}
