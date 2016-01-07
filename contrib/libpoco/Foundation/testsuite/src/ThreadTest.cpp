//
// ThreadTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/ThreadTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "ThreadTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Thread.h"
#include "Poco/Runnable.h"
#include "Poco/ThreadTarget.h"
#include "Poco/Event.h"
#include "Poco/Timestamp.h"
#include "Poco/Timespan.h"
//#include <iostream>
#if defined(__sun) && defined(__SVR4) && !defined(__EXTENSIONS__)
#define __EXTENSIONS__
#endif
#include <climits>


using Poco::Thread;
using Poco::Runnable;
using Poco::ThreadTarget;
using Poco::Event;


class MyRunnable: public Runnable
{
public:
	MyRunnable(): _ran(false)
	{
	}
	
	void run()
	{
		Thread* pThread = Thread::current();
		if (pThread)
			_threadName = pThread->name();
		_ran = true;
		_event.wait();
	}
	
	bool ran() const
	{
		return _ran;
	}
	
	const std::string& threadName() const
	{
		return _threadName;
	}
	
	void notify()
	{
		_event.set();
	}
	
	static void staticFunc()
	{
		++_staticVar;
	}

	static int _staticVar;

private:
	bool _ran;
	std::string _threadName;
	Event _event;
};


int MyRunnable::_staticVar = 0;


void freeFunc()
{
	++MyRunnable::_staticVar;
}


void freeFunc(void* pData)
{
	MyRunnable::_staticVar += *reinterpret_cast<int*>(pData);
}


class NonJoinRunnable : public Runnable
{
public:
	NonJoinRunnable() : _finished(false)
	{
	}
	
	void run()
	{
		_finished = true;
	}
	
	bool finished() const
	{
		return _finished;
	}
	
private:
	bool _finished;
};


class TrySleepRunnable : public Runnable
{
public:
	TrySleepRunnable() : _counter(0), _sleepy(true)
	{
	}

	void run()
	{
		_sleepy = !Thread::trySleep(300000);
		++_counter;
		_sleepy = !Thread::trySleep(300000);
		++_counter;
		_sleepy = !Thread::trySleep(100);
		++_counter;
	}

	int counter() const
	{
		return _counter;
	}

	bool isSleepy() const
	{
		return _sleepy;
	}

private:
	int _counter;
	bool _sleepy;
};


ThreadTest::ThreadTest(const std::string& name): CppUnit::TestCase(name)
{
}


ThreadTest::~ThreadTest()
{
}


void ThreadTest::testThread()
{
	Thread thread;
	MyRunnable r;
	assert (!thread.isRunning());
	thread.start(r);
	Thread::sleep(200);
	assert (thread.isRunning());
	r.notify();
	thread.join();
	assert (!thread.isRunning());
	assert (r.ran());
	assert (!r.threadName().empty());
}


void ThreadTest::testNamedThread()
{
	Thread thread("MyThread");
	MyRunnable r;
	thread.start(r);
	r.notify();
	thread.join();
	assert (r.ran());
	assert (r.threadName() == "MyThread");
}


void ThreadTest::testCurrent()
{
	assertNullPtr (Thread::current());
}


void ThreadTest::testThreads()
{
	Thread thread1("Thread1");
	Thread thread2("Thread2");
	Thread thread3("Thread3");
	Thread thread4("Thread4");

	MyRunnable r1;
	MyRunnable r2;
	MyRunnable r3;
	MyRunnable r4;
	assert (!thread1.isRunning());
	assert (!thread2.isRunning());
	assert (!thread3.isRunning());
	assert (!thread4.isRunning());
	thread1.start(r1);
	Thread::sleep(200);
	assert (thread1.isRunning());
	assert (!thread2.isRunning());
	assert (!thread3.isRunning());
	assert (!thread4.isRunning());
	thread2.start(r2);
	thread3.start(r3);
	thread4.start(r4);
	Thread::sleep(200);
	assert (thread1.isRunning());
	assert (thread2.isRunning());
	assert (thread3.isRunning());
	assert (thread4.isRunning());
	r4.notify();
	thread4.join();
	assert (!thread4.isRunning());
	assert (thread1.isRunning());
	assert (thread2.isRunning());
	assert (thread3.isRunning());
	r3.notify();
	thread3.join();
	assert (!thread3.isRunning());
	r2.notify();
	thread2.join();
	assert (!thread2.isRunning());
	r1.notify();
	thread1.join();
	assert (!thread1.isRunning());
	assert (r1.ran());
	assert (r1.threadName() == "Thread1");
	assert (r2.ran());
	assert (r2.threadName() == "Thread2");
	assert (r3.ran());
	assert (r3.threadName() == "Thread3");
	assert (r4.ran());
	assert (r4.threadName() == "Thread4");
}


void ThreadTest::testJoin()
{
	Thread thread;
	MyRunnable r;
	assert (!thread.isRunning());
	thread.start(r);
	Thread::sleep(200);
	assert (thread.isRunning());
	assert (!thread.tryJoin(100));
	r.notify();
	assert (thread.tryJoin(500));
	assert (!thread.isRunning());
}


void ThreadTest::testNotJoin()
{
	Thread thread;
	NonJoinRunnable r;
	thread.start(r);
	
	while (!r.finished())
	{
		Thread::sleep(10);
	}
	
	Thread::sleep(100);
	assert (!thread.isRunning());
}


void ThreadTest::testTrySleep()
{
	Thread thread;
	TrySleepRunnable r;
	assert(r.isSleepy());
	assert(!thread.isRunning());
	assert(r.counter() == 0);
	thread.start(r);
	assert(thread.isRunning());
	assert(r.counter() == 0);
	assert(r.isSleepy());
	Thread::sleep(100);
	assert(r.counter() == 0);
	assert(r.isSleepy());
	thread.wakeUp(); Thread::sleep(10);
	assert(r.counter() == 1);
	assert(r.isSleepy());
	Thread::sleep(100);
	assert(r.counter() == 1);
	thread.wakeUp(); Thread::sleep(10);
	assert(r.counter() == 2);
	assert(r.isSleepy());
	Thread::sleep(200);
	assert(r.counter() == 3);
	assert(!r.isSleepy());
	assert(!thread.isRunning());
	thread.wakeUp();
	assert(!thread.isRunning());
}


void ThreadTest::testNotRun()
{
	Thread thread;
}


void ThreadTest::testNotRunJoin()
{
	Thread thread;
	thread.join();
}


void ThreadTest::testThreadTarget()
{
	ThreadTarget te(&MyRunnable::staticFunc);
	Thread thread;

	assert (!thread.isRunning());

	int tmp = MyRunnable::_staticVar;
	thread.start(te);
	thread.join();
	assert (tmp + 1 == MyRunnable::_staticVar);

	ThreadTarget te1(freeFunc);
	assert (!thread.isRunning());

	tmp = MyRunnable::_staticVar;
	thread.start(te1);
	thread.join();
	assert (tmp + 1 == MyRunnable::_staticVar);
}


void ThreadTest::testThreadFunction()
{
	Thread thread;

	assert (!thread.isRunning());

	int tmp = MyRunnable::_staticVar;
	thread.start(freeFunc, &tmp);
	thread.join();
	assert (tmp * 2 == MyRunnable::_staticVar);

	assert (!thread.isRunning());

	tmp = MyRunnable::_staticVar = 0;
	thread.start(freeFunc, &tmp);
	thread.join();
	assert (0 == MyRunnable::_staticVar);
}


struct Functor
{
	void operator () ()
	{
		++MyRunnable::_staticVar;
	}
};


void ThreadTest::testThreadFunctor()
{
	Thread thread;

	assert (!thread.isRunning());

	MyRunnable::_staticVar = 0;
	thread.startFunc(Functor());
	thread.join();
	assert (1 == MyRunnable::_staticVar);

	assert (!thread.isRunning());

#if __cplusplus >= 201103L

	Thread thread2;

	assert (!thread2.isRunning());

	MyRunnable::_staticVar = 0;
	thread.startFunc([] () {MyRunnable::_staticVar++;});
	thread.join();
	assert (1 == MyRunnable::_staticVar);

	assert (!thread2.isRunning());

#endif
}


void ThreadTest::testThreadStackSize()
{
	int stackSize = 50000000;

	Thread thread;

	assert (0 == thread.getStackSize());
	thread.setStackSize(stackSize);
	assert (stackSize <= thread.getStackSize());
	int tmp = MyRunnable::_staticVar;
	thread.start(freeFunc, &tmp);
	thread.join();
	assert (tmp * 2 == MyRunnable::_staticVar);

	stackSize = 1;
	thread.setStackSize(stackSize);

#if !defined(POCO_OS_FAMILY_BSD) // on BSD family, stack size is rounded
	#ifdef PTHREAD_STACK_MIN
		assert (PTHREAD_STACK_MIN == thread.getStackSize());
	#else
		assert (stackSize >= thread.getStackSize());
	#endif
#endif

	tmp = MyRunnable::_staticVar;
	thread.start(freeFunc, &tmp);
	thread.join();
	assert (tmp * 2 == MyRunnable::_staticVar);

	thread.setStackSize(0);
	assert (0 == thread.getStackSize());
	tmp = MyRunnable::_staticVar;
	thread.start(freeFunc, &tmp);
	thread.join();
	assert (tmp * 2 == MyRunnable::_staticVar);
}


void ThreadTest::testSleep()
{
	Poco::Timestamp start;
	Thread::sleep(200);
	Poco::Timespan elapsed = start.elapsed();
	assert (elapsed.totalMilliseconds() >= 190 && elapsed.totalMilliseconds() < 250);
}


void ThreadTest::setUp()
{
}


void ThreadTest::tearDown()
{
}


CppUnit::Test* ThreadTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("ThreadTest");

	CppUnit_addTest(pSuite, ThreadTest, testThread);
	CppUnit_addTest(pSuite, ThreadTest, testNamedThread);
	CppUnit_addTest(pSuite, ThreadTest, testCurrent);
	CppUnit_addTest(pSuite, ThreadTest, testThreads);
	CppUnit_addTest(pSuite, ThreadTest, testJoin);
	CppUnit_addTest(pSuite, ThreadTest, testNotJoin);
	CppUnit_addTest(pSuite, ThreadTest, testNotRun);
	CppUnit_addTest(pSuite, ThreadTest, testNotRunJoin);
	CppUnit_addTest(pSuite, ThreadTest, testTrySleep);
	CppUnit_addTest(pSuite, ThreadTest, testThreadTarget);
	CppUnit_addTest(pSuite, ThreadTest, testThreadFunction);
	CppUnit_addTest(pSuite, ThreadTest, testThreadFunctor);
	CppUnit_addTest(pSuite, ThreadTest, testThreadStackSize);
	CppUnit_addTest(pSuite, ThreadTest, testSleep);

	return pSuite;
}
