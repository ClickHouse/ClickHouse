//
// TaskManagerTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/TaskManagerTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "TaskManagerTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/Exception.h"
#include "Poco/TaskManager.h"
#include "Poco/Task.h"
#include "Poco/TaskNotification.h"
#include "Poco/NotificationCenter.h"
#include "Poco/Thread.h"
#include "Poco/ThreadPool.h"
#include "Poco/Event.h"
#include "Poco/Observer.h"
#include "Poco/Exception.h"
#include "Poco/AutoPtr.h"


using Poco::TaskManager;
using Poco::Task;
using Poco::NotificationCenter;
using Poco::TaskStartedNotification;
using Poco::TaskCancelledNotification;
using Poco::TaskFinishedNotification;
using Poco::TaskFailedNotification;
using Poco::TaskProgressNotification;
using Poco::TaskCustomNotification;
using Poco::Thread;
using Poco::ThreadPool;
using Poco::Event;
using Poco::Observer;
using Poco::Exception;
using Poco::NoThreadAvailableException;
using Poco::SystemException;
using Poco::NullPointerException;
using Poco::AutoPtr;


namespace
{
	class TestTask: public Task
	{
	public:
		TestTask(): 
			Task("TestTask"),
			_fail(false)
		{
		}
		
		void runTask()
		{
			_event.wait();
			setProgress(0.5);
			_event.wait();
			if (isCancelled())
				return;
			if (_fail)
				throw SystemException("warp core breach detected");
			setProgress(1.0);
			_event.wait();
		}
		
		void fail()
		{
			_fail = true;
		}
		
		void cont()
		{
			_event.set();
		}

	private:
		Event _event;
		bool  _fail;
	};
	
	class SimpleTask: public Task
	{
	public:
		SimpleTask(): Task("SimpleTask")
		{
		}
		
		void runTask()
		{
			sleep(10000);
		}
	};
	
	class TaskObserver
	{
	public:
		TaskObserver():
			_started(false),
			_cancelled(false),
			_finished(false),
			_pException(0),
			_progress(0.0)
		{
		}
		
		~TaskObserver()
		{
			delete _pException;
		}
		
		void taskStarted(TaskStartedNotification* pNf)
		{
			_started = true;
			pNf->release();
		}
		
		void taskCancelled(TaskCancelledNotification* pNf)
		{
			_cancelled = true;
			pNf->release();
		}
		
		void taskFinished(TaskFinishedNotification* pNf)
		{
			_finished = true;
			pNf->release();
		}
		
		void taskFailed(TaskFailedNotification* pNf)
		{
			_pException = pNf->reason().clone();
			pNf->release();
		}
		
		void taskProgress(TaskProgressNotification* pNf)
		{
			_progress = pNf->progress();
			pNf->release();
		}
		
		bool started() const
		{
			return _started;
		}
		
		bool cancelled() const
		{
			return _cancelled;
		}
		
		bool finished() const
		{
			return _finished;
		}
		
		float progress() const
		{
			return _progress;
		}
		
		Exception* error() const
		{
			return _pException;
		}
		
	private:
		bool       _started;
		bool       _cancelled;
		bool       _finished;
		Exception* _pException;
		float      _progress;
	};


	template <typename T>
	class CustomNotificationTask: public Task
	{
	public:
		CustomNotificationTask(const T& t): 
			Task("CustomNotificationTask"),
			_custom(t)
		{
		}
		
		void runTask()
		{
			sleep(10000);
		}
		
		void setCustom(const T& custom)
		{
			_custom = custom;
			postNotification(new TaskCustomNotification<T>(this, _custom));
		}
		
	private:
		T _custom;
	};
	

	template <class C>
	class CustomTaskObserver
	{
	public:
		CustomTaskObserver(const C& custom): _custom(custom)
		{
		}
		
		~CustomTaskObserver()
		{
		}
		
		void taskCustom(TaskCustomNotification<C>* pNf)
		{
			_custom = pNf->custom();
			pNf->release();
		}

		const C& custom() const
		{
			return _custom;
		}

	private:
		C _custom;
	};
}


TaskManagerTest::TaskManagerTest(const std::string& name): CppUnit::TestCase(name)
{
}


TaskManagerTest::~TaskManagerTest()
{
}


void TaskManagerTest::testFinish()
{
	TaskManager tm;
	TaskObserver to;
	tm.addObserver(Observer<TaskObserver, TaskStartedNotification>(to, &TaskObserver::taskStarted));
	tm.addObserver(Observer<TaskObserver, TaskCancelledNotification>(to, &TaskObserver::taskCancelled));
	tm.addObserver(Observer<TaskObserver, TaskFailedNotification>(to, &TaskObserver::taskFailed));
	tm.addObserver(Observer<TaskObserver, TaskFinishedNotification>(to, &TaskObserver::taskFinished));
	tm.addObserver(Observer<TaskObserver, TaskProgressNotification>(to, &TaskObserver::taskProgress));
	AutoPtr<TestTask> pTT = new TestTask;
	tm.start(pTT.duplicate());
	assert (pTT->progress() == 0);
	Thread::sleep(200);
	pTT->cont();
	while (pTT->progress() != 0.5) Thread::sleep(50);
	assert (to.progress() == 0.5);
	assert (to.started());
	assert (pTT->state() == Task::TASK_RUNNING);
	TaskManager::TaskList list = tm.taskList();
	assert (list.size() == 1);
	assert (tm.count() == 1);
	pTT->cont();
	while (pTT->progress() != 1.0) Thread::sleep(50);
	pTT->cont();
	while (pTT->state() != Task::TASK_FINISHED) Thread::sleep(50);
	assert (pTT->state() == Task::TASK_FINISHED);
	while (!to.finished()) Thread::sleep(50);
	assert (to.finished());
	while (tm.count() == 1) Thread::sleep(50);
	list = tm.taskList();
	assert (list.empty());
	assert (!to.error());
}


void TaskManagerTest::testCancel()
{
	TaskManager tm;
	TaskObserver to;
	tm.addObserver(Observer<TaskObserver, TaskStartedNotification>(to, &TaskObserver::taskStarted));
	tm.addObserver(Observer<TaskObserver, TaskCancelledNotification>(to, &TaskObserver::taskCancelled));
	tm.addObserver(Observer<TaskObserver, TaskFailedNotification>(to, &TaskObserver::taskFailed));
	tm.addObserver(Observer<TaskObserver, TaskFinishedNotification>(to, &TaskObserver::taskFinished));
	tm.addObserver(Observer<TaskObserver, TaskProgressNotification>(to, &TaskObserver::taskProgress));
	AutoPtr<TestTask> pTT = new TestTask;
	tm.start(pTT.duplicate());
	assert (pTT->progress() == 0);
	Thread::sleep(200);
	pTT->cont();
	while (pTT->progress() != 0.5) Thread::sleep(50);
	assert (to.progress() == 0.5);
	assert (to.started());
	assert (pTT->state() == Task::TASK_RUNNING);
	TaskManager::TaskList list = tm.taskList();
	assert (list.size() == 1);
	assert (tm.count() == 1);
	tm.cancelAll();
	assert (to.cancelled());
	pTT->cont();
	while (pTT->state() != Task::TASK_FINISHED) Thread::sleep(50);
	assert (pTT->state() == Task::TASK_FINISHED);
	assert (to.finished());
	while (tm.count() == 1) Thread::sleep(50);
	list = tm.taskList();
	assert (list.empty());
	assert (!to.error());
}


void TaskManagerTest::testError()
{
	TaskManager tm;
	TaskObserver to;
	tm.addObserver(Observer<TaskObserver, TaskStartedNotification>(to, &TaskObserver::taskStarted));
	tm.addObserver(Observer<TaskObserver, TaskCancelledNotification>(to, &TaskObserver::taskCancelled));
	tm.addObserver(Observer<TaskObserver, TaskFailedNotification>(to, &TaskObserver::taskFailed));
	tm.addObserver(Observer<TaskObserver, TaskFinishedNotification>(to, &TaskObserver::taskFinished));
	tm.addObserver(Observer<TaskObserver, TaskProgressNotification>(to, &TaskObserver::taskProgress));
	AutoPtr<TestTask> pTT = new TestTask;
	tm.start(pTT.duplicate());
	assert (pTT->progress() == 0);
	Thread::sleep(200);
	pTT->cont();
	while (pTT->progress() != 0.5) Thread::sleep(50);
	assert (to.progress() == 0.5);
	assert (to.started());
	assert (pTT->state() == Task::TASK_RUNNING);
	TaskManager::TaskList list = tm.taskList();
	assert (list.size() == 1);
	assert (tm.count() == 1);
	pTT->fail();
	pTT->cont();
	while (pTT->state() != Task::TASK_FINISHED) Thread::sleep(50);
	assert (pTT->state() == Task::TASK_FINISHED);
	assert (to.finished());
	assert (to.error() != 0);
	while (tm.count() == 1) Thread::sleep(50);
	list = tm.taskList();
	assert (list.empty());
}


void TaskManagerTest::testCustom()
{
	TaskManager tm;
	
	CustomTaskObserver<int> ti(0);
	tm.addObserver(
		Observer<CustomTaskObserver<int>, TaskCustomNotification<int> >
			(ti, &CustomTaskObserver<int>::taskCustom));
	
	AutoPtr<CustomNotificationTask<int> > pCNT1 = new CustomNotificationTask<int>(0);
	tm.start(pCNT1.duplicate());
	assert (ti.custom() == 0);
	
	for (int i = 1; i < 10; ++i)
	{
		pCNT1->setCustom(i);
		assert (ti.custom() == i);
	}

	CustomTaskObserver<std::string> ts("");
	tm.addObserver(
		Observer<CustomTaskObserver<std::string>, TaskCustomNotification<std::string> >
			(ts, &CustomTaskObserver<std::string>::taskCustom));
	
	AutoPtr<CustomNotificationTask<std::string> > pCNT2 = new CustomNotificationTask<std::string>("");
	tm.start(pCNT2.duplicate());
	assert (tm.taskList().size() == 2);
	assert (ts.custom() == "");
	std::string str("notify me");
	pCNT2->setCustom(str);
	assert (ts.custom() == str);
	
	S s;
	s.i = 0;
	s.str = "";

	CustomTaskObserver<S*> ptst(&s);
	
	tm.addObserver(
		Observer<CustomTaskObserver<S*>, TaskCustomNotification<S*> >
			(ptst, &CustomTaskObserver<S*>::taskCustom));
	
	AutoPtr<CustomNotificationTask<S*> > pCNT3 = new CustomNotificationTask<S*>(&s);
	tm.start(pCNT3.duplicate());
	assert (tm.taskList().size() == 3);
	assert (ptst.custom()->i == 0);
	assert (ptst.custom()->str == "");
	s.i = 123;
	s.str = "123";
	pCNT3->setCustom(&s);
	assert (ptst.custom()->i == 123);
	assert (ptst.custom()->str == "123");

	s.i = 0;
	s.str = "";

	CustomTaskObserver<S> tst(s);
	
	tm.addObserver(
		Observer<CustomTaskObserver<S>, TaskCustomNotification<S> >
			(tst, &CustomTaskObserver<S>::taskCustom));
	
	AutoPtr<CustomNotificationTask<S> > pCNT4 = new CustomNotificationTask<S>(s);
	tm.start(pCNT4.duplicate());
	assert (tm.taskList().size() == 4);
	assert (tst.custom().i == 0);
	assert (tst.custom().str == "");
	s.i = 123;
	s.str = "123";
	pCNT4->setCustom(s);
	assert (tst.custom().i == 123);
	assert (tst.custom().str == "123");
	
	AutoPtr<SimpleTask> pST = new SimpleTask;
	tm.start(pST.duplicate());
	assert (tm.taskList().size() == 5);

	tm.cancelAll();
	while (tm.count() > 0) Thread::sleep(50);
	assert (tm.count() == 0);
}


void TaskManagerTest::testMultiTasks()
{
	TaskManager tm;
	tm.start(new SimpleTask);
	tm.start(new SimpleTask);
	tm.start(new SimpleTask);
	
	TaskManager::TaskList list = tm.taskList();
	assert (list.size() == 3);
	
	tm.cancelAll();
	while (tm.count() > 0) Thread::sleep(100);
	assert (tm.count() == 0);
}


void TaskManagerTest::testCustomThreadPool()
{
	ThreadPool  tp(2, 5, 120);
	TaskManager tm(tp);

	// fill up the thread pool
	for (int i=0; i < tp.capacity(); ++i)
	{
		tm.start(new SimpleTask);
	}
	assert (tp.allocated() == tp.capacity());
	assert (tm.count() == tp.allocated());

	// the next one should fail
	try
	{
		tm.start(new SimpleTask);
		failmsg("thread pool exhausted - must throw exception");
	}
	catch (NoThreadAvailableException const&)
	{
	}
	catch (...)
	{
		failmsg("wrong exception thrown");
	}

	assert (tm.count() == tp.allocated());
	
	tp.joinAll();
}

void TaskManagerTest::setUp()
{
}


void TaskManagerTest::tearDown()
{
}


CppUnit::Test* TaskManagerTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("TaskManagerTest");

	CppUnit_addTest(pSuite, TaskManagerTest, testFinish);
	CppUnit_addTest(pSuite, TaskManagerTest, testCancel);
	CppUnit_addTest(pSuite, TaskManagerTest, testError);
	CppUnit_addTest(pSuite, TaskManagerTest, testMultiTasks);
	CppUnit_addTest(pSuite, TaskManagerTest, testCustom);
	CppUnit_addTest(pSuite, TaskManagerTest, testCustomThreadPool);

	return pSuite;
}
