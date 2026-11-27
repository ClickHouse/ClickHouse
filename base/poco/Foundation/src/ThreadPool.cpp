//
// ThreadPool.cpp
//
// Library: Foundation
// Package: Threading
// Module:  ThreadPool
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/ThreadPool.h"
#include "Poco/Runnable.h"
#include "Poco/Thread.h"
#include "Poco/Event.h"
#include "Poco/ThreadLocal.h"
#include "Poco/ErrorHandler.h"
#include <sstream>
#include <ctime>
#include <Common/ThreadPool.h>


namespace Poco {


class PooledThread: public Runnable
{
public:
	explicit PooledThread(
		const std::string& name,
		int stackSize = POCO_THREAD_STACK_SIZE,
        size_t globalProfilerRealTimePeriodNs_ = 0,
        size_t globalProfilerCPUTimePeriodNs_ = 0);
	~PooledThread();

	void start();
	void start(Thread::Priority priority, Runnable& target);
	void start(Thread::Priority priority, Runnable& target, const std::string& name);
	bool idle();
	int idleTime();
	void join();
	void activate();
	void release();
	void run();

private:
	volatile bool        _idle;
	volatile std::time_t _idleTime;
	Runnable*            _pTarget;
	std::string          _name;
	Thread               _thread;
	Event                _targetReady;
	Event                _targetCompleted;
	Event                _started;
	FastMutex            _mutex;
	size_t _globalProfilerRealTimePeriodNs;
	size_t _globalProfilerCPUTimePeriodNs;
};


PooledThread::PooledThread(
	const std::string& name,
	int stackSize,
	size_t globalProfilerRealTimePeriodNs_,
	size_t globalProfilerCPUTimePeriodNs_) :
	_idle(true),
	_idleTime(0),
	_pTarget(0),
	_name(name),
	_thread(name),
	_targetCompleted(false),
	_globalProfilerRealTimePeriodNs(globalProfilerRealTimePeriodNs_),
	_globalProfilerCPUTimePeriodNs(globalProfilerCPUTimePeriodNs_)
{
	poco_assert_dbg (stackSize >= 0);
	_thread.setStackSize(stackSize);
	_idleTime = std::time(NULL);
}


PooledThread::~PooledThread()
{
}


void PooledThread::start()
{
	_thread.start(*this);
	_started.wait();
}


void PooledThread::start(Thread::Priority priority, Runnable& target)
{
	FastMutex::ScopedLock lock(_mutex);

	poco_assert (_pTarget == 0);

	_pTarget = &target;
	_thread.setPriority(priority);
	_targetReady.set();
}


void PooledThread::start(Thread::Priority priority, Runnable& target, const std::string& name)
{
	FastMutex::ScopedLock lock(_mutex);

	std::string fullName(name);
	if (name.empty())
	{
		fullName = _name;
	}
	else
	{
		fullName.append(" (");
		fullName.append(_name);
		fullName.append(")");
	}
	_thread.setName(fullName);
	_thread.setPriority(priority);

	poco_assert (_pTarget == 0);

	_pTarget = &target;
	_targetReady.set();
}


inline bool PooledThread::idle()
{
	FastMutex::ScopedLock lock(_mutex);
	return _idle;
}


int PooledThread::idleTime()
{
	FastMutex::ScopedLock lock(_mutex);

	return (int) (time(NULL) - _idleTime);
}


void PooledThread::join()
{
	_mutex.lock();
	Runnable* pTarget = _pTarget;
	_mutex.unlock();
	if (pTarget)
		_targetCompleted.wait();
}


void PooledThread::activate()
{
	FastMutex::ScopedLock lock(_mutex);

	poco_assert (_idle);
	_idle = false;
	_targetCompleted.reset();
}


void PooledThread::release()
{
	const long JOIN_TIMEOUT = 10000;

	_mutex.lock();
	_pTarget = 0;
	_mutex.unlock();
	// In case of a statically allocated thread pool (such
	// as the default thread pool), Windows may have already
	// terminated the thread before we got here.
	if (_thread.isRunning())
		_targetReady.set();

	if (_thread.tryJoin(JOIN_TIMEOUT))
	{
		delete this;
	}
}


void PooledThread::run()
{
    DB::ThreadStatus thread_status;
	if (unlikely(_globalProfilerRealTimePeriodNs != 0 || _globalProfilerCPUTimePeriodNs != 0))
		thread_status.initGlobalProfiler(_globalProfilerRealTimePeriodNs, _globalProfilerCPUTimePeriodNs);

	_started.set();
	for (;;)
	{
		_targetReady.wait();
		_mutex.lock();
		if (_pTarget) // a NULL target means kill yourself
		{
			Runnable* pTarget = _pTarget;
			_mutex.unlock();
			try
			{
				pTarget->run();
			}
			catch (Exception& exc)
			{
				ErrorHandler::handle(exc);
			}
			catch (std::exception& exc)
			{
				ErrorHandler::handle(exc);
			}
			catch (...)
			{
				ErrorHandler::handle();
			}
			FastMutex::ScopedLock lock(_mutex);
			_pTarget  = 0;
			_idleTime = time(NULL);
			_idle     = true;
			_targetCompleted.set();
			ThreadLocalStorage::clear();
			_thread.setName(_name);
			_thread.setPriority(Thread::PRIO_NORMAL);
		}
		else
		{
			_mutex.unlock();
			break;
		}
	}
}


ThreadPool::ThreadPool(int minCapacity,
	int maxCapacity,
	int idleTime,
	int stackSize,
	size_t globalProfilerRealTimePeriodNs_,
	size_t globalProfilerCPUTimePeriodNs_) :
	_minCapacity(minCapacity),
	_maxCapacity(maxCapacity),
	_idleTime(idleTime),
	_serial(0),
	_age(0),
	_stackSize(stackSize),
    _globalProfilerRealTimePeriodNs(globalProfilerRealTimePeriodNs_),
    _globalProfilerCPUTimePeriodNs(globalProfilerCPUTimePeriodNs_)
{
	poco_assert (minCapacity >= 1 && maxCapacity >= minCapacity && idleTime > 0);

	for (int i = 0; i < _minCapacity; i++)
	{
		PooledThread* pThread = createThread();
		_threads.push_back(pThread);
		pThread->start();
	}
}


ThreadPool::ThreadPool(const std::string& name,
	int minCapacity,
	int maxCapacity,
	int idleTime,
	int stackSize,
	size_t globalProfilerRealTimePeriodNs_,
	size_t globalProfilerCPUTimePeriodNs_) :
	_name(name),
	_minCapacity(minCapacity),
	_maxCapacity(maxCapacity),
	_idleTime(idleTime),
	_serial(0),
	_age(0),
	_stackSize(stackSize),
    _globalProfilerRealTimePeriodNs(globalProfilerRealTimePeriodNs_),
    _globalProfilerCPUTimePeriodNs(globalProfilerCPUTimePeriodNs_)
{
	poco_assert (minCapacity >= 1 && maxCapacity >= minCapacity && idleTime > 0);

	for (int i = 0; i < _minCapacity; i++)
	{
		PooledThread* pThread = createThread();
		_threads.push_back(pThread);
		pThread->start();
	}
}


ThreadPool::~ThreadPool()
{
	try
	{
		stopAll();
	}
	catch (...)
	{
		poco_unexpected();
	}
}


void ThreadPool::addCapacity(int n)
{
	FastMutex::ScopedLock lock(_mutex);

	poco_assert (_maxCapacity + n >= _minCapacity);
	_maxCapacity += n;
	housekeep();
}


int ThreadPool::capacity() const
{
	FastMutex::ScopedLock lock(_mutex);
	return _maxCapacity;
}


int ThreadPool::available() const
{
	FastMutex::ScopedLock lock(_mutex);

	int count = 0;
	for (ThreadVec::const_iterator it = _threads.begin(); it != _threads.end(); ++it)
	{
		if ((*it)->idle()) ++count;
	}
	return (int) (count + _maxCapacity - _threads.size());
}


int ThreadPool::used() const
{
	FastMutex::ScopedLock lock(_mutex);

	int count = 0;
	for (ThreadVec::const_iterator it = _threads.begin(); it != _threads.end(); ++it)
	{
		if (!(*it)->idle()) ++count;
	}
	return count;
}


int ThreadPool::allocated() const
{
	FastMutex::ScopedLock lock(_mutex);

	return int(_threads.size());
}


void ThreadPool::start(Runnable& target)
{
	getThread()->start(Thread::PRIO_NORMAL, target);
}


void ThreadPool::start(Runnable& target, const std::string& name)
{
	getThread()->start(Thread::PRIO_NORMAL, target, name);
}


void ThreadPool::startWithPriority(Thread::Priority priority, Runnable& target)
{
	getThread()->start(priority, target);
}


void ThreadPool::startWithPriority(Thread::Priority priority, Runnable& target, const std::string& name)
{
	getThread()->start(priority, target, name);
}


void ThreadPool::stopAll()
{
	FastMutex::ScopedLock lock(_mutex);

	for (ThreadVec::iterator it = _threads.begin(); it != _threads.end(); ++it)
	{
		(*it)->release();
	}
	_threads.clear();
}


void ThreadPool::joinAll()
{
	FastMutex::ScopedLock lock(_mutex);

	for (ThreadVec::iterator it = _threads.begin(); it != _threads.end(); ++it)
	{
		(*it)->join();
	}
	housekeep();
}


void ThreadPool::collect()
{
	FastMutex::ScopedLock lock(_mutex);
	housekeep();
}


void ThreadPool::housekeep()
{
	_age = 0;
	if (_threads.size() <= _minCapacity)
		return;

	ThreadVec idleThreads;
	ThreadVec expiredThreads;
	ThreadVec activeThreads;
	idleThreads.reserve(_threads.size());
	activeThreads.reserve(_threads.size());

	for (ThreadVec::iterator it = _threads.begin(); it != _threads.end(); ++it)
	{
		if ((*it)->idle())
		{
			if ((*it)->idleTime() < _idleTime)
				idleThreads.push_back(*it);
			else
				expiredThreads.push_back(*it);
		}
		else activeThreads.push_back(*it);
	}
	int n = (int) activeThreads.size();
	int limit = (int) idleThreads.size() + n;
	if (limit < _minCapacity) limit = _minCapacity;
	idleThreads.insert(idleThreads.end(), expiredThreads.begin(), expiredThreads.end());
	_threads.clear();
	for (ThreadVec::iterator it = idleThreads.begin(); it != idleThreads.end(); ++it)
	{
		if (n < limit)
		{
			_threads.push_back(*it);
			++n;
		}
		else (*it)->release();
	}
	_threads.insert(_threads.end(), activeThreads.begin(), activeThreads.end());
}


PooledThread* ThreadPool::getThread()
{
	FastMutex::ScopedLock lock(_mutex);

	if (++_age == 32)
		housekeep();

	PooledThread* pThread = 0;
	for (ThreadVec::iterator it = _threads.begin(); !pThread && it != _threads.end(); ++it)
	{
		if ((*it)->idle())
			pThread = *it;
	}
	if (!pThread)
	{
		if (_threads.size() < _maxCapacity)
		{
			pThread = createThread();
			try
			{
				pThread->start();
				_threads.push_back(pThread);
			} catch (...)
			{
				delete pThread;
				throw;
			}
		}
		else
			throw NoThreadAvailableException();
	}
	pThread->activate();
	return pThread;
}


PooledThread* ThreadPool::createThread()
{
	std::ostringstream name;
	name << _name << "[#" << ++_serial << "]";
	return new PooledThread(name.str(), _stackSize, _globalProfilerRealTimePeriodNs, _globalProfilerCPUTimePeriodNs);
}


class ThreadPoolSingletonHolder
{
public:
	ThreadPoolSingletonHolder()
	{
		_pPool = 0;
	}
	~ThreadPoolSingletonHolder()
	{
		delete _pPool;
	}
	ThreadPool* pool()
	{
		FastMutex::ScopedLock lock(_mutex);

		if (!_pPool)
		{
			_pPool = new ThreadPool("default");
			if (POCO_THREAD_STACK_SIZE > 0)
				_pPool->setStackSize(POCO_THREAD_STACK_SIZE);
		}
		return _pPool;
	}

private:
	ThreadPool* _pPool;
	FastMutex   _mutex;
};


namespace
{
	static ThreadPoolSingletonHolder sh;
}


ThreadPool& ThreadPool::defaultPool()
{
	return *sh.pool();
}


} // namespace Poco
