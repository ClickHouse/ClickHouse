//
// Thread_VX.cpp
//
// $Id: //poco/1.4/Foundation/src/Thread_VX.cpp#1 $
//
// Library: Foundation
// Package: Threading
// Module:  Thread
//
// Copyright (c) 2004-2011, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Thread_VX.h"
#include "Poco/ErrorHandler.h"
#include "Poco/Exception.h"
#include "Poco/Timestamp.h"
#include "Poco/Timespan.h"
#include <timers.h>


namespace Poco {


ThreadImpl* ThreadImpl::_pCurrent(0);


ThreadImpl::ThreadImpl():
	_pData(new ThreadData)
{
}

			
ThreadImpl::~ThreadImpl()
{
}


void ThreadImpl::setPriorityImpl(int prio)
{
	if (prio != _pData->prio)
	{
		_pData->prio   = prio;
		_pData->osPrio = mapPrio(_pData->prio);
		if (isRunningImpl())
		{
			if (taskPrioritySet(_pData->task, _pData->osPrio) != OK)
				throw SystemException("cannot set task priority");
		}
	}
}


void ThreadImpl::setOSPriorityImpl(int prio, int /* policy */)
{
	if (prio != _pData->osPrio)
	{
		_pData->prio   = reverseMapPrio(prio);
		_pData->osPrio = prio;
		if (_pData->pRunnableTarget || _pData->pCallbackTarget)
		{
			if (taskPrioritySet(_pData->task, prio) != OK)
				throw SystemException("cannot set task priority");
		}
	}
}


int ThreadImpl::getMinOSPriorityImpl(int /* policy */)
{
	return 255;
}


int ThreadImpl::getMaxOSPriorityImpl(int /* policy */)
{
	return 0;
}


void ThreadImpl::setStackSizeImpl(int size)
{
	_pData->stackSize = size;
}


void ThreadImpl::startImpl(Runnable& target)
{
	if (_pData->pRunnableTarget)
		throw SystemException("thread already running");

	_pData->pRunnableTarget = &target;
	
	int stackSize = _pData->stackSize == 0 ? DEFAULT_THREAD_STACK_SIZE : _pData->stackSize;
	int id = taskSpawn(NULL, _pData->osPrio, VX_FP_TASK, stackSize, reinterpret_cast<FUNCPTR>(runnableEntry), reinterpret_cast<int>(this), 0, 0, 0, 0, 0, 0, 0, 0, 0);
	if (id == ERROR)
		throw SystemException("cannot spawn task");

	_pData->task = id;
}


void ThreadImpl::startImpl(Callable target, void* pData)
{
	if (_pData->pCallbackTarget && _pData->pCallbackTarget->callback)
		throw SystemException("thread already running");

	if (0 == _pData->pCallbackTarget.get())
		_pData->pCallbackTarget = new CallbackData;

	_pData->pCallbackTarget->callback = target;
	_pData->pCallbackTarget->pData = pData;

	int stackSize = _pData->stackSize == 0 ? DEFAULT_THREAD_STACK_SIZE : _pData->stackSize;
	int id = taskSpawn(NULL, _pData->osPrio, VX_FP_TASK, stackSize, reinterpret_cast<FUNCPTR>(callableEntry), reinterpret_cast<int>(this), 0, 0, 0, 0, 0, 0, 0, 0, 0);
	if (id == ERROR)
		throw SystemException("cannot spawn task");

	_pData->task = id;
}


void ThreadImpl::joinImpl()
{
	_pData->done.wait();
}


bool ThreadImpl::joinImpl(long milliseconds)
{
	return _pData->done.tryWait(milliseconds);
}


ThreadImpl* ThreadImpl::currentImpl()
{
	return _pCurrent;
}


ThreadImpl::TIDImpl ThreadImpl::currentTidImpl()
{
    return taskIdSelf();
}


void ThreadImpl::sleepImpl(long milliseconds)
{
	Poco::Timespan remainingTime(1000*Poco::Timespan::TimeDiff(milliseconds));
	int rc;
	do
	{
		struct timespec ts;
		ts.tv_sec  = (long) remainingTime.totalSeconds();
		ts.tv_nsec = (long) remainingTime.useconds()*1000;
		Poco::Timestamp start;
		rc = ::nanosleep(&ts, 0);
		if (rc < 0 && errno == EINTR)
		{
			Poco::Timestamp end;
			Poco::Timespan waited = start.elapsed();
			if (waited < remainingTime)
				remainingTime -= waited;
			else
				remainingTime = 0;
		}
	}
	while (remainingTime > 0 && rc < 0 && errno == EINTR);
	if (rc < 0 && remainingTime > 0) throw Poco::SystemException("Thread::sleep(): nanosleep() failed");
}


void ThreadImpl::runnableEntry(void* pThread, int, int, int, int, int, int, int, int, int)
{
	taskVarAdd(0, reinterpret_cast<int*>(&_pCurrent));
	_pCurrent = reinterpret_cast<ThreadImpl*>(pThread);

	AutoPtr<ThreadData> pData = _pCurrent->_pData;
	try
	{
		pData->pRunnableTarget->run();
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

	pData->pRunnableTarget = 0;
	pData->done.set();
}


void ThreadImpl::callableEntry(void* pThread, int, int, int, int, int, int, int, int, int)
{
	taskVarAdd(0, reinterpret_cast<int*>(&_pCurrent));
	_pCurrent = reinterpret_cast<ThreadImpl*>(pThread);

	AutoPtr<ThreadData> pData = _pCurrent->_pData;
	try
	{
		pData->pCallbackTarget->callback(pData->pCallbackTarget->pData);
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

	pData->pCallbackTarget->callback = 0;
	pData->pCallbackTarget->pData = 0;
	pData->done.set();
}


int ThreadImpl::mapPrio(int prio)
{
	int pmin = getMinOSPriorityImpl();
	int pmax = getMaxOSPriorityImpl();

	switch (prio)
	{
	case PRIO_LOWEST_IMPL:
		return pmin;
	case PRIO_LOW_IMPL:
		return pmin + (pmax - pmin)/4;
	case PRIO_NORMAL_IMPL:
		return pmin + (pmax - pmin)/2;
	case PRIO_HIGH_IMPL:
		return pmin + 3*(pmax - pmin)/4;
	case PRIO_HIGHEST_IMPL:
		return pmax;
	default:
		poco_bugcheck_msg("invalid thread priority");
	}
	return -1; // just to satisfy compiler - we'll never get here anyway
}


int ThreadImpl::reverseMapPrio(int prio)
{
	int pmin = getMinOSPriorityImpl();
	int pmax = getMaxOSPriorityImpl();
	int normal = pmin + (pmax - pmin)/2;
	if (prio == pmax)
		return PRIO_HIGHEST_IMPL;
	if (prio > normal)
		return PRIO_HIGH_IMPL;
	else if (prio == normal)
		return PRIO_NORMAL_IMPL;
	else if (prio > pmin)
		return PRIO_LOW_IMPL;
	else
		return PRIO_LOWEST_IMPL;
}


} // namespace Poco
