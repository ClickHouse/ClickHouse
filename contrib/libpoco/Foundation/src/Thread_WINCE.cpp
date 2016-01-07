//
// Thread_WINCE.h
//
// $Id: //poco/1.4/Foundation/src/Thread_WINCE.cpp#1 $
//
// Library: Foundation
// Package: Threading
// Module:  Thread
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Thread_WINCE.h"
#include "Poco/Exception.h"
#include "Poco/ErrorHandler.h"


namespace Poco {


ThreadImpl::CurrentThreadHolder ThreadImpl::_currentThreadHolder;


ThreadImpl::ThreadImpl():
	_pRunnableTarget(0),
	_thread(0),
	_threadId(0),
	_prio(PRIO_NORMAL_IMPL),
	_stackSize(POCO_THREAD_STACK_SIZE)
{
}

			
ThreadImpl::~ThreadImpl()
{
	if (_thread) CloseHandle(_thread);
}


void ThreadImpl::setPriorityImpl(int prio)
{
	if (prio != _prio)
	{
		_prio = prio;
		if (_thread)
		{
			if (SetThreadPriority(_thread, _prio) == 0)
				throw SystemException("cannot set thread priority");
		}
	}
}


void ThreadImpl::setOSPriorityImpl(int prio, int /* policy */)
{
	setPriorityImpl(prio);
}


void ThreadImpl::startImpl(SharedPtr<Runnable> pTarget)
{
	if (isRunningImpl())
		throw SystemException("thread already running");

	_pRunnableTarget = pTarget;

	createImpl(runnableEntry, this);
}


void ThreadImpl::createImpl(Entry ent, void* pData)
{
	_thread = CreateThread(NULL, _stackSize, ent, pData, 0, &_threadId);

	if (!_thread)
		throw SystemException("cannot create thread");
	if (_prio != PRIO_NORMAL_IMPL && !SetThreadPriority(_thread, _prio))
		throw SystemException("cannot set thread priority");
}


void ThreadImpl::joinImpl()
{
	if (!_thread) return;

	switch (WaitForSingleObject(_thread, INFINITE))
	{
	case WAIT_OBJECT_0:
		threadCleanup();
		return;
	default:
		throw SystemException("cannot join thread");
	}
}


bool ThreadImpl::joinImpl(long milliseconds)
{
	if (!_thread) return true;

	switch (WaitForSingleObject(_thread, milliseconds + 1))
	{
	case WAIT_TIMEOUT:
		return false;
	case WAIT_OBJECT_0:
		threadCleanup();
		return true;
	default:
		throw SystemException("cannot join thread");
	}
}


bool ThreadImpl::isRunningImpl() const
{
	if (_thread)
	{
		DWORD ec = 0;
		return GetExitCodeThread(_thread, &ec) && ec == STILL_ACTIVE;
	}
	return false;
}


void ThreadImpl::threadCleanup()
{
	if (!_thread) return;
	if (CloseHandle(_thread)) _thread = 0;
}


ThreadImpl* ThreadImpl::currentImpl()
{
	return _currentThreadHolder.get();
}


ThreadImpl::TIDImpl ThreadImpl::currentTidImpl()
{
    return GetCurrentThreadId();
}


DWORD WINAPI ThreadImpl::runnableEntry(LPVOID pThread)
{
	_currentThreadHolder.set(reinterpret_cast<ThreadImpl*>(pThread));
	try
	{
		reinterpret_cast<ThreadImpl*>(pThread)->_pRunnableTarget->run();
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
	return 0;
}


} // namespace Poco
