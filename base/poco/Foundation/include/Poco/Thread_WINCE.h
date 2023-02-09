//
// Thread_WINCE.h
//
// Library: Foundation
// Package: Threading
// Module:  Thread
//
// Definition of the ThreadImpl class for WIN32.
//
// Copyright (c) 2004-2010, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_Thread_WINCE_INCLUDED
#define Foundation_Thread_WINCE_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Runnable.h"
#include "Poco/SharedPtr.h"
#include "Poco/UnWindows.h"


#if !defined(TLS_OUT_OF_INDEXES) // Windows CE 5.x does not define this
#define TLS_OUT_OF_INDEXES 0xFFFFFFFF
#endif


namespace Poco {


class Foundation_API ThreadImpl
{
public:	
    typedef DWORD TIDImpl;
	typedef void (*Callable)(void*);
	typedef DWORD (WINAPI *Entry)(LPVOID);

	enum Priority
	{
		PRIO_LOWEST_IMPL  = THREAD_PRIORITY_LOWEST,
		PRIO_LOW_IMPL     = THREAD_PRIORITY_BELOW_NORMAL,
		PRIO_NORMAL_IMPL  = THREAD_PRIORITY_NORMAL,
		PRIO_HIGH_IMPL    = THREAD_PRIORITY_ABOVE_NORMAL,
		PRIO_HIGHEST_IMPL = THREAD_PRIORITY_HIGHEST
	};

	enum Policy
	{
		POLICY_DEFAULT_IMPL = 0
	};

	ThreadImpl();				
	~ThreadImpl();

	TIDImpl tidImpl() const;
	void setPriorityImpl(int prio);
	int getPriorityImpl() const;
	void setOSPriorityImpl(int prio, int policy = 0);
	int getOSPriorityImpl() const;
	static int getMinOSPriorityImpl(int policy);
	static int getMaxOSPriorityImpl(int policy);
	void setStackSizeImpl(int size);
	int getStackSizeImpl() const;
	void startImpl(SharedPtr<Runnable> pTarget);
	void joinImpl();
	bool joinImpl(long milliseconds);
	bool isRunningImpl() const;
	static void sleepImpl(long milliseconds);
	static void yieldImpl();
	static ThreadImpl* currentImpl();
	static TIDImpl currentTidImpl();

protected:
	static DWORD WINAPI runnableEntry(LPVOID pThread);

	void createImpl(Entry ent, void* pData);
	void threadCleanup();

private:
	class CurrentThreadHolder
	{
	public:
		CurrentThreadHolder(): _slot(TlsAlloc())
		{
			if (_slot == TLS_OUT_OF_INDEXES)
				throw SystemException("cannot allocate thread context key");
		}
		~CurrentThreadHolder()
		{
			TlsFree(_slot);
		}
		ThreadImpl* get() const
		{
			return reinterpret_cast<ThreadImpl*>(TlsGetValue(_slot));
		}
		void set(ThreadImpl* pThread)
		{
			TlsSetValue(_slot, pThread);
		}
	
	private:
		DWORD _slot;
	};

	SharedPtr<Runnable> _pRunnableTarget;
	HANDLE       _thread;
	DWORD        _threadId;
	int          _prio;
	int          _stackSize;

	static CurrentThreadHolder _currentThreadHolder;
};


//
// inlines
//
inline int ThreadImpl::getPriorityImpl() const
{
	return _prio;
}


inline int ThreadImpl::getOSPriorityImpl() const
{
	return _prio;
}


inline int ThreadImpl::getMinOSPriorityImpl(int /* policy */)
{
	return PRIO_LOWEST_IMPL;
}


inline int ThreadImpl::getMaxOSPriorityImpl(int /* policy */)
{
	return PRIO_HIGHEST_IMPL;
}


inline void ThreadImpl::sleepImpl(long milliseconds)
{
	Sleep(DWORD(milliseconds));
}


inline void ThreadImpl::yieldImpl()
{
	Sleep(0);
}


inline void ThreadImpl::setStackSizeImpl(int size)
{
	_stackSize = size;
}


inline int ThreadImpl::getStackSizeImpl() const
{
	return _stackSize;
}


inline ThreadImpl::TIDImpl ThreadImpl::tidImpl() const
{
	return _threadId;
}


} // namespace Poco


#endif // Foundation_Thread_WINCE_INCLUDED
