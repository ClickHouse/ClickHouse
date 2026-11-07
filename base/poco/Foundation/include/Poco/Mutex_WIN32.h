//
// Mutex_WIN32.h
//
// Library: Foundation
// Package: Threading
// Module:  Mutex
//
// Definition of the MutexImpl and FastMutexImpl classes for WIN32.
//
// Copyright (c) 2004-2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_Mutex_WIN32_INCLUDED
#define Foundation_Mutex_WIN32_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Exception.h"
#include "Poco/UnWindows.h"
#include <chrono>
#include <mutex>


namespace Poco {


class Foundation_API MutexImpl
{
protected:
	MutexImpl();
	~MutexImpl();
	void lockImpl();
	bool tryLockImpl();
	bool tryLockImpl(long milliseconds);
	void unlockImpl();

private:
	CRITICAL_SECTION _cs;
};


/// Upstream Poco aliases this to `MutexImpl`, but a `CRITICAL_SECTION` is recursive, while `FastMutex`
/// is documented as not recursive and is not on POSIX (`PTHREAD_MUTEX_NORMAL`). The alias would let a
/// same-thread reentry succeed on Windows only, hiding there the reentrancy and lock-order bugs that
/// POSIX surfaces as a deadlock. `std::timed_mutex` is not recursive, and it also gives the timed
/// `tryLockImpl` a real timed wait instead of the polling loop of `MutexImpl`.
class Foundation_API FastMutexImpl
{
protected:
	void lockImpl();
	bool tryLockImpl();
	bool tryLockImpl(long milliseconds);
	void unlockImpl();

private:
	std::timed_mutex _mutex;
};


//
// inlines
//
inline void MutexImpl::lockImpl()
{
	try
	{
		EnterCriticalSection(&_cs);
	}
	catch (...)
	{
		throw SystemException("cannot lock mutex");
	}
}


inline bool MutexImpl::tryLockImpl()
{
	try
	{
		return TryEnterCriticalSection(&_cs) != 0;
	}
	catch (...)
	{
	}
	throw SystemException("cannot lock mutex");
}


inline void MutexImpl::unlockImpl()
{
	LeaveCriticalSection(&_cs);
}


inline void FastMutexImpl::lockImpl()
{
	try
	{
		_mutex.lock();
	}
	catch (...)
	{
		throw SystemException("cannot lock mutex");
	}
}


inline bool FastMutexImpl::tryLockImpl()
{
	return _mutex.try_lock();
}


inline bool FastMutexImpl::tryLockImpl(long milliseconds)
{
	try
	{
		return _mutex.try_lock_for(std::chrono::milliseconds(milliseconds));
	}
	catch (...)
	{
		throw SystemException("cannot lock mutex");
	}
}


inline void FastMutexImpl::unlockImpl()
{
	_mutex.unlock();
}


} // namespace Poco


#endif // Foundation_Mutex_WIN32_INCLUDED
