//
// ScopedLock.h
//
// Library: Foundation
// Package: Threading
// Module:  Mutex
//
// Definition of the ScopedLock template class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_ScopedLock_INCLUDED
#define Foundation_ScopedLock_INCLUDED


#include "Poco/Foundation.h"


namespace Poco {


template <class M>
class ScopedLock
	/// A class that simplifies thread synchronization
	/// with a mutex.
	/// The constructor accepts a Mutex (and optionally
	/// a timeout value in milliseconds) and locks it.
	/// The destructor unlocks the mutex.
{
public:
	explicit ScopedLock(M& mutex): _mutex(mutex)
	{
		_mutex.lock();
	}
	
	ScopedLock(M& mutex, long milliseconds): _mutex(mutex)
	{
		_mutex.lock(milliseconds);
	}
	
	~ScopedLock()
	{
		try
		{
			_mutex.unlock();
		}
		catch (...)
		{
			poco_unexpected();
		}
	}

private:
	M& _mutex;

	ScopedLock();
	ScopedLock(const ScopedLock&);
	ScopedLock& operator = (const ScopedLock&);
};


template <class M>
class ScopedLockWithUnlock
	/// A class that simplifies thread synchronization
	/// with a mutex.
	/// The constructor accepts a Mutex (and optionally
	/// a timeout value in milliseconds) and locks it.
	/// The destructor unlocks the mutex.
	/// The unlock() member function allows for manual
	/// unlocking of the mutex.
{
public:
	explicit ScopedLockWithUnlock(M& mutex): _pMutex(&mutex)
	{
		_pMutex->lock();
	}
	
	ScopedLockWithUnlock(M& mutex, long milliseconds): _pMutex(&mutex)
	{
		_pMutex->lock(milliseconds);
	}
	
	~ScopedLockWithUnlock()
	{
		try
		{
			unlock();
		}
		catch (...)
		{
			poco_unexpected();
		}
	}
	
	void unlock()
	{
		if (_pMutex)
		{
			_pMutex->unlock();
			_pMutex = 0;
		}
	}

private:
	M* _pMutex;

	ScopedLockWithUnlock();
	ScopedLockWithUnlock(const ScopedLockWithUnlock&);
	ScopedLockWithUnlock& operator = (const ScopedLockWithUnlock&);
};


} // namespace Poco


#endif // Foundation_ScopedLock_INCLUDED
