//
// ScopedUnlock.h
//
// $Id: //poco/1.4/Foundation/include/Poco/ScopedUnlock.h#1 $
//
// Library: Foundation
// Package: Threading
// Module:  Mutex
//
// Definition of the ScopedUnlock template class.
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_ScopedUnlock_INCLUDED
#define Foundation_ScopedUnlock_INCLUDED


#include "Poco/Foundation.h"


namespace Poco {


template <class M>
class ScopedUnlock
	/// A class that simplifies thread synchronization
	/// with a mutex.
	/// The constructor accepts a Mutex and unlocks it.
	/// The destructor locks the mutex.
{
public:
	inline ScopedUnlock(M& mutex, bool unlockNow = true): _mutex(mutex)
	{
		if (unlockNow)
			_mutex.unlock();
	}
	inline ~ScopedUnlock()
	{
		try
		{
			_mutex.lock();
		}
		catch (...)
		{
			poco_unexpected();
		}
	}

private:
	M& _mutex;

	ScopedUnlock();
	ScopedUnlock(const ScopedUnlock&);
	ScopedUnlock& operator = (const ScopedUnlock&);
};


} // namespace Poco


#endif // Foundation_ScopedUnlock_INCLUDED
