//
// RWLock_Android.h
//
// Library: Foundation
// Package: Threading
// Module:  RWLock
//
// Definition of the RWLockImpl class for Android Threads.
//
// Copyright (c) 2004-2011, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_RWLock_Android_INCLUDED
#define Foundation_RWLock_Android_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Exception.h"
#include <pthread.h>
#include <errno.h>


namespace Poco {


class Foundation_API RWLockImpl
{
protected:
	RWLockImpl();
	~RWLockImpl();
	void readLockImpl();
	bool tryReadLockImpl();
	void writeLockImpl();
	bool tryWriteLockImpl();
	void unlockImpl();
	
private:
	pthread_mutex_t _mutex;
};


//
// inlines
//
inline void RWLockImpl::readLockImpl()
{
	if (pthread_mutex_lock(&_mutex)) 
		throw SystemException("cannot lock reader/writer lock");
}


inline bool RWLockImpl::tryReadLockImpl()
{
	int rc = pthread_mutex_trylock(&_mutex);
	if (rc == 0)
		return true;
	else if (rc == EBUSY)
		return false;
	else
		throw SystemException("cannot lock reader/writer lock");
}


inline void RWLockImpl::writeLockImpl()
{
	if (pthread_mutex_lock(&_mutex)) 
		throw SystemException("cannot lock reader/writer lock");
}


inline bool RWLockImpl::tryWriteLockImpl()
{
	int rc = pthread_mutex_trylock(&_mutex);
	if (rc == 0)
		return true;
	else if (rc == EBUSY)
		return false;
	else
		throw SystemException("cannot lock reader/writer lock");
}


inline void RWLockImpl::unlockImpl()
{
	if (pthread_mutex_unlock(&_mutex))
		throw SystemException("cannot unlock reader/writer lock");
}


} // namespace Poco


#endif // Foundation_RWLock_Android_INCLUDED
