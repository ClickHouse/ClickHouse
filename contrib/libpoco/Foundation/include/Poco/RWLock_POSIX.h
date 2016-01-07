//
// RWLock_POSIX.h
//
// $Id: //poco/1.4/Foundation/include/Poco/RWLock_POSIX.h#1 $
//
// Library: Foundation
// Package: Threading
// Module:  RWLock
//
// Definition of the RWLockImpl class for POSIX Threads.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_RWLock_POSIX_INCLUDED
#define Foundation_RWLock_POSIX_INCLUDED


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
	pthread_rwlock_t _rwl;
};


//
// inlines
//
inline void RWLockImpl::readLockImpl()
{
	if (pthread_rwlock_rdlock(&_rwl)) 
		throw SystemException("cannot lock reader/writer lock");
}


inline bool RWLockImpl::tryReadLockImpl()
{
	int rc = pthread_rwlock_tryrdlock(&_rwl);
	if (rc == 0)
		return true;
	else if (rc == EBUSY)
		return false;
	else
		throw SystemException("cannot lock reader/writer lock");

}


inline void RWLockImpl::writeLockImpl()
{
	if (pthread_rwlock_wrlock(&_rwl)) 
		throw SystemException("cannot lock reader/writer lock");
}


inline bool RWLockImpl::tryWriteLockImpl()
{
	int rc = pthread_rwlock_trywrlock(&_rwl);
	if (rc == 0)
		return true;
	else if (rc == EBUSY)
		return false;
	else
		throw SystemException("cannot lock reader/writer lock");

}


inline void RWLockImpl::unlockImpl()
{
	if (pthread_rwlock_unlock(&_rwl))
		throw SystemException("cannot unlock mutex");
}


} // namespace Poco


#endif // Foundation_RWLock_POSIX_INCLUDED
