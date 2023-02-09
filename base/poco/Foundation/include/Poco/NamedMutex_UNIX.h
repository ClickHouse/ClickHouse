//
// NamedMutex_UNIX.h
//
// Library: Foundation
// Package: Processes
// Module:  NamedMutex
//
// Definition of the NamedMutexImpl class for Unix.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_NamedMutex_UNIX_INCLUDED
#define Foundation_NamedMutex_UNIX_INCLUDED


#include "Poco/Foundation.h"
#include <sys/types.h>
#include <sys/stat.h>
#if defined(sun) || defined(__APPLE__) || defined(__osf__) || defined(__QNX__) || defined(_AIX)
#include <semaphore.h>
#endif


namespace Poco {


class Foundation_API NamedMutexImpl
{
protected:
	NamedMutexImpl(const std::string& name);
	~NamedMutexImpl();
	void lockImpl();
	bool tryLockImpl();
	void unlockImpl();
	
private:
	std::string getFileName();

	std::string _name;
#if defined(sun) || defined(__APPLE__) || defined(__osf__) || defined(__QNX__) || defined(_AIX)
	sem_t* _sem;
#else
	int _semid;  // semaphore id
	bool _owned;
#endif
};


} // namespace Poco


#endif // Foundation_NamedMutex_UNIX_INCLUDED
