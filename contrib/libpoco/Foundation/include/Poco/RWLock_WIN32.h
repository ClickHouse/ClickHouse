//
// RWLock_WIN32.h
//
// $Id: //poco/1.4/Foundation/include/Poco/RWLock_WIN32.h#1 $
//
// Library: Foundation
// Package: Threading
// Module:  RWLock
//
// Definition of the RWLockImpl class for WIN32.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_RWLock_WIN32_INCLUDED
#define Foundation_RWLock_WIN32_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Exception.h"
#include "Poco/UnWindows.h"


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
	void addWriter();
	void removeWriter();
	DWORD tryReadLockOnce();

	HANDLE   _mutex;
	HANDLE   _readEvent;
	HANDLE   _writeEvent;
	unsigned _readers;
	unsigned _writersWaiting;
	unsigned _writers;
};


} // namespace Poco


#endif // Foundation_RWLock_WIN32_INCLUDED
