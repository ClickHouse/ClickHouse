//
// Mutex_WIN32.h
//
// $Id: //poco/1.4/Foundation/include/Poco/Mutex_WINCE.h#1 $
//
// Library: Foundation
// Package: Threading
// Module:  Mutex
//
// Definition of the MutexImpl and FastMutexImpl classes for WIN32.
//
// Copyright (c) 2004-2010, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_Mutex_WINCE_INCLUDED
#define Foundation_Mutex_WINCE_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Exception.h"
#include "Poco/UnWindows.h"


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
	HANDLE _mutex;
};


typedef MutexImpl FastMutexImpl;


} // namespace Poco


#endif // Foundation_Mutex_WINCE_INCLUDED
