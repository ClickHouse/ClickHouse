//
// Mutex_WINCE.cpp
//
// $Id: //poco/1.4/Foundation/src/Mutex_WINCE.cpp#1 $
//
// Library: Foundation
// Package: Threading
// Module:  Mutex
//
// Copyright (c) 2004-2010, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Mutex_WINCE.h"


namespace Poco {


MutexImpl::MutexImpl()
{
	_mutex = CreateMutexW(NULL, FALSE, NULL);
	if (!_mutex) throw SystemException("cannot create mutex");
}


MutexImpl::~MutexImpl()
{
	CloseHandle(_mutex);
}


void MutexImpl::lockImpl()
{
	switch (WaitForSingleObject(_mutex, INFINITE))
	{
	case WAIT_OBJECT_0:
		return;
	default:
		throw SystemException("cannot lock mutex");
	}
}


bool MutexImpl::tryLockImpl()
{
	switch (WaitForSingleObject(_mutex, 0))
	{
	case WAIT_TIMEOUT:
		return false;
	case WAIT_OBJECT_0:
		return true;
	default:
		throw SystemException("cannot lock mutex");		
	}
}


bool MutexImpl::tryLockImpl(long milliseconds)
{
	switch (WaitForSingleObject(_mutex, milliseconds + 1))
	{
	case WAIT_TIMEOUT:
		return false;
	case WAIT_OBJECT_0:
		return true;
	default:
		throw SystemException("cannot lock mutex");		
	}
}


void MutexImpl::unlockImpl()
{
	ReleaseMutex(_mutex);
}


} // namespace Poco
