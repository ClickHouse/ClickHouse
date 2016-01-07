//
// Mutex_VX.cpp
//
// $Id: //poco/1.4/Foundation/src/Mutex_VX.cpp#1 $
//
// Library: Foundation
// Package: Threading
// Module:  Mutex
//
// Copyright (c) 2004-2011, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Mutex_VX.h"
#include <sysLib.h>


namespace Poco {


MutexImpl::MutexImpl()
{
	_sem = semMCreate(SEM_INVERSION_SAFE | SEM_Q_PRIORITY);
	if (_sem == 0)
		throw Poco::SystemException("cannot create mutex");
}


MutexImpl::MutexImpl(bool fast)
{
	if (fast)
	{
		_sem = semBCreate(SEM_Q_PRIORITY, SEM_FULL);
	}
	else
	{
		_sem = semMCreate(SEM_INVERSION_SAFE | SEM_Q_PRIORITY);
	}
	if (_sem == 0)
		throw Poco::SystemException("cannot create mutex");
}


MutexImpl::~MutexImpl()
{
	semDelete(_sem);
}


bool MutexImpl::tryLockImpl(long milliseconds)
{
	int ticks = milliseconds*sysClkRateGet()/1000;
	return semTake(_sem, ticks) == OK;
}


FastMutexImpl::FastMutexImpl(): MutexImpl(true)
{
}


FastMutexImpl::~FastMutexImpl()
{
}


} // namespace Poco
