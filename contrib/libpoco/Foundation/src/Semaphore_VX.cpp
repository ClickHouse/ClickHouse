//
// Semaphore_VX.cpp
//
// $Id: //poco/1.4/Foundation/src/Semaphore_VX.cpp#1 $
//
// Library: Foundation
// Package: Threading
// Module:  Semaphore
//
// Copyright (c) 2004-2011, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Semaphore_VX.h"
#include <sysLib.h>


namespace Poco {


SemaphoreImpl::SemaphoreImpl(int n, int max)
{
	poco_assert (n >= 0 && max > 0 && n <= max);

	_sem = semCCreate(SEM_Q_PRIORITY, n);
	if (_sem == 0)
		throw Poco::SystemException("cannot create semaphore");
}


SemaphoreImpl::~SemaphoreImpl()
{
	semDelete(_sem);
}


void SemaphoreImpl::waitImpl()
{
	if (semTake(_sem, WAIT_FOREVER) != OK)
		throw SystemException("cannot wait for semaphore");
}


bool SemaphoreImpl::waitImpl(long milliseconds)
{
	int ticks = milliseconds*sysClkRateGet()/1000;
	return semTake(_sem, ticks) == OK;
}


} // namespace Poco
