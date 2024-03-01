//
// Semaphore_WIN32.cpp
//
// Library: Foundation
// Package: Threading
// Module:  Semaphore
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Semaphore_WIN32.h"


namespace Poco {


SemaphoreImpl::SemaphoreImpl(int n, int max)
{
	poco_assert (n >= 0 && max > 0 && n <= max);

	_sema = CreateSemaphoreW(NULL, n, max, NULL);
	if (!_sema)
	{
		throw SystemException("cannot create semaphore");
	}
}


SemaphoreImpl::~SemaphoreImpl()
{
	CloseHandle(_sema);
}


void SemaphoreImpl::waitImpl()
{
	switch (WaitForSingleObject(_sema, INFINITE))
	{
	case WAIT_OBJECT_0:
		return;
	default:
		throw SystemException("wait for semaphore failed");
	}
}


bool SemaphoreImpl::waitImpl(long milliseconds)
{
	switch (WaitForSingleObject(_sema, milliseconds + 1))
	{
	case WAIT_TIMEOUT:
		return false;
	case WAIT_OBJECT_0:
		return true;
	default:
		throw SystemException("wait for semaphore failed");		
	}
}


} // namespace Poco
