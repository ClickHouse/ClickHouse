//
// RWLock_Android.cpp
//
// Library: Foundation
// Package: Threading
// Module:  RWLock
//
// Copyright (c) 2004-2011, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/RWLock_Android.h"


namespace Poco {


RWLockImpl::RWLockImpl()
{
	pthread_mutexattr_t attr;
	pthread_mutexattr_init(&attr);
	if (pthread_mutex_init(&_mutex, &attr))
	{
		pthread_mutexattr_destroy(&attr);
		throw SystemException("cannot create mutex");
	}
	pthread_mutexattr_destroy(&attr);}


RWLockImpl::~RWLockImpl()
{
	pthread_mutex_destroy(&_mutex);
}


} // namespace Poco
