//
// RWLock_POSIX.cpp
//
// Library: Foundation
// Package: Threading
// Module:  RWLock
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/RWLock_POSIX.h"


namespace Poco {


RWLockImpl::RWLockImpl()
{
	if (pthread_rwlock_init(&_rwl, NULL))
		throw SystemException("cannot create reader/writer lock");
}


RWLockImpl::~RWLockImpl()
{
	pthread_rwlock_destroy(&_rwl);
}


} // namespace Poco
