//
// NamedMutex_Android.cpp
//
// Library: Foundation
// Package: Processes
// Module:  NamedMutex
//
// Copyright (c) 2004-2011, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/NamedMutex_Android.h"
#include "Poco/Exception.h"


namespace Poco {


NamedMutexImpl::NamedMutexImpl(const std::string&)
{
}


NamedMutexImpl::~NamedMutexImpl()
{
}


void NamedMutexImpl::lockImpl()
{
	throw NotImplementedException("NamedMutex::lock() is not supported on Android");
}


bool NamedMutexImpl::tryLockImpl()
{
	throw NotImplementedException("NamedMutex::tryLock() is not supported on Android");
}


void NamedMutexImpl::unlockImpl()
{
	throw NotImplementedException("NamedMutex::unlock() is not supported on Android");
}


} // namespace Poco
