//
// NamedMutex_WIN32.cpp
//
// Library: Foundation
// Package: Processes
// Module:  NamedMutex
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/NamedMutex_WIN32.h"
#include "Poco/Exception.h"


namespace Poco {


NamedMutexImpl::NamedMutexImpl(const std::string& name):
	_name(name)
{
	_mutex = CreateMutexA(NULL, FALSE, _name.c_str());
	if (!_mutex) 
		throw SystemException("cannot create named mutex", _name);
}


NamedMutexImpl::~NamedMutexImpl()
{
	CloseHandle(_mutex);
}


void NamedMutexImpl::lockImpl()
{
	switch (WaitForSingleObject(_mutex, INFINITE))
	{
	case WAIT_OBJECT_0:
		return;
	case WAIT_ABANDONED:
		throw SystemException("cannot lock named mutex (abadoned)", _name);
	default:
		throw SystemException("cannot lock named mutex", _name);
	}
}


bool NamedMutexImpl::tryLockImpl()
{
	switch (WaitForSingleObject(_mutex, 0))
	{
	case WAIT_OBJECT_0:
		return true;
	case WAIT_TIMEOUT:
		return false;
	case WAIT_ABANDONED:
		throw SystemException("cannot lock named mutex (abadoned)", _name);
	default:
		throw SystemException("cannot lock named mutex", _name);
	}
}


void NamedMutexImpl::unlockImpl()
{
	ReleaseMutex(_mutex);
}


} // namespace Poco
