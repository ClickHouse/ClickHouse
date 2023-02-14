//
// Mutex.cpp
//
// Library: Foundation
// Package: Threading
// Module:  Mutex
//
// Copyright (c) 2004-2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Mutex.h"


#if defined(POCO_OS_FAMILY_WINDOWS)
#if defined(_WIN32_WCE)
#include "Mutex_WINCE.cpp"
#else
#include "Mutex_WIN32.cpp"
#endif
#else
#include "Mutex_POSIX.cpp"
#endif


namespace Poco {


Mutex::Mutex()
{
}


Mutex::~Mutex()
{
}


FastMutex::FastMutex()
{
}


FastMutex::~FastMutex()
{
}


} // namespace Poco
