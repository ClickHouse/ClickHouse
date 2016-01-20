//
// Semaphore.cpp
//
// $Id: //poco/1.4/Foundation/src/Semaphore.cpp#2 $
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


#include "Poco/Semaphore.h"


#if defined(POCO_OS_FAMILY_WINDOWS)
#include "Semaphore_WIN32.cpp"
#elif defined(POCO_VXWORKS)
#include "Semaphore_VX.cpp"
#else
#include "Semaphore_POSIX.cpp"
#endif


namespace Poco {


Semaphore::Semaphore(int n): SemaphoreImpl(n, n)
{
}


Semaphore::Semaphore(int n, int max): SemaphoreImpl(n, max)
{
}


Semaphore::~Semaphore()
{
}


} // namespace Poco
