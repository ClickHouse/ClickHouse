//
// Semaphore.cpp
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


#include "Semaphore_POSIX.cpp"


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
