//
// Semaphore_WIN32.h
//
// Library: Foundation
// Package: Threading
// Module:  Semaphore
//
// Definition of the SemaphoreImpl class for WIN32.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_Semaphore_WIN32_INCLUDED
#define Foundation_Semaphore_WIN32_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Exception.h"
#include "Poco/UnWindows.h"


namespace Poco {


class Foundation_API SemaphoreImpl
{
protected:
	SemaphoreImpl(int n, int max);		
	~SemaphoreImpl();
	void setImpl();
	void waitImpl();
	bool waitImpl(long milliseconds);
	
private:
	HANDLE _sema;
};


//
// inlines
//
inline void SemaphoreImpl::setImpl()
{
	if (!ReleaseSemaphore(_sema, 1, NULL))
	{
		throw SystemException("cannot signal semaphore");
	}
}


} // namespace Poco


#endif // Foundation_Semaphore_WIN32_INCLUDED
