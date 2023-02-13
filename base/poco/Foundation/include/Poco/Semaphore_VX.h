//
// Semaphore_VX.h
//
// Library: Foundation
// Package: Threading
// Module:  Semaphore
//
// Definition of the SemaphoreImpl class for VxWorks.
//
// Copyright (c) 2004-20011, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_Semaphore_VX_INCLUDED
#define Foundation_Semaphore_VX_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Exception.h"
#include <semLib.h>


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
	SEM_ID _sem;
};


//
// inlines
//
inline void SemaphoreImpl::setImpl()
{
	if (semGive(_sem) != OK)
		throw SystemException("cannot signal semaphore");
}


} // namespace Poco


#endif // Foundation_Semaphore_VX_INCLUDED
