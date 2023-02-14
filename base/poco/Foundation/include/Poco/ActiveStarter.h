//
// ActiveStarter.h
//
// Library: Foundation
// Package: Threading
// Module:  ActiveObjects
//
// Definition of the ActiveStarter class.
//
// Copyright (c) 2006-2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_ActiveStarter_INCLUDED
#define Foundation_ActiveStarter_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/ThreadPool.h"
#include "Poco/ActiveRunnable.h"


namespace Poco {


template <class OwnerType>
class ActiveStarter
	/// The default implementation of the StarterType 
	/// policy for ActiveMethod. It starts the method
	/// in its own thread, obtained from the default
	/// thread pool.
{
public:
	static void start(OwnerType* /*pOwner*/, ActiveRunnableBase::Ptr pRunnable)
	{
		ThreadPool::defaultPool().start(*pRunnable);
		pRunnable->duplicate(); // The runnable will release itself.
	}
};


} // namespace Poco


#endif // Foundation_ActiveStarter_INCLUDED
