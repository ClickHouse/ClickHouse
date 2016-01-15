//
// BasicEvent.h
//
// $Id: //poco/1.4/Foundation/include/Poco/BasicEvent.h#2 $
//
// Library: Foundation
// Package: Events
// Module:  BasicEvent
//
// Implementation of the BasicEvent template.
//
// Copyright (c) 2006-2011, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_BasicEvent_INCLUDED
#define Foundation_BasicEvent_INCLUDED


#include "Poco/AbstractEvent.h"
#include "Poco/DefaultStrategy.h"
#include "Poco/AbstractDelegate.h"
#include "Poco/Mutex.h"


namespace Poco {


template <class TArgs, class TMutex = FastMutex> 
class BasicEvent: public AbstractEvent < 
	TArgs, DefaultStrategy<TArgs, AbstractDelegate<TArgs> >,
	AbstractDelegate<TArgs>,
	TMutex
>
	/// A BasicEvent uses the DefaultStrategy which 
	/// invokes delegates in the order they have been registered.
	///
	/// Please see the AbstractEvent class template documentation
	/// for more information.
{
public:
	BasicEvent()
	{
	}

	~BasicEvent()
	{
	}

private:
	BasicEvent(const BasicEvent& e);
	BasicEvent& operator = (const BasicEvent& e);
};


} // namespace Poco


#endif // Foundation_BasicEvent_INCLUDED
