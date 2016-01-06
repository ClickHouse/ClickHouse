//
// PriorityEvent.h
//
// $Id: //poco/1.4/Foundation/include/Poco/PriorityEvent.h#2 $
//
// Library: Foundation
// Package: Events
// Module:  PriorityEvent
//
// Implementation of the PriorityEvent template.
//
// Copyright (c) 2006-2011, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_PriorityEvent_INCLUDED
#define Foundation_PriorityEvent_INCLUDED


#include "Poco/AbstractEvent.h"
#include "Poco/PriorityStrategy.h"
#include "Poco/AbstractPriorityDelegate.h"


namespace Poco {


template <class TArgs, class TMutex = FastMutex> 
class PriorityEvent: public AbstractEvent < 
	TArgs,
	PriorityStrategy<TArgs, AbstractPriorityDelegate<TArgs> >,
	AbstractPriorityDelegate<TArgs>,
	TMutex
>
	/// A PriorityEvent uses internally a PriorityStrategy which 
	/// invokes delegates in order of priority (lower priorities first).
	/// PriorityEvent's can only be used together with PriorityDelegate's.
	/// PriorityDelegate's are sorted according to the priority value, when
	/// two delegates have the same priority, they are invoked in
	/// an arbitrary manner.
{
public:
	PriorityEvent()
	{
	}

	~PriorityEvent()
	{
	}

private:
	PriorityEvent(const PriorityEvent&);
	PriorityEvent& operator = (const PriorityEvent&);
};


} // namespace Poco


#endif // Foundation_PriorityEvent_INCLUDED
