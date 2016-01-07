//
// AbstractPriorityDelegate.h
//
// $Id: //poco/1.4/Foundation/include/Poco/AbstractPriorityDelegate.h#3 $
//
// Library: Foundation
// Package: Events
// Module:  AbstractPriorityDelegate
//
// Implementation of the AbstractPriorityDelegate template.
//
// Copyright (c) 2006-2011, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_AbstractPriorityDelegate_INCLUDED
#define Foundation_AbstractPriorityDelegate_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/AbstractDelegate.h"


namespace Poco {


template <class TArgs> 
class AbstractPriorityDelegate: public AbstractDelegate<TArgs>
	/// Base class for PriorityDelegate and PriorityExpire.
	///
	/// Extends AbstractDelegate with a priority value.
{
public:
	AbstractPriorityDelegate(int prio):
		_priority(prio)
	{
	}

	AbstractPriorityDelegate(const AbstractPriorityDelegate& del):
		AbstractDelegate<TArgs>(del),
		_priority(del._priority)
	{
	}

	virtual ~AbstractPriorityDelegate() 
	{
	}

	int priority() const
	{
		return _priority;
	}

protected:
	int _priority;
};


} // namespace Poco


#endif // Foundation_AbstractPriorityDelegate_INCLUDED
