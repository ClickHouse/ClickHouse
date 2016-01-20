//
// NotificationStrategy.h
//
// $Id: //poco/1.4/Foundation/include/Poco/NotificationStrategy.h#2 $
//
// Library: Foundation
// Package: Events
// Module:  NotificationStrategy
//
// Definition of the NotificationStrategy interface.
//
// Copyright (c) 2006-2011, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_NotificationStrategy_INCLUDED
#define Foundation_NotificationStrategy_INCLUDED


#include "Poco/Foundation.h"


namespace Poco {


template <class TArgs, class TDelegate> 
class NotificationStrategy
	/// The interface that all notification strategies must implement.
	/// 
	/// Note: Event is based on policy-driven design, so every strategy implementation
	/// must provide all the methods from this interface (otherwise: compile errors)
	/// but does not need to inherit from NotificationStrategy.
{
public:
	typedef TDelegate* DelegateHandle;

	NotificationStrategy()
	{
	}

	virtual ~NotificationStrategy()
	{
	}

	virtual void notify(const void* sender, TArgs& arguments) = 0;
		/// Sends a notification to all registered delegates.

	virtual DelegateHandle add(const TDelegate& delegate) = 0;
		/// Adds a delegate to the strategy.

	virtual void remove(const TDelegate& delegate) = 0;
		/// Removes a delegate from the strategy, if found.
		/// Does nothing if the delegate has not been added.

	virtual void remove(DelegateHandle delegateHandle) = 0;
		/// Removes a delegate from the strategy, if found.
		/// Does nothing if the delegate has not been added.

	virtual void clear() = 0;
		/// Removes all delegates from the strategy.

	virtual bool empty() const = 0;
		/// Returns false if the strategy contains at least one delegate.
};


template <class TDelegate> 
class NotificationStrategy<void, TDelegate>
	/// The interface that all notification strategies must implement.
	/// 
	/// Note: Event is based on policy-driven design, so every strategy implementation
	/// must provide all the methods from this interface (otherwise: compile errors)
	/// but does not need to inherit from NotificationStrategy.
{
public:
	typedef TDelegate* DelegateHandle;

	NotificationStrategy()
	{
	}

	virtual ~NotificationStrategy()
	{
	}

	virtual void notify(const void* sender) = 0;
		/// Sends a notification to all registered delegates.

	virtual DelegateHandle add(const TDelegate& delegate) = 0;
		/// Adds a delegate to the strategy.

	virtual void remove(const TDelegate& delegate) = 0;
		/// Removes a delegate from the strategy, if found.
		/// Does nothing if the delegate has not been added.

	virtual void remove(DelegateHandle delegateHandle) = 0;
		/// Removes a delegate from the strategy, if found.
		/// Does nothing if the delegate has not been added.

	virtual void clear() = 0;
		/// Removes all delegates from the strategy.

	virtual bool empty() const = 0;
		/// Returns false if the strategy contains at least one delegate.
};


} // namespace Poco


#endif // Foundation_NotificationStrategy_INCLUDED
