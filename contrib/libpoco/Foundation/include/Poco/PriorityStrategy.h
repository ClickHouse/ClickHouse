//
// PriorityStrategy.h
//
// $Id: //poco/1.4/Foundation/include/Poco/PriorityStrategy.h#2 $
//
// Library: Foundation
// Package: Events
// Module:  PrioritytStrategy
//
// Implementation of the DefaultStrategy template.
//
// Copyright (c) 2006-2011, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_PriorityStrategy_INCLUDED
#define Foundation_PriorityStrategy_INCLUDED


#include "Poco/NotificationStrategy.h"
#include "Poco/SharedPtr.h"
#include <vector>


namespace Poco {


template <class TArgs, class TDelegate> 
class PriorityStrategy: public NotificationStrategy<TArgs, TDelegate>
	/// NotificationStrategy for PriorityEvent.
	///
	/// Delegates are kept in a std::vector<>, ordered
	/// by their priority.
{
public:
	typedef TDelegate*                   DelegateHandle;
	typedef SharedPtr<TDelegate>         DelegatePtr;
	typedef std::vector<DelegatePtr>     Delegates;
	typedef typename Delegates::iterator Iterator;

public:
	PriorityStrategy()
	{
	}

	PriorityStrategy(const PriorityStrategy& s):
		_delegates(s._delegates)
	{
	}

	~PriorityStrategy()
	{
	}

	void notify(const void* sender, TArgs& arguments)
	{
		for (Iterator it = _delegates.begin(); it != _delegates.end(); ++it)
		{
			(*it)->notify(sender, arguments);
		}
	}

	DelegateHandle add(const TDelegate& delegate)
	{
		for (Iterator it = _delegates.begin(); it != _delegates.end(); ++it)
		{
			if ((*it)->priority() > delegate.priority())
			{
				DelegatePtr pDelegate(static_cast<TDelegate*>(delegate.clone()));
				_delegates.insert(it, pDelegate);
				return pDelegate.get();
			}
		}
		DelegatePtr pDelegate(static_cast<TDelegate*>(delegate.clone()));
		_delegates.push_back(pDelegate);
		return pDelegate.get();
	}

	void remove(const TDelegate& delegate)
	{
		for (Iterator it = _delegates.begin(); it != _delegates.end(); ++it)
		{
			if (delegate.equals(**it))
			{
				(*it)->disable();
				_delegates.erase(it);
				return;
			}
		}
	}

	void remove(DelegateHandle delegateHandle)
	{
		for (Iterator it = _delegates.begin(); it != _delegates.end(); ++it)
		{
			if (*it == delegateHandle)
			{
				(*it)->disable();
				_delegates.erase(it);
				return;
			}
		}
	}

	PriorityStrategy& operator = (const PriorityStrategy& s)
	{
		if (this != &s)
		{
			_delegates = s._delegates;
		}
		return *this;
	}

	void clear()
	{
		for (Iterator it = _delegates.begin(); it != _delegates.end(); ++it)
		{
			(*it)->disable();
		}
		_delegates.clear();
	}

	bool empty() const
	{
		return _delegates.empty();
	}

protected:
	Delegates _delegates;
};


template <class TDelegate>
class PriorityStrategy<void, TDelegate>
	/// NotificationStrategy for PriorityEvent.
	///
	/// Delegates are kept in a std::vector<>, ordered
	/// by their priority.
{
public:
	typedef TDelegate*                   DelegateHandle;
	typedef SharedPtr<TDelegate>         DelegatePtr;
	typedef std::vector<DelegatePtr>     Delegates;
	typedef typename Delegates::iterator Iterator;

public:

	void notify(const void* sender)
	{
		for (Iterator it = _delegates.begin(); it != _delegates.end(); ++it)
		{
			(*it)->notify(sender);
		}
	}

	DelegateHandle add(const TDelegate& delegate)
	{
		for (Iterator it = _delegates.begin(); it != _delegates.end(); ++it)
		{
			if ((*it)->priority() > delegate.priority())
			{
				DelegatePtr pDelegate(static_cast<TDelegate*>(delegate.clone()));
				_delegates.insert(it, pDelegate);
				return pDelegate.get();
			}
		}
		DelegatePtr pDelegate(static_cast<TDelegate*>(delegate.clone()));
		_delegates.push_back(pDelegate);
		return pDelegate.get();
	}

	void remove(const TDelegate& delegate)
	{
		for (Iterator it = _delegates.begin(); it != _delegates.end(); ++it)
		{
			if (delegate.equals(**it))
			{
				(*it)->disable();
				_delegates.erase(it);
				return;
			}
		}
	}

	void remove(DelegateHandle delegateHandle)
	{
		for (Iterator it = _delegates.begin(); it != _delegates.end(); ++it)
		{
			if (*it == delegateHandle)
			{
				(*it)->disable();
				_delegates.erase(it);
				return;
			}
		}
	}

	PriorityStrategy& operator = (const PriorityStrategy& s)
	{
		if (this != &s)
		{
			_delegates = s._delegates;
		}
		return *this;
	}

	void clear()
	{
		for (Iterator it = _delegates.begin(); it != _delegates.end(); ++it)
		{
			(*it)->disable();
		}
		_delegates.clear();
	}

	bool empty() const
	{
		return _delegates.empty();
	}

protected:
	Delegates _delegates;
};


} // namespace Poco


#endif // Foundation_PriorityStrategy_INCLUDED
