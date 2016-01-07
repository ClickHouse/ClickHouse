//
// PriorityExpire.h
//
// $Id: //poco/1.4/Foundation/include/Poco/PriorityExpire.h#3 $
//
// Library: Foundation
// Package: Events
// Module:  PriorityExpire
//
// Implementation of the PriorityExpire template.
//
// Copyright (c) 2006-2011, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_PriorityExpire_INCLUDED
#define Foundation_PriorityExpire_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Timestamp.h"
#include "Poco/AbstractPriorityDelegate.h"


namespace Poco {


template <class TArgs>
class PriorityExpire: public AbstractPriorityDelegate<TArgs>
	/// Decorator for AbstractPriorityDelegate adding automatic 
	/// expiring of registrations to AbstractPriorityDelegate.
{
public:
	PriorityExpire(const AbstractPriorityDelegate<TArgs>& p, Timestamp::TimeDiff expireMilliSec):
		AbstractPriorityDelegate<TArgs>(p),
		_pDelegate(static_cast<AbstractPriorityDelegate<TArgs>*>(p.clone())), 
		_expire(expireMilliSec*1000)
	{
	}
	
	PriorityExpire(const PriorityExpire& expire):
		AbstractPriorityDelegate<TArgs>(expire),
		_pDelegate(static_cast<AbstractPriorityDelegate<TArgs>*>(expire._pDelegate->clone())),
		_expire(expire._expire),
		_creationTime(expire._creationTime)
	{
	}

	~PriorityExpire()
	{
		delete _pDelegate;
	}
	
	PriorityExpire& operator = (const PriorityExpire& expire)
	{
		if (&expire != this)
		{
			delete this->_pDelegate;
			this->_pTarget      = expire._pTarget;
			this->_pDelegate    = expire._pDelegate->clone();
			this->_expire       = expire._expire;
			this->_creationTime = expire._creationTime;
		}
		return *this; 
	}

	bool notify(const void* sender, TArgs& arguments)
	{
		if (!expired())
			return this->_pDelegate->notify(sender, arguments);
		else
			return false;
	}

	bool equals(const AbstractDelegate<TArgs>& other) const
	{
		return other.equals(*_pDelegate);
	}

	AbstractPriorityDelegate<TArgs>* clone() const
	{
		return new PriorityExpire(*this);
	}

	void disable()
	{
		_pDelegate->disable();
	}

	const AbstractPriorityDelegate<TArgs>* unwrap() const
	{
		return this->_pDelegate;
	}

protected:
	bool expired() const
	{
		return _creationTime.isElapsed(_expire);
	}

	AbstractPriorityDelegate<TArgs>* _pDelegate;
	Timestamp::TimeDiff _expire;
	Timestamp _creationTime;

private:
	PriorityExpire();
};


template <>
class PriorityExpire<void>: public AbstractPriorityDelegate<void>
	/// Decorator for AbstractPriorityDelegate adding automatic
	/// expiring of registrations to AbstractPriorityDelegate.
{
public:
	PriorityExpire(const AbstractPriorityDelegate<void>& p, Timestamp::TimeDiff expireMilliSec):
		AbstractPriorityDelegate<void>(p),
		_pDelegate(static_cast<AbstractPriorityDelegate<void>*>(p.clone())),
		_expire(expireMilliSec*1000)
	{
	}

	PriorityExpire(const PriorityExpire& expire):
		AbstractPriorityDelegate<void>(expire),
		_pDelegate(static_cast<AbstractPriorityDelegate<void>*>(expire._pDelegate->clone())),
		_expire(expire._expire),
		_creationTime(expire._creationTime)
	{
	}

	~PriorityExpire()
	{
		delete _pDelegate;
	}

	PriorityExpire& operator = (const PriorityExpire& expire)
	{
		if (&expire != this)
		{
			delete this->_pDelegate;
			this->_pDelegate    = static_cast<AbstractPriorityDelegate<void>*>(expire._pDelegate->clone());
			this->_expire       = expire._expire;
			this->_creationTime = expire._creationTime;
		}
		return *this;
	}

	bool notify(const void* sender)
	{
		if (!expired())
			return this->_pDelegate->notify(sender);
		else
			return false;
	}

	bool equals(const AbstractDelegate<void>& other) const
	{
		return other.equals(*_pDelegate);
	}

	AbstractPriorityDelegate<void>* clone() const
	{
		return new PriorityExpire(*this);
	}

	void disable()
	{
		_pDelegate->disable();
	}

	const AbstractPriorityDelegate<void>* unwrap() const
	{
		return this->_pDelegate;
	}

protected:
	bool expired() const
	{
		return _creationTime.isElapsed(_expire);
	}

	AbstractPriorityDelegate<void>* _pDelegate;
	Timestamp::TimeDiff _expire;
	Timestamp _creationTime;

private:
	PriorityExpire();
};


} // namespace Poco


#endif // Foundation_PriorityExpire_INCLUDED
