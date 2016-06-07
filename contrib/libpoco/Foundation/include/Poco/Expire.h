//
// Expire.h
//
// $Id: //poco/1.4/Foundation/include/Poco/Expire.h#3 $
//
// Library: Foundation
// Package: Events
// Module:  Expire
//
// Implementation of the Expire template.
//
// Copyright (c) 2006-2011, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_Expire_INCLUDED
#define Foundation_Expire_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/AbstractDelegate.h"
#include "Poco/Timestamp.h"


namespace Poco {


template <class TArgs>
class Expire: public AbstractDelegate<TArgs>
	/// Decorator for AbstractDelegate adding automatic 
	/// expiration of registrations to AbstractDelegate's.
{
public:
	Expire(const AbstractDelegate<TArgs>& p, Timestamp::TimeDiff expireMillisecs):
		_pDelegate(p.clone()), 
		_expire(expireMillisecs*1000)
	{
	}

	Expire(const Expire& expire):
		AbstractDelegate<TArgs>(expire),
		_pDelegate(expire._pDelegate->clone()),
		_expire(expire._expire),
		_creationTime(expire._creationTime)
	{
	}

	~Expire()
	{
		delete _pDelegate;
	}
	
	Expire& operator = (const Expire& expire)
	{
		if (&expire != this)
		{
			delete this->_pDelegate;
			this->_pDelegate    = expire._pDelegate->clone();
			this->_expire       = expire._expire;
			this->_creationTime = expire._creationTime;
			this->_pTarget = expire._pTarget;
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

	AbstractDelegate<TArgs>* clone() const
	{
		return new Expire(*this);
	}
	
	void disable()
	{
		_pDelegate->disable();
	}

	const AbstractDelegate<TArgs>* unwrap() const
	{
		return this->_pDelegate;
	}

protected:
	bool expired() const
	{
		return _creationTime.isElapsed(_expire);
	}

	AbstractDelegate<TArgs>* _pDelegate;
	Timestamp::TimeDiff _expire;
	Timestamp _creationTime;

private:
	Expire();
};


template <>
class Expire<void>: public AbstractDelegate<void>
	/// Decorator for AbstractDelegate adding automatic 
	/// expiration of registrations to AbstractDelegate's.
{
public:
	Expire(const AbstractDelegate<void>& p, Timestamp::TimeDiff expireMillisecs):
		_pDelegate(p.clone()), 
		_expire(expireMillisecs*1000)
	{
	}

	Expire(const Expire& expire):
		AbstractDelegate<void>(expire),
		_pDelegate(expire._pDelegate->clone()),
		_expire(expire._expire),
		_creationTime(expire._creationTime)
	{
	}

	~Expire()
	{
		delete _pDelegate;
	}
	
	Expire& operator = (const Expire& expire)
	{
		if (&expire != this)
		{
			delete this->_pDelegate;
			this->_pDelegate    = expire._pDelegate->clone();
			this->_expire       = expire._expire;
			this->_creationTime = expire._creationTime;
			//this->_pTarget = expire._pTarget;
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

	AbstractDelegate<void>* clone() const
	{
		return new Expire(*this);
	}
	
	void disable()
	{
		_pDelegate->disable();
	}

	const AbstractDelegate<void>* unwrap() const
	{
		return this->_pDelegate;
	}

protected:
	bool expired() const
	{
		return _creationTime.isElapsed(_expire);
	}

	AbstractDelegate<void>* _pDelegate;
	Timestamp::TimeDiff _expire;
	Timestamp _creationTime;

private:
	Expire();
};


} // namespace Poco


#endif // Foundation_Expire_INCLUDED
