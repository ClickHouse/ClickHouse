//
// PriorityDelegate.h
//
// $Id: //poco/1.4/Foundation/include/Poco/PriorityDelegate.h#5 $
//
// Library: Foundation
// Package: Events
// Module:  PriorityDelegate
//
// Implementation of the PriorityDelegate template.
//
// Copyright (c) 2006-2011, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_PriorityDelegate_INCLUDED
#define Foundation_PriorityDelegate_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/AbstractPriorityDelegate.h"
#include "Poco/PriorityExpire.h"
#include "Poco/FunctionPriorityDelegate.h"
#include "Poco/Mutex.h"


namespace Poco {


template <class TObj, class TArgs, bool useSender = true> 
class PriorityDelegate: public AbstractPriorityDelegate<TArgs>
{
public:
	typedef void (TObj::*NotifyMethod)(const void*, TArgs&);

	PriorityDelegate(TObj* obj, NotifyMethod method, int prio):
		AbstractPriorityDelegate<TArgs>(prio),
		_receiverObject(obj),
		_receiverMethod(method)
	{
	}
	
	PriorityDelegate(const PriorityDelegate& delegate):
		AbstractPriorityDelegate<TArgs>(delegate),
		_receiverObject(delegate._receiverObject),
		_receiverMethod(delegate._receiverMethod)
	{
	}
	
	PriorityDelegate& operator = (const PriorityDelegate& delegate)
	{
		if (&delegate != this)
		{
			this->_pTarget        = delegate._pTarget;
			this->_receiverObject = delegate._receiverObject;
			this->_receiverMethod = delegate._receiverMethod;
			this->_priority       = delegate._priority;
		}
		return *this;
	}

	~PriorityDelegate()
	{
	}

	bool notify(const void* sender, TArgs& arguments)
	{
		Mutex::ScopedLock lock(_mutex);
		if (_receiverObject)
		{
			(_receiverObject->*_receiverMethod)(sender, arguments);
			return true; 
		}
		else return false;
	}

	bool equals(const AbstractDelegate<TArgs>& other) const
	{
		const PriorityDelegate* pOtherDelegate = dynamic_cast<const PriorityDelegate*>(other.unwrap());
		return pOtherDelegate && this->priority() == pOtherDelegate->priority() && _receiverObject == pOtherDelegate->_receiverObject && _receiverMethod == pOtherDelegate->_receiverMethod;
	}

	AbstractDelegate<TArgs>* clone() const
	{
		return new PriorityDelegate(*this);
	}

	void disable()
	{
		Mutex::ScopedLock lock(_mutex);
		_receiverObject = 0;
	}
	
protected:
	TObj*        _receiverObject;
	NotifyMethod _receiverMethod;
	Mutex _mutex;

private:
	PriorityDelegate();
};


template <class TObj, class TArgs> 
class PriorityDelegate<TObj, TArgs, false>: public AbstractPriorityDelegate<TArgs>
{
public:
	typedef void (TObj::*NotifyMethod)(TArgs&);

	PriorityDelegate(TObj* obj, NotifyMethod method, int prio):
		AbstractPriorityDelegate<TArgs>(prio),
		_receiverObject(obj),
		_receiverMethod(method)
	{
	}
	
	PriorityDelegate(const PriorityDelegate& delegate):
		AbstractPriorityDelegate<TArgs>(delegate),
		_receiverObject(delegate._receiverObject),
		_receiverMethod(delegate._receiverMethod)
	{
	}
	
	PriorityDelegate& operator = (const PriorityDelegate& delegate)
	{
		if (&delegate != this)
		{
			this->_pTarget        = delegate._pTarget;
			this->_receiverObject = delegate._receiverObject;
			this->_receiverMethod = delegate._receiverMethod;
			this->_priority       = delegate._priority;
		}
		return *this;
	}

	~PriorityDelegate()
	{
	}

	bool notify(const void* sender, TArgs& arguments)
	{
		Mutex::ScopedLock lock(_mutex);
		if (_receiverObject)
		{
			(_receiverObject->*_receiverMethod)(arguments);
			return true;
		}
		return false;
	}

	bool equals(const AbstractDelegate<TArgs>& other) const
	{
		const PriorityDelegate* pOtherDelegate = dynamic_cast<const PriorityDelegate*>(other.unwrap());
		return pOtherDelegate && this->priority() == pOtherDelegate->priority() && _receiverObject == pOtherDelegate->_receiverObject && _receiverMethod == pOtherDelegate->_receiverMethod;
	}

	AbstractDelegate<TArgs>* clone() const
	{
		return new PriorityDelegate(*this);
	}

	void disable()
	{
		Mutex::ScopedLock lock(_mutex);
		_receiverObject = 0;
	}

protected:
	TObj*        _receiverObject;
	NotifyMethod _receiverMethod;
	Mutex _mutex;

private:
	PriorityDelegate();
};


template <class TObj>
class PriorityDelegate<TObj, void, true>: public AbstractPriorityDelegate<void>
{
public:
	typedef void (TObj::*NotifyMethod)(const void*);

	PriorityDelegate(TObj* obj, NotifyMethod method, int prio):
		AbstractPriorityDelegate<void>(prio),
		_receiverObject(obj),
		_receiverMethod(method)
	{
	}

	PriorityDelegate(const PriorityDelegate& delegate):
		AbstractPriorityDelegate<void>(delegate),
		_receiverObject(delegate._receiverObject),
		_receiverMethod(delegate._receiverMethod)
	{
	}

	PriorityDelegate& operator = (const PriorityDelegate& delegate)
	{
		if (&delegate != this)
		{
			this->_pTarget        = delegate._pTarget;
			this->_receiverObject = delegate._receiverObject;
			this->_receiverMethod = delegate._receiverMethod;
			this->_priority       = delegate._priority;
		}
		return *this;
	}

	~PriorityDelegate()
	{
	}

	bool notify(const void* sender)
	{
		Mutex::ScopedLock lock(_mutex);
		if (_receiverObject)
		{
			(_receiverObject->*_receiverMethod)(sender);
			return true;
		}
		else return false;
	}

	bool equals(const AbstractDelegate<void>& other) const
	{
		const PriorityDelegate* pOtherDelegate = dynamic_cast<const PriorityDelegate*>(other.unwrap());
		return pOtherDelegate && this->priority() == pOtherDelegate->priority() && _receiverObject == pOtherDelegate->_receiverObject && _receiverMethod == pOtherDelegate->_receiverMethod;
	}

	AbstractDelegate<void>* clone() const
	{
		return new PriorityDelegate(*this);
	}

	void disable()
	{
		Mutex::ScopedLock lock(_mutex);
		_receiverObject = 0;
	}

protected:
	TObj*        _receiverObject;
	NotifyMethod _receiverMethod;
	Mutex _mutex;

private:
	PriorityDelegate();
};


template <class TObj>
class PriorityDelegate<TObj, void, false>: public AbstractPriorityDelegate<void>
{
public:
	typedef void (TObj::*NotifyMethod)();

	PriorityDelegate(TObj* obj, NotifyMethod method, int prio):
		AbstractPriorityDelegate<void>(prio),
		_receiverObject(obj),
		_receiverMethod(method)
	{
	}

	PriorityDelegate(const PriorityDelegate& delegate):
		AbstractPriorityDelegate<void>(delegate),
		_receiverObject(delegate._receiverObject),
		_receiverMethod(delegate._receiverMethod)
	{
	}

	PriorityDelegate& operator = (const PriorityDelegate& delegate)
	{
		if (&delegate != this)
		{
			this->_pTarget        = delegate._pTarget;
			this->_receiverObject = delegate._receiverObject;
			this->_receiverMethod = delegate._receiverMethod;
			this->_priority       = delegate._priority;
		}
		return *this;
	}

	~PriorityDelegate()
	{
	}

	bool notify(const void* sender)
	{
		Mutex::ScopedLock lock(_mutex);
		if (_receiverObject)
		{
			(_receiverObject->*_receiverMethod)();
			return true;
		}
		return false;
	}

	bool equals(const AbstractDelegate<void>& other) const
	{
		const PriorityDelegate* pOtherDelegate = dynamic_cast<const PriorityDelegate*>(other.unwrap());
		return pOtherDelegate && this->priority() == pOtherDelegate->priority() && _receiverObject == pOtherDelegate->_receiverObject && _receiverMethod == pOtherDelegate->_receiverMethod;
	}

	AbstractDelegate<void>* clone() const
	{
		return new PriorityDelegate(*this);
	}

	void disable()
	{
		Mutex::ScopedLock lock(_mutex);
		_receiverObject = 0;
	}

protected:
	TObj*        _receiverObject;
	NotifyMethod _receiverMethod;
	Mutex _mutex;

private:
	PriorityDelegate();
};


template <class TObj, class TArgs>
static PriorityDelegate<TObj, TArgs, true> priorityDelegate(TObj* pObj, void (TObj::*NotifyMethod)(const void*, TArgs&), int prio)
{
	return PriorityDelegate<TObj, TArgs, true>(pObj, NotifyMethod, prio);
}


template <class TObj, class TArgs>
static PriorityDelegate<TObj, TArgs, false> priorityDelegate(TObj* pObj, void (TObj::*NotifyMethod)(TArgs&), int prio)
{
	return PriorityDelegate<TObj, TArgs, false>(pObj, NotifyMethod, prio);
}


template <class TObj, class TArgs>
static PriorityExpire<TArgs> priorityDelegate(TObj* pObj, void (TObj::*NotifyMethod)(const void*, TArgs&), int prio, Timestamp::TimeDiff expireMilliSec)
{
	return PriorityExpire<TArgs>(PriorityDelegate<TObj, TArgs, true>(pObj, NotifyMethod, prio), expireMilliSec);
}


template <class TObj, class TArgs>
static PriorityExpire<TArgs> priorityDelegate(TObj* pObj, void (TObj::*NotifyMethod)(TArgs&), int prio, Timestamp::TimeDiff expireMilliSec)
{
	return PriorityExpire<TArgs>(PriorityDelegate<TObj, TArgs, false>(pObj, NotifyMethod, prio), expireMilliSec);
}


template <class TArgs>
static PriorityExpire<TArgs> priorityDelegate(void (*NotifyMethod)(const void*, TArgs&), int prio, Timestamp::TimeDiff expireMilliSec)
{
	return PriorityExpire<TArgs>(FunctionPriorityDelegate<TArgs, true, true>(NotifyMethod, prio), expireMilliSec);
}


template <class TArgs>
static PriorityExpire<TArgs> priorityDelegate(void (*NotifyMethod)(void*, TArgs&), int prio, Timestamp::TimeDiff expireMilliSec)
{
	return PriorityExpire<TArgs>(FunctionPriorityDelegate<TArgs, true, false>(NotifyMethod, prio), expireMilliSec);
}


template <class TArgs>
static PriorityExpire<TArgs> priorityDelegate(void (*NotifyMethod)(TArgs&), int prio, Timestamp::TimeDiff expireMilliSec)
{
	return PriorityExpire<TArgs>(FunctionPriorityDelegate<TArgs, false>(NotifyMethod, prio), expireMilliSec);
}


template <class TArgs>
static FunctionPriorityDelegate<TArgs, true, true> priorityDelegate(void (*NotifyMethod)(const void*, TArgs&), int prio)
{
	return FunctionPriorityDelegate<TArgs, true, true>(NotifyMethod, prio);
}


template <class TArgs>
static FunctionPriorityDelegate<TArgs, true, false> priorityDelegate(void (*NotifyMethod)(void*, TArgs&), int prio)
{
	return FunctionPriorityDelegate<TArgs, true, false>(NotifyMethod, prio);
}


template <class TArgs>
static FunctionPriorityDelegate<TArgs, false> priorityDelegate(void (*NotifyMethod)(TArgs&), int prio)
{
	return FunctionPriorityDelegate<TArgs, false>(NotifyMethod, prio);
}


template <class TObj>
static PriorityDelegate<TObj, void, true> priorityDelegate(TObj* pObj, void (TObj::*NotifyMethod)(const void*), int prio)
{
	return PriorityDelegate<TObj, void, true>(pObj, NotifyMethod, prio);
}


template <class TObj>
static PriorityDelegate<TObj, void, false> priorityDelegate(TObj* pObj, void (TObj::*NotifyMethod)(), int prio)
{
	return PriorityDelegate<TObj, void, false>(pObj, NotifyMethod, prio);
}


template <class TObj>
static PriorityExpire<void> priorityDelegate(TObj* pObj, void (TObj::*NotifyMethod)(const void*), int prio, Timestamp::TimeDiff expireMilliSec)
{
	return PriorityExpire<void>(PriorityDelegate<TObj, void, true>(pObj, NotifyMethod, prio), expireMilliSec);
}


template <class TObj>
static PriorityExpire<void> priorityDelegate(TObj* pObj, void (TObj::*NotifyMethod)(), int prio, Timestamp::TimeDiff expireMilliSec)
{
	return PriorityExpire<void>(PriorityDelegate<TObj, void, false>(pObj, NotifyMethod, prio), expireMilliSec);
}


inline PriorityExpire<void> priorityDelegate(void (*NotifyMethod)(const void*), int prio, Timestamp::TimeDiff expireMilliSec)
{
	return PriorityExpire<void>(FunctionPriorityDelegate<void, true, true>(NotifyMethod, prio), expireMilliSec);
}


inline PriorityExpire<void> priorityDelegate(void (*NotifyMethod)(void*), int prio, Timestamp::TimeDiff expireMilliSec)
{
	return PriorityExpire<void>(FunctionPriorityDelegate<void, true, false>(NotifyMethod, prio), expireMilliSec);
}


inline PriorityExpire<void> priorityDelegate(void (*NotifyMethod)(), int prio, Timestamp::TimeDiff expireMilliSec)
{
	return PriorityExpire<void>(FunctionPriorityDelegate<void, false>(NotifyMethod, prio), expireMilliSec);
}


inline FunctionPriorityDelegate<void, true, true> priorityDelegate(void (*NotifyMethod)(const void*), int prio)
{
	return FunctionPriorityDelegate<void, true, true>(NotifyMethod, prio);
}


inline FunctionPriorityDelegate<void, true, false> priorityDelegate(void (*NotifyMethod)(void*), int prio)
{
	return FunctionPriorityDelegate<void, true, false>(NotifyMethod, prio);
}


inline FunctionPriorityDelegate<void, false> priorityDelegate(void (*NotifyMethod)(), int prio)
{
	return FunctionPriorityDelegate<void, false>(NotifyMethod, prio);
}


} // namespace Poco


#endif // Foundation_PriorityDelegate_INCLUDED
