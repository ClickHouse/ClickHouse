//
// Delegate.h
//
// Library: Foundation
// Package: Events
// Module:  Delegate
//
// Implementation of the Delegate template.
//
// Copyright (c) 2006-2011, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_Delegate_INCLUDED
#define Foundation_Delegate_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/AbstractDelegate.h"
#include "Poco/FunctionDelegate.h"
#include "Poco/Expire.h"
#include "Poco/Mutex.h"


namespace Poco {


template <class TObj, class TArgs, bool withSender = true>
class Delegate: public AbstractDelegate<TArgs>
{
public:
	typedef void (TObj::*NotifyMethod)(const void*, TArgs&);

	Delegate(TObj* obj, NotifyMethod method):
		_receiverObject(obj),
		_receiverMethod(method)
	{
	}

	Delegate(const Delegate& delegate):
		AbstractDelegate<TArgs>(delegate),
		_receiverObject(delegate._receiverObject),
		_receiverMethod(delegate._receiverMethod)
	{
	}

	~Delegate()
	{
	}

	Delegate& operator = (const Delegate& delegate)
	{
		if (&delegate != this)
		{
			this->_receiverObject = delegate._receiverObject;
			this->_receiverMethod = delegate._receiverMethod;
		}
		return *this;
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
		const Delegate* pOtherDelegate = dynamic_cast<const Delegate*>(other.unwrap());
		return pOtherDelegate && _receiverObject == pOtherDelegate->_receiverObject && _receiverMethod == pOtherDelegate->_receiverMethod;
	}

	AbstractDelegate<TArgs>* clone() const
	{
		return new Delegate(*this);
	}

	void disable()
	{
		Mutex::ScopedLock lock(_mutex);
		_receiverObject = 0;
	}

protected:
	TObj*        _receiverObject;
	NotifyMethod _receiverMethod;
	Mutex        _mutex;

private:
	Delegate();
};


template <class TObj, class TArgs>
class Delegate<TObj, TArgs, false>: public AbstractDelegate<TArgs>
{
public:
	typedef void (TObj::*NotifyMethod)(TArgs&);

	Delegate(TObj* obj, NotifyMethod method):
		_receiverObject(obj),
		_receiverMethod(method)
	{
	}

	Delegate(const Delegate& delegate):
		AbstractDelegate<TArgs>(delegate),
		_receiverObject(delegate._receiverObject),
		_receiverMethod(delegate._receiverMethod)
	{
	}

	~Delegate()
	{
	}

	Delegate& operator = (const Delegate& delegate)
	{
		if (&delegate != this)
		{
			this->_receiverObject = delegate._receiverObject;
			this->_receiverMethod = delegate._receiverMethod;
		}
		return *this;
	}

	bool notify(const void*, TArgs& arguments)
	{
		Mutex::ScopedLock lock(_mutex);
		if (_receiverObject)
		{
			(_receiverObject->*_receiverMethod)(arguments);
			return true;
		}
		else return false;
	}

	bool equals(const AbstractDelegate<TArgs>& other) const
	{
		const Delegate* pOtherDelegate = dynamic_cast<const Delegate*>(other.unwrap());
		return pOtherDelegate && _receiverObject == pOtherDelegate->_receiverObject && _receiverMethod == pOtherDelegate->_receiverMethod;
	}

	AbstractDelegate<TArgs>* clone() const
	{
		return new Delegate(*this);
	}

	void disable()
	{
		Mutex::ScopedLock lock(_mutex);
		_receiverObject = 0;
	}

protected:
	TObj*        _receiverObject;
	NotifyMethod _receiverMethod;
	Mutex        _mutex;

private:
	Delegate();
};


template <class TObj, class TArgs>
inline Delegate<TObj, TArgs, true> delegate(TObj* pObj, void (TObj::*NotifyMethod)(const void*, TArgs&))
{
	return Delegate<TObj, TArgs, true>(pObj, NotifyMethod);
}


template <class TObj, class TArgs>
inline Delegate<TObj, TArgs, false> delegate(TObj* pObj, void (TObj::*NotifyMethod)(TArgs&))
{
	return Delegate<TObj, TArgs, false>(pObj, NotifyMethod);
}


template <class TObj, class TArgs>
inline Expire<TArgs> delegate(TObj* pObj, void (TObj::*NotifyMethod)(const void*, TArgs&), Timestamp::TimeDiff expireMillisecs)
{
	return Expire<TArgs>(Delegate<TObj, TArgs, true>(pObj, NotifyMethod), expireMillisecs);
}


template <class TObj, class TArgs>
inline Expire<TArgs> delegate(TObj* pObj, void (TObj::*NotifyMethod)(TArgs&), Timestamp::TimeDiff expireMillisecs)
{
	return Expire<TArgs>(Delegate<TObj, TArgs, false>(pObj, NotifyMethod), expireMillisecs);
}


template <class TArgs>
inline Expire<TArgs> delegate(void (*NotifyMethod)(const void*, TArgs&), Timestamp::TimeDiff expireMillisecs)
{
	return Expire<TArgs>(FunctionDelegate<TArgs, true, true>(NotifyMethod), expireMillisecs);
}


template <class TArgs>
inline Expire<TArgs> delegate(void (*NotifyMethod)(void*, TArgs&), Timestamp::TimeDiff expireMillisecs)
{
	return Expire<TArgs>(FunctionDelegate<TArgs, true, false>(NotifyMethod), expireMillisecs);
}


template <class TArgs>
inline Expire<TArgs> delegate(void (*NotifyMethod)(TArgs&), Timestamp::TimeDiff expireMillisecs)
{
	return Expire<TArgs>(FunctionDelegate<TArgs, false>(NotifyMethod), expireMillisecs);
}


template <class TArgs>
inline FunctionDelegate<TArgs, true, true> delegate(void (*NotifyMethod)(const void*, TArgs&))
{
	return FunctionDelegate<TArgs, true, true>(NotifyMethod);
}


template <class TArgs>
inline FunctionDelegate<TArgs, true, false> delegate(void (*NotifyMethod)(void*, TArgs&))
{
	return FunctionDelegate<TArgs, true, false>(NotifyMethod);
}


template <class TArgs>
inline FunctionDelegate<TArgs, false> delegate(void (*NotifyMethod)(TArgs&))
{
	return FunctionDelegate<TArgs, false>(NotifyMethod);
}


template <class TObj>
class Delegate<TObj,void,true>: public AbstractDelegate<void>
{
public:
	typedef void (TObj::*NotifyMethod)(const void*);

	Delegate(TObj* obj, NotifyMethod method):
		_receiverObject(obj),
		_receiverMethod(method)
	{
	}

	Delegate(const Delegate& delegate):
		AbstractDelegate<void>(delegate),
		_receiverObject(delegate._receiverObject),
		_receiverMethod(delegate._receiverMethod)
	{
	}

	~Delegate()
	{
	}

	Delegate& operator = (const Delegate& delegate)
	{
		if (&delegate != this)
		{
			this->_receiverObject = delegate._receiverObject;
			this->_receiverMethod = delegate._receiverMethod;
		}
		return *this;
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
		const Delegate* pOtherDelegate = dynamic_cast<const Delegate*>(other.unwrap());
		return pOtherDelegate && _receiverObject == pOtherDelegate->_receiverObject && _receiverMethod == pOtherDelegate->_receiverMethod;
	}

	AbstractDelegate<void>* clone() const
	{
		return new Delegate(*this);
	}

	void disable()
	{
		Mutex::ScopedLock lock(_mutex);
		_receiverObject = 0;
	}

protected:
	TObj*        _receiverObject;
	NotifyMethod _receiverMethod;
	Mutex        _mutex;

private:
	Delegate();
};


template <class TObj>
class Delegate<TObj, void, false>: public AbstractDelegate<void>
{
public:
	typedef void (TObj::*NotifyMethod)();

	Delegate(TObj* obj, NotifyMethod method):
		_receiverObject(obj),
		_receiverMethod(method)
	{
	}

	Delegate(const Delegate& delegate):
		AbstractDelegate<void>(delegate),
		_receiverObject(delegate._receiverObject),
		_receiverMethod(delegate._receiverMethod)
	{
	}

	~Delegate()
	{
	}

	Delegate& operator = (const Delegate& delegate)
	{
		if (&delegate != this)
		{
			this->_receiverObject = delegate._receiverObject;
			this->_receiverMethod = delegate._receiverMethod;
		}
		return *this;
	}

	bool notify(const void*)
	{
		Mutex::ScopedLock lock(_mutex);
		if (_receiverObject)
		{
			(_receiverObject->*_receiverMethod)();
			return true;
		}
		else return false;
	}

	bool equals(const AbstractDelegate<void>& other) const
	{
		const Delegate* pOtherDelegate = dynamic_cast<const Delegate*>(other.unwrap());
		return pOtherDelegate && _receiverObject == pOtherDelegate->_receiverObject && _receiverMethod == pOtherDelegate->_receiverMethod;
	}

	AbstractDelegate<void>* clone() const
	{
		return new Delegate(*this);
	}

	void disable()
	{
		Mutex::ScopedLock lock(_mutex);
		_receiverObject = 0;
	}

protected:
	TObj*        _receiverObject;
	NotifyMethod _receiverMethod;
	Mutex        _mutex;

private:
	Delegate();
};


template <class TObj>
inline Delegate<TObj, void, true> delegate(TObj* pObj, void (TObj::*NotifyMethod)(const void*))
{
	return Delegate<TObj, void, true>(pObj, NotifyMethod);
}


template <class TObj>
inline Delegate<TObj, void, false> delegate(TObj* pObj, void (TObj::*NotifyMethod)())
{
	return Delegate<TObj, void, false>(pObj, NotifyMethod);
}


template <class TObj>
inline Expire<void> delegate(TObj* pObj, void (TObj::*NotifyMethod)(const void*), Timestamp::TimeDiff expireMillisecs)
{
	return Expire<void>(Delegate<TObj, void, true>(pObj, NotifyMethod), expireMillisecs);
}


template <class TObj>
inline Expire<void> delegate(TObj* pObj, void (TObj::*NotifyMethod)(), Timestamp::TimeDiff expireMillisecs)
{
	return Expire<void>(Delegate<TObj, void, false>(pObj, NotifyMethod), expireMillisecs);
}


inline Expire<void> delegate(void (*NotifyMethod)(const void*), Timestamp::TimeDiff expireMillisecs)
{
	return Expire<void>(FunctionDelegate<void, true, true>(NotifyMethod), expireMillisecs);
}


inline Expire<void> delegate(void (*NotifyMethod)(void*), Timestamp::TimeDiff expireMillisecs)
{
	return Expire<void>(FunctionDelegate<void, true, false>(NotifyMethod), expireMillisecs);
}


inline Expire<void> delegate(void (*NotifyMethod)(), Timestamp::TimeDiff expireMillisecs)
{
	return Expire<void>(FunctionDelegate<void, false>(NotifyMethod), expireMillisecs);
}


inline FunctionDelegate<void, true, true> delegate(void (*NotifyMethod)(const void*))
{
	return FunctionDelegate<void, true, true>(NotifyMethod);
}


inline FunctionDelegate<void, true, false> delegate(void (*NotifyMethod)(void*))
{
	return FunctionDelegate<void, true, false>(NotifyMethod);
}


inline FunctionDelegate<void, false> delegate(void (*NotifyMethod)())
{
	return FunctionDelegate<void, false>(NotifyMethod);
}


} // namespace Poco


#endif // Foundation_Delegate_INCLUDED
