//
// NObserver.h
//
// $Id: //poco/1.4/Foundation/include/Poco/NObserver.h#2 $
//
// Library: Foundation
// Package: Notifications
// Module:  NotificationCenter
//
// Definition of the NObserver class template.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_NObserver_INCLUDED
#define Foundation_NObserver_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/AbstractObserver.h"
#include "Poco/Mutex.h"


namespace Poco {


template <class C, class N>
class NObserver: public AbstractObserver
	/// This template class implements an adapter that sits between
	/// a NotificationCenter and an object receiving notifications
	/// from it. It is quite similar in concept to the 
	/// RunnableAdapter, but provides some NotificationCenter
	/// specific additional methods.
	/// See the NotificationCenter class for information on how
	/// to use this template class.
	///
	/// This class template is quite similar to the Observer class
	/// template. The only difference is that the NObserver
	/// expects the callback function to accept a const AutoPtr& 
	/// instead of a plain pointer as argument, thus simplifying memory
	/// management.
{
public:
	typedef AutoPtr<N> NotificationPtr;
	typedef void (C::*Callback)(const NotificationPtr&);

	NObserver(C& object, Callback method): 
		_pObject(&object), 
		_method(method)
	{
	}
	
	NObserver(const NObserver& observer):
		AbstractObserver(observer),
		_pObject(observer._pObject), 
		_method(observer._method)
	{
	}
	
	~NObserver()
	{
	}
	
	NObserver& operator = (const NObserver& observer)
	{
		if (&observer != this)
		{
			_pObject = observer._pObject;
			_method  = observer._method;
		}
		return *this;
	}
	
	void notify(Notification* pNf) const
	{
		Poco::Mutex::ScopedLock lock(_mutex);

		if (_pObject)
		{
			N* pCastNf = dynamic_cast<N*>(pNf);
			if (pCastNf)
			{
				NotificationPtr ptr(pCastNf, true);
				(_pObject->*_method)(ptr);
			}
		}
	}
	
	bool equals(const AbstractObserver& abstractObserver) const
	{
		const NObserver* pObs = dynamic_cast<const NObserver*>(&abstractObserver);
		return pObs && pObs->_pObject == _pObject && pObs->_method == _method;
	}

	bool accepts(Notification* pNf) const
	{
		return dynamic_cast<N*>(pNf) != 0;
	}
	
	AbstractObserver* clone() const
	{
		return new NObserver(*this);
	}
	
	void disable()
	{
		Poco::Mutex::ScopedLock lock(_mutex);
		
		_pObject = 0;
	}

private:
	NObserver();

	C*       _pObject;
	Callback _method;
	mutable Poco::Mutex _mutex;
};


} // namespace Poco


#endif // Foundation_NObserver_INCLUDED
