//
// RunnableAdapter.h
//
// $Id: //poco/1.4/Foundation/include/Poco/RunnableAdapter.h#1 $
//
// Library: Foundation
// Package: Threading
// Module:  Thread
//
// Definition of the RunnableAdapter template class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_RunnableAdapter_INCLUDED
#define Foundation_RunnableAdapter_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Runnable.h"


namespace Poco {


template <class C>
class RunnableAdapter: public Runnable
	/// This adapter simplifies using ordinary methods as
	/// targets for threads.
	/// Usage:
	///    RunnableAdapter<MyClass> ra(myObject, &MyObject::doSomething));
	///    Thread thr;
	///    thr.Start(ra);
	///
	/// For using a freestanding or static member function as a thread
	/// target, please see the ThreadTarget class.
{
public:
	typedef void (C::*Callback)();
	
	RunnableAdapter(C& object, Callback method): _pObject(&object), _method(method)
	{
	}
	
	RunnableAdapter(const RunnableAdapter& ra): _pObject(ra._pObject), _method(ra._method)
	{
	}

	~RunnableAdapter()
	{
	}

	RunnableAdapter& operator = (const RunnableAdapter& ra)
	{
		_pObject = ra._pObject;
		_method  = ra._method;
		return *this;
	}

	void run()
	{
		(_pObject->*_method)();
	}
	
private:
	RunnableAdapter();

	C*       _pObject;
	Callback _method;
};


} // namespace Poco


#endif // Foundation_RunnableAdapter_INCLUDED
