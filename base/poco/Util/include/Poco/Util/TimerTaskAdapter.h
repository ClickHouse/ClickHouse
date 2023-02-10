//
// TimerTaskAdapter.h
//
// Library: Util
// Package: Timer
// Module:  TimerTaskAdapter
//
// Definition of the TimerTaskAdapter class template.
//
// Copyright (c) 2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Util_TimerTaskAdapter_INCLUDED
#define Util_TimerTaskAdapter_INCLUDED


#include "Poco/Util/Util.h"
#include "Poco/Util/TimerTask.h"


namespace Poco {
namespace Util {


template <class C>
class TimerTaskAdapter: public TimerTask
	/// This class template simplifies the implementation
	/// of TimerTask objects by allowing a member function
	/// of an object to be called as task. 
{
public:
	typedef void (C::*Callback)(TimerTask&);
	
	TimerTaskAdapter(C& object, Callback method): _pObject(&object), _method(method)
		/// Creates the TimerTaskAdapter, using the given 
		/// object and its member function as task target.
		///
		/// The member function must accept one argument,
		/// a reference to a TimerTask object.
	{
	}
	
	void run()
	{
		(_pObject->*_method)(*this);
	}
			
protected:
	~TimerTaskAdapter()
		/// Destroys the TimerTaskAdapter.
	{
	}
	
private:
	TimerTaskAdapter();

	C*       _pObject;
	Callback _method;
};


} } // namespace Poco::Util


#endif // Util_TimerTaskAdapter_INCLUDED
