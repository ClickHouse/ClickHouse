//
// OptionCallback.h
//
// $Id: //poco/1.4/Util/include/Poco/Util/OptionCallback.h#1 $
//
// Library: Util
// Package: Options
// Module:  OptionCallback
//
// Definition of the OptionCallback class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Util_OptionCallback_INCLUDED
#define Util_OptionCallback_INCLUDED


#include "Poco/Util/Util.h"


namespace Poco {
namespace Util {


class Util_API AbstractOptionCallback
	/// Base class for OptionCallback.
{
public:
	virtual void invoke(const std::string& name, const std::string& value) const = 0;
		/// Invokes the callback member function.
		
	virtual AbstractOptionCallback* clone() const = 0;
		/// Creates and returns a copy of the object.

	virtual ~AbstractOptionCallback();
		/// Destroys the AbstractOptionCallback.

protected:
	AbstractOptionCallback();
	AbstractOptionCallback(const AbstractOptionCallback&);
};


template <class C>
class OptionCallback: public AbstractOptionCallback
	/// This class is used as an argument to Option::callback().
	///
	/// It stores a pointer to an object and a pointer to a member
	/// function of the object's class.
{
public:
	typedef void (C::*Callback)(const std::string& name, const std::string& value);

	OptionCallback(C* pObject, Callback method):
		_pObject(pObject),
		_method(method)
		/// Creates the OptionCallback for the given object and member function.
	{
		poco_check_ptr (pObject);
	}
	
	OptionCallback(const OptionCallback& cb):
		AbstractOptionCallback(cb),
		_pObject(cb._pObject),
		_method(cb._method)
		/// Creates an OptionCallback from another one.
	{
	}
	
	~OptionCallback()
		/// Destroys the OptionCallback.
	{
	}
	
	OptionCallback& operator = (const OptionCallback& cb)
	{
		if (&cb != this)
		{
			this->_pObject = cb._pObject;
			this->_method  = cb._method;
		}
		return *this;
	}
	
	void invoke(const std::string& name, const std::string& value) const
	{
		(_pObject->*_method)(name, value);
	}
	
	AbstractOptionCallback* clone() const
	{
		return new OptionCallback(_pObject, _method);
	}
	
private:
	OptionCallback();
	
	C* _pObject;
	Callback _method;
};


} } // namespace Poco::Util


#endif // Util_OptionCallback_INCLUDED
