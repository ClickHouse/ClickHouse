//
// AutoPtr.h
//
// $Id: //poco/1.4/Foundation/include/Poco/AutoPtr.h#1 $
//
// Library: Foundation
// Package: Core
// Module:  AutoPtr
//
// Definition of the AutoPtr template class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_AutoPtr_INCLUDED
#define Foundation_AutoPtr_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Exception.h"
#include <algorithm>


namespace Poco {


template <class C>
class AutoPtr
	/// AutoPtr is a "smart" pointer for classes implementing
	/// reference counting based garbage collection.
	/// To be usable with the AutoPtr template, a class must
	/// implement the following behaviour:
	/// A class must maintain a reference count.
	/// The constructors of the object initialize the reference
	/// count to one.
	/// The class must implement a public duplicate() method:
	///     void duplicate();
	/// that increments the reference count by one.
	/// The class must implement a public release() method:
	///     void release()
	/// that decrements the reference count by one, and,
	/// if the reference count reaches zero, deletes the
	/// object.
	///
	/// AutoPtr works in the following way:
	/// If an AutoPtr is assigned an ordinary pointer to
	/// an object (via the constructor or the assignment operator),
	/// it takes ownership of the object and the object's reference 
	/// count remains unchanged.
	/// If the AutoPtr is assigned another AutoPtr, the
	/// object's reference count is incremented by one by
	/// calling duplicate() on its object.
	/// The destructor of AutoPtr calls release() on its
	/// object.
	/// AutoPtr supports dereferencing with both the ->
	/// and the * operator. An attempt to dereference a null
	/// AutoPtr results in a NullPointerException being thrown.
	/// AutoPtr also implements all relational operators.
	/// Note that AutoPtr allows casting of its encapsulated data types.
{
public:
	AutoPtr(): _ptr(0)
	{
	}

	AutoPtr(C* ptr): _ptr(ptr)
	{
	}

	AutoPtr(C* ptr, bool shared): _ptr(ptr)
	{
		if (shared && _ptr) _ptr->duplicate();
	}

	AutoPtr(const AutoPtr& ptr): _ptr(ptr._ptr)
	{
		if (_ptr) _ptr->duplicate();
	}

	template <class Other> 
	AutoPtr(const AutoPtr<Other>& ptr): _ptr(const_cast<Other*>(ptr.get()))
	{
		if (_ptr) _ptr->duplicate();
	}

	~AutoPtr()
	{
		if (_ptr) _ptr->release();
	}
	
	AutoPtr& assign(C* ptr)
	{
		if (_ptr != ptr)
		{
			if (_ptr) _ptr->release();
			_ptr = ptr;
		}
		return *this;
	}

	AutoPtr& assign(C* ptr, bool shared)
	{
		if (_ptr != ptr)
		{
			if (_ptr) _ptr->release();
			_ptr = ptr;
			if (shared && _ptr) _ptr->duplicate();
		}
		return *this;
	}
	
	AutoPtr& assign(const AutoPtr& ptr)
	{
		if (&ptr != this)
		{
			if (_ptr) _ptr->release();
			_ptr = ptr._ptr;
			if (_ptr) _ptr->duplicate();
		}
		return *this;
	}
	
	template <class Other> 
	AutoPtr& assign(const AutoPtr<Other>& ptr)
	{
		if (ptr.get() != _ptr)
		{
			if (_ptr) _ptr->release();
			_ptr = const_cast<Other*>(ptr.get());
			if (_ptr) _ptr->duplicate();
		}
		return *this;
	}

	AutoPtr& operator = (C* ptr)
	{
		return assign(ptr);
	}

	AutoPtr& operator = (const AutoPtr& ptr)
	{
		return assign(ptr);
	}
	
	template <class Other> 
	AutoPtr& operator = (const AutoPtr<Other>& ptr)
	{
		return assign<Other>(ptr);
	}

	void swap(AutoPtr& ptr)
	{
		std::swap(_ptr, ptr._ptr);
	}
	
	template <class Other> 
	AutoPtr<Other> cast() const
		/// Casts the AutoPtr via a dynamic cast to the given type.
		/// Returns an AutoPtr containing NULL if the cast fails.
		/// Example: (assume class Sub: public Super)
		///    AutoPtr<Super> super(new Sub());
		///    AutoPtr<Sub> sub = super.cast<Sub>();
		///    poco_assert (sub.get());
	{
		Other* pOther = dynamic_cast<Other*>(_ptr);
		return AutoPtr<Other>(pOther, true);
	}

	template <class Other> 
	AutoPtr<Other> unsafeCast() const
		/// Casts the AutoPtr via a static cast to the given type.
		/// Example: (assume class Sub: public Super)
		///    AutoPtr<Super> super(new Sub());
		///    AutoPtr<Sub> sub = super.unsafeCast<Sub>();
		///    poco_assert (sub.get());
	{
		Other* pOther = static_cast<Other*>(_ptr);
		return AutoPtr<Other>(pOther, true);
	}

	C* operator -> ()
	{
		if (_ptr)
			return _ptr;
		else
			throw NullPointerException();
	}

	const C* operator -> () const
	{
		if (_ptr)
			return _ptr;
		else
			throw NullPointerException();
	}

	C& operator * ()
	{
		if (_ptr)
			return *_ptr;
		else
			throw NullPointerException();
	}

	const C& operator * () const
	{
		if (_ptr)
			return *_ptr;
		else
			throw NullPointerException();
	}

	C* get()
	{
		return _ptr;
	}

	const C* get() const
	{
		return _ptr;
	}

	operator C* ()
	{
		return _ptr;
	}
	
	operator const C* () const
	{
		return _ptr;
	}
	
	bool operator ! () const
	{
		return _ptr == 0;
	}

	bool isNull() const
	{
		return _ptr == 0;
	}
	
	C* duplicate()
	{
		if (_ptr) _ptr->duplicate();
		return _ptr;
	}

	bool operator == (const AutoPtr& ptr) const
	{
		return _ptr == ptr._ptr;
	}

	bool operator == (const C* ptr) const
	{
		return _ptr == ptr;
	}

	bool operator == (C* ptr) const
	{
		return _ptr == ptr;
	}

	bool operator != (const AutoPtr& ptr) const
	{
		return _ptr != ptr._ptr;
	}

	bool operator != (const C* ptr) const
	{
		return _ptr != ptr;
	}

	bool operator != (C* ptr) const
	{
		return _ptr != ptr;
	}

	bool operator < (const AutoPtr& ptr) const
	{
		return _ptr < ptr._ptr;
	}

	bool operator < (const C* ptr) const
	{
		return _ptr < ptr;
	}

	bool operator < (C* ptr) const
	{
		return _ptr < ptr;
	}

	bool operator <= (const AutoPtr& ptr) const
	{
		return _ptr <= ptr._ptr;
	}

	bool operator <= (const C* ptr) const
	{
		return _ptr <= ptr;
	}

	bool operator <= (C* ptr) const
	{
		return _ptr <= ptr;
	}

	bool operator > (const AutoPtr& ptr) const
	{
		return _ptr > ptr._ptr;
	}

	bool operator > (const C* ptr) const
	{
		return _ptr > ptr;
	}

	bool operator > (C* ptr) const
	{
		return _ptr > ptr;
	}

	bool operator >= (const AutoPtr& ptr) const
	{
		return _ptr >= ptr._ptr;
	}

	bool operator >= (const C* ptr) const
	{
		return _ptr >= ptr;
	}

	bool operator >= (C* ptr) const
	{
		return _ptr >= ptr;
	}

private:
	C* _ptr;
};


template <class C>
inline void swap(AutoPtr<C>& p1, AutoPtr<C>& p2)
{
	p1.swap(p2);
}


} // namespace Poco


#endif // Foundation_AutoPtr_INCLUDED
