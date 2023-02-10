//
// AutoReleasePool.h
//
// Library: Foundation
// Package: Core
// Module:  AutoReleasePool
//
// Definition of the AutoReleasePool class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_AutoReleasePool_INCLUDED
#define Foundation_AutoReleasePool_INCLUDED


#include "Poco/Foundation.h"
#include <list>


namespace Poco {


template <class C>
class AutoReleasePool
	/// An AutoReleasePool implements simple garbage collection for 
	/// reference-counted objects.
	/// It temporarily takes ownwership of reference-counted objects that 
	/// nobody else wants to take ownership of and releases them 
	/// at a later, appropriate point in time.
	///
	/// Note: The correct way to add an object hold by an AutoPtr<> to
	/// an AutoReleasePool is by invoking the AutoPtr's duplicate()
	/// method. Example:
	///    AutoReleasePool<C> arp;
	///    AutoPtr<C> ptr = new C;
	///    ...
	///    arp.add(ptr.duplicate());
{
public:
	AutoReleasePool()
		/// Creates the AutoReleasePool.
	{
	}
	
	~AutoReleasePool()
		/// Destroys the AutoReleasePool and releases
		/// all objects it currently holds.
	{
		release();
	}
	
	void add(C* pObject)
		/// Adds the given object to the AutoReleasePool.
		/// The object's reference count is not modified
	{
		if (pObject)
			_list.push_back(pObject);
	}
	
	void release()
		/// Releases all objects the AutoReleasePool currently holds
		/// by calling each object's release() method.
	{
		while (!_list.empty())
		{
			_list.front()->release();
			_list.pop_front();
		}
	}
	
private:
	typedef std::list<C*> ObjectList;
	
	ObjectList _list;
};


} // namespace Poco


#endif // Foundation_AutoReleasePool_INCLUDED
