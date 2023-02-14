//
// SingletonHolder.h
//
// Library: Foundation
// Package: Core
// Module:  SingletonHolder
//
// Definition of the SingletonHolder template.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_SingletonHolder_INCLUDED
#define Foundation_SingletonHolder_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Mutex.h"


namespace Poco {


template <class S>
class SingletonHolder
	/// This is a helper template class for managing
	/// singleton objects allocated on the heap.
	/// The class ensures proper deletion (including
	/// calling of the destructor) of singleton objects
	/// when the application that created them terminates.
{
public:
	SingletonHolder():
		_pS(0)
		/// Creates the SingletonHolder.
	{
	}
	
	~SingletonHolder()
		/// Destroys the SingletonHolder and the singleton
		/// object that it holds.
	{
		delete _pS;
	}
	
	S* get()
		/// Returns a pointer to the singleton object
		/// hold by the SingletonHolder. The first call
		/// to get will create the singleton.
	{
		FastMutex::ScopedLock lock(_m);
		if (!_pS) _pS = new S;
		return _pS;
	}
	
	void reset()
		/// Deletes the singleton object.
	{
		FastMutex::ScopedLock lock(_m);
		delete _pS;
		_pS = 0;
	}
	
private:
	S* _pS;
	FastMutex _m;
};


} // namespace Poco


#endif // Foundation_SingletonHolder_INCLUDED
