//
// SynchronizedObject.h
//
// $Id: //poco/1.4/Foundation/include/Poco/SynchronizedObject.h#1 $
//
// Library: Foundation
// Package: Threading
// Module:  SynchronizedObject
//
// Definition of the SynchronizedObject class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_SynchronizedObject_INCLUDED
#define Foundation_SynchronizedObject_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Mutex.h"
#include "Poco/Event.h"


namespace Poco {


class Foundation_API SynchronizedObject
	/// This class aggregates a Mutex and an Event
	/// and can act as a base class for all objects
	/// requiring synchronization in a multithreaded
	/// scenario.
{
public:
	typedef Poco::ScopedLock<SynchronizedObject> ScopedLock;
	
	SynchronizedObject();
		/// Creates the object.
		
	virtual ~SynchronizedObject();
		/// Destroys the object.

	void lock() const;
		/// Locks the object. Blocks if the object
		/// is locked by another thread.

	bool tryLock() const;
		/// Tries to lock the object. Returns false immediately
		/// if the object is already locked by another thread
		/// Returns true if the object was successfully locked.
	
	void unlock() const;
		/// Unlocks the object so that it can be locked by
		/// other threads.
		
	void notify() const;
		/// Signals the object. 
		/// Exactly only one thread waiting for the object 
		/// can resume execution.

	void wait() const;
		/// Waits for the object to become signalled.

	void wait(long milliseconds) const;
		/// Waits for the object to become signalled.
		/// Throws a TimeoutException if the object
		/// does not become signalled within the specified
		/// time interval.

	bool tryWait(long milliseconds) const;
		/// Waits for the object to become signalled.
		/// Returns true if the object
		/// became signalled within the specified
		/// time interval, false otherwise.

private:
	mutable Mutex _mutex;
	mutable Event _event;
};


//
// inlines
//
inline void SynchronizedObject::lock() const
{
	_mutex.lock();
}


inline bool SynchronizedObject::tryLock() const
{
	return _mutex.tryLock();
}


inline void SynchronizedObject::unlock() const
{
	_mutex.unlock();
}


inline void SynchronizedObject::notify() const
{
	_event.set();
}


inline void SynchronizedObject::wait() const
{
	_event.wait();
}


inline void SynchronizedObject::wait(long milliseconds) const
{
	_event.wait(milliseconds);
}


inline bool SynchronizedObject::tryWait(long milliseconds) const
{
	return _event.tryWait(milliseconds);
}


} // namespace Poco


#endif // Foundation_SynchronizedObject_INCLUDED
