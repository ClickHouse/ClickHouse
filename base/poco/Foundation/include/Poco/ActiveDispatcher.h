//
// ActiveDispatcher.h
//
// Library: Foundation
// Package: Threading
// Module:  ActiveObjects
//
// Definition of the ActiveDispatcher class.
//
// Copyright (c) 2006-2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_ActiveDispatcher_INCLUDED
#define Foundation_ActiveDispatcher_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Runnable.h"
#include "Poco/Thread.h"
#include "Poco/ActiveStarter.h"
#include "Poco/ActiveRunnable.h"
#include "Poco/NotificationQueue.h"


namespace Poco {


class Foundation_API ActiveDispatcher: protected Runnable
	/// This class is used to implement an active object
	/// with strictly serialized method execution.
	///
	/// An active object, which is an ordinary object
	/// containing ActiveMethod members, executes all
	/// active methods in their own thread. 
	/// This behavior does not fit the "classic"
	/// definition of an active object, which serializes
	/// the execution of active methods (in other words,
	/// only one active method can be running at any given
	/// time).
	///
	/// Using this class as a base class, the serializing
	/// behavior for active objects can be implemented.
	/// 
	/// The following example shows how this is done:
	///
	///     class ActiveObject: public ActiveDispatcher
	///     {
	///     public:
	///         ActiveObject():
	///             exampleActiveMethod(this, &ActiveObject::exampleActiveMethodImpl)
	///         {
	///         }
	///
	///         ActiveMethod<std::string, std::string, ActiveObject, ActiveStarter<ActiveDispatcher> > exampleActiveMethod;
	///
	///     protected:
	///         std::string exampleActiveMethodImpl(const std::string& arg)
	///         {
	///             ...
	///         }
	///     };
	///
	/// The only things different from the example in
	/// ActiveMethod is that the ActiveObject in this case
	/// inherits from ActiveDispatcher, and that the ActiveMethod
	/// template for exampleActiveMethod has an additional parameter,
	/// specifying the specialized ActiveStarter for ActiveDispatcher.
{
public:
	ActiveDispatcher();
		/// Creates the ActiveDispatcher.

	ActiveDispatcher(Thread::Priority prio);
		/// Creates the ActiveDispatcher and sets
		/// the priority of its thread.

	virtual ~ActiveDispatcher();
		/// Destroys the ActiveDispatcher.

	void start(ActiveRunnableBase::Ptr pRunnable);
		/// Adds the Runnable to the dispatch queue.

	void cancel();
		/// Cancels all queued methods.
		
protected:
	void run();
	void stop();

private:
	Thread            _thread;
	NotificationQueue _queue;
};


template <>
class ActiveStarter<ActiveDispatcher>
	/// A specialization of ActiveStarter
	/// for ActiveDispatcher.
{
public:
	static void start(ActiveDispatcher* pOwner, ActiveRunnableBase::Ptr pRunnable)
	{
		pOwner->start(pRunnable);
	}
};


} // namespace Poco


#endif // Foundation_ActiveDispatcher_INCLUDED
