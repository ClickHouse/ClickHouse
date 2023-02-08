//
// Activity.h
//
// Library: Foundation
// Package: Threading
// Module:  ActiveObjects
//
// Definition of the Activity template class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_Activity_INCLUDED
#define Foundation_Activity_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/RunnableAdapter.h"
#include "Poco/ThreadPool.h"
#include "Poco/Event.h"
#include "Poco/Mutex.h"


namespace Poco {


template <class C>
class Activity: public Runnable
	/// This template class helps to implement active objects.
	/// An active object uses threads to decouple method
	/// execution from method invocation, or to perform tasks
	/// autonomously, without intervention of a caller.
	///
	/// An activity is a (typically longer running) method
	/// that executes within its own task. Activities can
	/// be started automatically (upon object construction)
	/// or manually at a later time. Activities can also
	/// be stopped at any time. However, to make stopping
	/// an activity work, the method implementing the
	/// activity has to check periodically whether it
	/// has been requested to stop, and if so, return. 
	/// Activities are stopped before the object they belong to is
	/// destroyed. Methods implementing activities cannot have arguments
	/// or return values. 
	///
	/// Activity objects are used as follows:
	///
	///     class ActiveObject
	///     {
	///     public:
	///         ActiveObject(): 
	///             _activity(this, &ActiveObject::runActivity)
	///         {
	///             ...
	///         }
	///   
	///         ...
	///  
	///     protected:
	///         void runActivity()
	///         {
	///             while (!_activity.isStopped())
	///             {
	///                 ...
	///             }
	///         }
	///
	///     private:
	///         Activity<ActiveObject> _activity;
	///     };
{
public:
	typedef RunnableAdapter<C> RunnableAdapterType;
	typedef typename RunnableAdapterType::Callback Callback;

	Activity(C* pOwner, Callback method):
		_pOwner(pOwner),
		_runnable(*pOwner, method),
		_stopped(true),
		_running(false),
		_done(false)
		/// Creates the activity. Call start() to
		/// start it.
	{
		poco_check_ptr (pOwner);
	}
	
	~Activity()
		/// Stops and destroys the activity.
	{
		try
		{
			stop();
			wait();
		}
		catch (...)
		{
			poco_unexpected();
		}
	}
	
	void start()
		/// Starts the activity by acquiring a
		/// thread for it from the default thread pool.
	{
		start(ThreadPool::defaultPool());
	}

	void start(ThreadPool& pool)
	{
		FastMutex::ScopedLock lock(_mutex);
		
		if (!_running)
		{
			_done.reset();
			_stopped = false;
			_running = true;
			try
			{
				pool.start(*this);
			}
			catch (...)
			{
				_running = false;
				throw;
			}
		}
	}
	
	void stop()
		/// Requests to stop the activity.
	{
		FastMutex::ScopedLock lock(_mutex);

		_stopped = true;
	}
	
	void wait()
		/// Waits for the activity to complete.
	{
		if (_running)
		{
			_done.wait();
		}
	}

	void wait(long milliseconds)
		/// Waits the given interval for the activity to complete.
		/// An TimeoutException is thrown if the activity does not
		/// complete within the given interval.
	{
		if (_running)
		{
			_done.wait(milliseconds);
		}
	}
	
	bool isStopped() const
		/// Returns true if the activity has been requested to stop.
	{
		return _stopped;
	}
	
	bool isRunning() const
		/// Returns true if the activity is running.
	{
		return _running;
	}

protected:
	void run()
	{
		try
		{
			_runnable.run();
		}
		catch (...)
		{
			_running = false;
			_done.set();
			throw;
		}
		_running = false;
		_done.set();
	}
	
private:
	Activity();
	Activity(const Activity&);
	Activity& operator = (const Activity&);

	C*                  _pOwner;
	RunnableAdapterType _runnable;
	volatile bool       _stopped;
	volatile bool       _running;
	Event               _done;
	FastMutex           _mutex;
};


} // namespace Poco


#endif // Foundation_Activity_INCLUDED
