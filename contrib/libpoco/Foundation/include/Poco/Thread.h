//
// Thread.h
//
// $Id: //poco/1.4/Foundation/include/Poco/Thread.h#3 $
//
// Library: Foundation
// Package: Threading
// Module:  Thread
//
// Definition of the Thread class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_Thread_INCLUDED
#define Foundation_Thread_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Event.h"
#include "Poco/Mutex.h"


#if defined(POCO_OS_FAMILY_WINDOWS)
#if defined(_WIN32_WCE)
#include "Poco/Thread_WINCE.h"
#else
#include "Poco/Thread_WIN32.h"
#endif
#elif defined(POCO_VXWORKS)
#include "Poco/Thread_VX.h"
#else
#include "Poco/Thread_POSIX.h"
#endif


namespace Poco {


class Runnable;
class ThreadLocalStorage;


class Foundation_API Thread: private ThreadImpl
	/// This class implements a platform-independent
	/// wrapper to an operating system thread.
	///
	/// Every Thread object gets a unique (within
	/// its process) numeric thread ID.
	/// Furthermore, a thread can be assigned a name.
	/// The name of a thread can be changed at any time.
{
public:	
	typedef ThreadImpl::TIDImpl TID;

	using ThreadImpl::Callable;

	enum Priority
		/// Thread priorities.
	{
		PRIO_LOWEST  = PRIO_LOWEST_IMPL, /// The lowest thread priority.
		PRIO_LOW     = PRIO_LOW_IMPL,    /// A lower than normal thread priority.
		PRIO_NORMAL  = PRIO_NORMAL_IMPL, /// The normal thread priority.
		PRIO_HIGH    = PRIO_HIGH_IMPL,   /// A higher than normal thread priority.
		PRIO_HIGHEST = PRIO_HIGHEST_IMPL /// The highest thread priority.
	};
	
	enum Policy
	{
		POLICY_DEFAULT = POLICY_DEFAULT_IMPL
	};

	Thread();
		/// Creates a thread. Call start() to start it.
		
	Thread(const std::string& name);
		/// Creates a named thread. Call start() to start it.
		
	~Thread();
		/// Destroys the thread.

	int id() const;
		/// Returns the unique thread ID of the thread.

	TID tid() const;
		/// Returns the native thread ID of the thread.

	std::string name() const;
		/// Returns the name of the thread.

	std::string getName() const;
		/// Returns the name of the thread.

	void setName(const std::string& name);
		/// Sets the name of the thread.

	void setPriority(Priority prio);
		/// Sets the thread's priority.
		///
		/// Some platform only allow changing a thread's priority
		/// if the process has certain privileges.

	Priority getPriority() const;
		/// Returns the thread's priority.

	void setOSPriority(int prio, int policy = POLICY_DEFAULT);
		/// Sets the thread's priority, using an operating system specific
		/// priority value. Use getMinOSPriority() and getMaxOSPriority() to
		/// obtain mininum and maximum priority values. Additionally,
		/// a scheduling policy can be specified. The policy is currently
		/// only used on POSIX platforms where the values SCHED_OTHER (default),
		/// SCHED_FIFO and SCHED_RR are supported.
		
	int getOSPriority() const;
		/// Returns the thread's priority, expressed as an operating system
		/// specific priority value.
		///
		/// May return 0 if the priority has not been explicitly set.
		
	static int getMinOSPriority(int policy = POLICY_DEFAULT);
		/// Returns the minimum operating system-specific priority value,
		/// which can be passed to setOSPriority() for the given policy.
		
	static int getMaxOSPriority(int policy = POLICY_DEFAULT);
		/// Returns the maximum operating system-specific priority value,
		/// which can be passed to setOSPriority() for the given policy.

	void setStackSize(int size);
		/// Sets the thread's stack size in bytes.
		/// Setting the stack size to 0 will use the default stack size.
		/// Typically, the real stack size is rounded up to the nearest
		/// page size multiple.

	int getStackSize() const;
		/// Returns the thread's stack size in bytes.
		/// If the default stack size is used, 0 is returned.

	void start(Runnable& target);
		/// Starts the thread with the given target.
		///
		/// Note that the given Runnable object must remain
		/// valid during the entire lifetime of the thread, as
		/// only a reference to it is stored internally.

	void start(Callable target, void* pData = 0);
		/// Starts the thread with the given target and parameter.

	template <class Functor>
	void startFunc(Functor fn)
		/// Starts the thread with the given functor object or lambda.
	{
		startImpl(new FunctorRunnable<Functor>(fn));
	}

	void join();
		/// Waits until the thread completes execution.	
		/// If multiple threads try to join the same
		/// thread, the result is undefined.
		
	void join(long milliseconds);
		/// Waits for at most the given interval for the thread
		/// to complete. Throws a TimeoutException if the thread
		/// does not complete within the specified time interval.
		
	bool tryJoin(long milliseconds);
		/// Waits for at most the given interval for the thread
		/// to complete. Returns true if the thread has finished,
		/// false otherwise.

	bool isRunning() const;
		/// Returns true if the thread is running.

	static bool trySleep(long milliseconds);
		/// Starts an interruptible sleep. When trySleep() is called,
		/// the thread will remain suspended until:
		///   - the timeout expires or 
		///   - wakeUp() is called
		/// 
		/// Function returns true if sleep attempt was completed, false
		/// if sleep was interrupted by a wakeUp() call.
		/// A frequent scenario where trySleep()/wakeUp() pair of functions
		/// is useful is with threads spending most of the time idle,
		/// with periodic activity between the idle times; trying to sleep
		/// (as opposed to sleeping) allows immediate ending of idle thread
		/// from the outside.
		/// 
		/// The trySleep() and wakeUp() calls should be used with 
		/// understanding that the suspended state is not a true sleep, 
		/// but rather a state of waiting for an event, with timeout 
		/// expiration. This makes order of calls significant; calling 
		/// wakeUp() before calling trySleep() will prevent the next  
		/// trySleep() call to actually suspend the thread (which, in 
		/// some scenarios, may be desirable behavior).

	void wakeUp();
		/// Wakes up the thread which is in the state of interruptible
		/// sleep. For threads that are not suspended, calling this
		/// function has the effect of preventing the subsequent
		/// trySleep() call to put thread in a suspended state.

	static void sleep(long milliseconds);
		/// Suspends the current thread for the specified
		/// amount of time.

	static void yield();
		/// Yields cpu to other threads.

	static Thread* current();
		/// Returns the Thread object for the currently active thread.
		/// If the current thread is the main thread, 0 is returned.

 	static TID currentTid();
 		/// Returns the native thread ID for the current thread.    

protected:
	ThreadLocalStorage& tls();
		/// Returns a reference to the thread's local storage.

	void clearTLS();
		/// Clears the thread's local storage.

	std::string makeName();
		/// Creates a unique name for a thread.
		
	static int uniqueId();
		/// Creates and returns a unique id for a thread.

	template <class Functor>
	class FunctorRunnable: public Runnable
	{
	public:
		FunctorRunnable(const Functor& functor):
			_functor(functor)
		{
		}

		~FunctorRunnable()
		{
		}

		void run()
		{
			_functor();
		}
	
	private:
		Functor _functor;
	};

private:
	Thread(const Thread&);
	Thread& operator = (const Thread&);

	int                 _id;
	std::string         _name;
	ThreadLocalStorage* _pTLS;
	Event               _event;
	mutable FastMutex   _mutex;

	friend class ThreadLocalStorage;
	friend class PooledThread;
};


//
// inlines
//
inline Thread::TID Thread::tid() const
{
	return tidImpl();
}


inline int Thread::id() const
{
	return _id;
}


inline std::string Thread::name() const
{
	FastMutex::ScopedLock lock(_mutex);
	
	return _name;
}


inline std::string Thread::getName() const
{
	FastMutex::ScopedLock lock(_mutex);
	
	return _name;
}


inline bool Thread::isRunning() const
{
	return isRunningImpl();
}


inline void Thread::sleep(long milliseconds)
{
	sleepImpl(milliseconds);
}


inline void Thread::yield()
{
	yieldImpl();
}


inline Thread* Thread::current()
{
	return static_cast<Thread*>(currentImpl());
}


inline void Thread::setOSPriority(int prio, int policy)
{
	setOSPriorityImpl(prio, policy);	
}

	
inline int Thread::getOSPriority() const
{
	return getOSPriorityImpl();
}

	
inline int Thread::getMinOSPriority(int policy)
{
	return ThreadImpl::getMinOSPriorityImpl(policy);
}

	
inline int Thread::getMaxOSPriority(int policy)
{
	return ThreadImpl::getMaxOSPriorityImpl(policy);
}


inline void Thread::setStackSize(int size)
{
	setStackSizeImpl(size);
}


inline int Thread::getStackSize() const
{
	return getStackSizeImpl();
}


inline Thread::TID Thread::currentTid()
{
	return currentTidImpl();
}


} // namespace Poco


#endif // Foundation_Thread_INCLUDED
