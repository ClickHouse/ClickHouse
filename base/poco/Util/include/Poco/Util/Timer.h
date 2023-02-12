//
// Timer.h
//
// Library: Util
// Package: Timer
// Module:  Timer
//
// Definition of the Timer class.
//
// Copyright (c) 2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Util_Timer_INCLUDED
#define Util_Timer_INCLUDED


#include "Poco/Util/Util.h"
#include "Poco/Util/TimerTask.h"
#include "Poco/TimedNotificationQueue.h"
#include "Poco/Thread.h"
#include "Poco/Runnable.h"


namespace Poco {
namespace Util {


class Util_API Timer: protected Poco::Runnable
	/// A Timer allows to schedule tasks (TimerTask objects) for future execution 
	/// in a background thread. Tasks may be scheduled for one-time execution, 
	/// or for repeated execution at regular intervals. 
	///
	/// The Timer object creates a thread that executes all scheduled tasks
	/// sequentially. Therefore, tasks should complete their work as quickly
	/// as possible, otherwise subsequent tasks may be delayed.
	///
	/// Timer is safe for multithreaded use - multiple threads can schedule
	/// new tasks simultaneously.
	///
	/// Acknowledgement: The interface of this class has been inspired by
	/// the java.util.Timer class from Java 1.3.
{
public:
	Timer();
		/// Creates the Timer.
	
	explicit Timer(Poco::Thread::Priority priority);
		/// Creates the Timer, using a timer thread with
		/// the given priority.
	
	~Timer();
		/// Destroys the Timer, cancelling all pending tasks.
		
	void cancel(bool wait = false);
		/// Cancels all pending tasks.
		///
		/// If a task is currently running, it is allowed to finish.
		///
		/// Task cancellation is done asynchronously. If wait
		/// is false, cancel() returns immediately and the
		/// task queue will be purged as soon as the currently
		/// running task finishes. If wait is true, waits
		/// until the queue has been purged.
	
	void schedule(TimerTask::Ptr pTask, Poco::Timestamp time);
		/// Schedules a task for execution at the specified time.
		///
		/// If the time lies in the past, the task is executed
		/// immediately.
		///
		/// Note: the relative time the task will be executed
		/// won't change if the system's time changes. If the
		/// given time is 10 seconds in the future at the point
		/// schedule() is called, the task will be executed 10
		/// seconds later, even if the system time changes in
		/// between.

	void schedule(TimerTask::Ptr pTask, Poco::Clock clock);
		/// Schedules a task for execution at the specified time.
		///
		/// If the time lies in the past, the task is executed
		/// immediately.
		
	void schedule(TimerTask::Ptr pTask, long delay, long interval);
		/// Schedules a task for periodic execution.
		///
		/// The task is first executed after the given delay.
		/// Subsequently, the task is executed periodically with
		/// the given interval in milliseconds between invocations.

	void schedule(TimerTask::Ptr pTask, Poco::Timestamp time, long interval);
		/// Schedules a task for periodic execution.
		///
		/// The task is first executed at the given time.
		/// Subsequently, the task is executed periodically with
		/// the given interval in milliseconds between invocations.
		///
		/// Note: the relative time the task will be executed
		/// won't change if the system's time changes. If the
		/// given time is 10 seconds in the future at the point
		/// schedule() is called, the task will be executed 10
		/// seconds later, even if the system time changes in
		/// between.

	void schedule(TimerTask::Ptr pTask, Poco::Clock clock, long interval);
		/// Schedules a task for periodic execution.
		///
		/// The task is first executed at the given time.
		/// Subsequently, the task is executed periodically with
		/// the given interval in milliseconds between invocations.
		
	void scheduleAtFixedRate(TimerTask::Ptr pTask, long delay, long interval);
		/// Schedules a task for periodic execution at a fixed rate.
		///
		/// The task is first executed after the given delay.
		/// Subsequently, the task is executed periodically 
		/// every number of milliseconds specified by interval.
		///
		/// If task execution takes longer than the given interval,
		/// further executions are delayed.

	void scheduleAtFixedRate(TimerTask::Ptr pTask, Poco::Timestamp time, long interval);
		/// Schedules a task for periodic execution at a fixed rate.
		///
		/// The task is first executed at the given time.
		/// Subsequently, the task is executed periodically 
		/// every number of milliseconds specified by interval.
		///
		/// If task execution takes longer than the given interval,
		/// further executions are delayed.
		///
		/// Note: the relative time the task will be executed
		/// won't change if the system's time changes. If the
		/// given time is 10 seconds in the future at the point
		/// scheduleAtFixedRate() is called, the task will be executed 10
		/// seconds later, even if the system time changes in
		/// between.

	void scheduleAtFixedRate(TimerTask::Ptr pTask, Poco::Clock clock, long interval);
		/// Schedules a task for periodic execution at a fixed rate.
		///
		/// The task is first executed at the given time.
		/// Subsequently, the task is executed periodically 
		/// every number of milliseconds specified by interval.
		///
		/// If task execution takes longer than the given interval,
		/// further executions are delayed.

protected:
	void run();
	static void validateTask(const TimerTask::Ptr& pTask);
	
private:
	Timer(const Timer&);
	Timer& operator = (const Timer&);
	
	Poco::TimedNotificationQueue _queue;
	Poco::Thread _thread;
};


} } // namespace Poco::Util


#endif // Util_Timer_INCLUDED
