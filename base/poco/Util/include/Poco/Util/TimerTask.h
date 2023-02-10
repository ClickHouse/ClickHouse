//
// TimerTask.h
//
// Library: Util
// Package: Timer
// Module:  TimerTask
//
// Definition of the TimerTask class.
//
// Copyright (c) 2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Util_TimerTask_INCLUDED
#define Util_TimerTask_INCLUDED


#include "Poco/Util/Util.h"
#include "Poco/Runnable.h"
#include "Poco/RefCountedObject.h"
#include "Poco/AutoPtr.h"
#include "Poco/Timestamp.h"


namespace Poco {
namespace Util {


class Util_API TimerTask: public Poco::RefCountedObject, public Poco::Runnable
	/// A task that can be scheduled for one-time or 
	/// repeated execution by a Timer.
	///
	/// This is an abstract class. Subclasses must override the run() member
	/// function to implement the actual task logic.
{
public:
	typedef Poco::AutoPtr<TimerTask> Ptr;
	
	TimerTask();
		/// Creates the TimerTask.
		
	void cancel();
		/// Cancels the execution of the timer.
		/// If the task has been scheduled for one-time execution and has 
		/// not yet run, or has not yet been scheduled, it will never run. 
		/// If the task has been scheduled for repeated execution, it will never 
		/// run again. If the task is running when this call occurs, the task 
		/// will run to completion, but will never run again.
		///
		/// Warning: A TimerTask that has been cancelled must not be scheduled again.
		/// An attempt to do so results in a Poco::Util::IllegalStateException being thrown.
	
	bool isCancelled() const;
		/// Returns true iff the TimerTask has been cancelled by a call
		/// to cancel().
	
	Poco::Timestamp lastExecution() const;
		/// Returns the time of the last execution of the timer task.
		///
		/// Returns 0 if the timer has never been executed.
		
protected:
	~TimerTask();
		/// Destroys the TimerTask.
	
private:
	TimerTask(const TimerTask&);
	TimerTask& operator = (const TimerTask&);
	
	Poco::Timestamp _lastExecution;
	bool _isCancelled;
	
	friend class TaskNotification;
};


//
// inlines
//
inline bool TimerTask::isCancelled() const
{
	return _isCancelled;
}


inline Poco::Timestamp TimerTask::lastExecution() const
{
	return _lastExecution;
}


} } // namespace Poco::Util


#endif // Util_TimerTask_INCLUDED
