//
// Task.h
//
// $Id: //poco/1.4/Foundation/include/Poco/Task.h#2 $
//
// Library: Foundation
// Package: Tasks
// Module:  Tasks
//
// Definition of the Task class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_Task_INCLUDED
#define Foundation_Task_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Runnable.h"
#include "Poco/RefCountedObject.h"
#include "Poco/Mutex.h"
#include "Poco/Event.h"


namespace Poco {


class TaskManager;
class Notification;
class NotificationCenter;


class Foundation_API Task: public Runnable, public RefCountedObject
	/// A Task is a subclass of Runnable that has a name 
	/// and supports progress reporting and cancellation.
	///
	/// A TaskManager object can be used to take care of the
	/// lifecycle of a Task.
{
public:
	enum TaskState
	{
		TASK_IDLE,
		TASK_STARTING,
		TASK_RUNNING,
		TASK_CANCELLING,
		TASK_FINISHED
	};
	
	Task(const std::string& name);
		/// Creates the Task.

	const std::string& name() const;
		/// Returns the task's name.	
		
	float progress() const;
		/// Returns the task's progress.
		/// The value will be between 0.0 (just started)
		/// and 1.0 (completed).

	virtual void cancel();
		/// Requests the task to cancel itself. For cancellation
		/// to work, the task's runTask() method must periodically
		/// call isCancelled() and react accordingly.
		///
		/// Can be overridden to implement custom behavior,
		/// but the base class implementation of cancel() should
		/// be called to ensure proper behavior.

	bool isCancelled() const;
		/// Returns true if cancellation of the task has been
		/// requested. 
		///
		/// A Task's runTask() method should periodically
		/// call this method and stop whatever it is doing in an
		/// orderly way when this method returns true.

	TaskState state() const;
		/// Returns the task's current state.

	void reset();
		/// Sets the task's progress to zero and clears the
		/// cancel flag.
		
	virtual void runTask() = 0;
		/// Do whatever the task needs to do. Must
		/// be overridden by subclasses.
		
	void run();
		/// Calls the task's runTask() method and notifies the owner
		/// of the task's start and completion.

protected:
	bool sleep(long milliseconds);
		/// Suspends the current thread for the specified
		/// amount of time.
		///
		/// If the task is cancelled while it is sleeping,
		/// sleep() will return immediately and the return
		/// value will be true. If the time interval
		/// passes without the task being cancelled, the
		/// return value is false.
		///
		/// A Task should use this method in favor of Thread::sleep().

	void setProgress(float progress);
		/// Sets the task's progress.
		/// The value should be between 0.0 (just started)
		/// and 1.0 (completed).

	virtual void postNotification(Notification* pNf);
		/// Posts a notification to the task manager's
		/// notification center.
		///
		/// A task can use this method to post custom
		/// notifications about its progress.

	void setOwner(TaskManager* pOwner);
		/// Sets the (optional) owner of the task.
		
	TaskManager* getOwner() const;
		/// Returns the owner of the task, which may be NULL.

	void setState(TaskState state);
		/// Sets the task's state.

	virtual ~Task();
		/// Destroys the Task.
		
private:
	Task();
	Task(const Task&);
	Task& operator = (const Task&);
	
	std::string       _name;
	TaskManager*      _pOwner;
	float             _progress;
	TaskState         _state;
	Event             _cancelEvent;
	mutable FastMutex _mutex;
	
	friend class TaskManager;
};


//
// inlines
//
inline const std::string& Task::name() const
{
	return _name;
}


inline float Task::progress() const
{
	FastMutex::ScopedLock lock(_mutex);

	return _progress;
}


inline bool Task::isCancelled() const
{
	return _state == TASK_CANCELLING;
}


inline Task::TaskState Task::state() const
{
	return _state;
}


inline TaskManager* Task::getOwner() const
{
	FastMutex::ScopedLock lock(_mutex);

	return _pOwner;
}


} // namespace Poco


#endif // Foundation_Task_INCLUDED
