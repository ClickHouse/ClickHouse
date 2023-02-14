//
// TaskManager.h
//
// Library: Foundation
// Package: Tasks
// Module:  Tasks
//
// Definition of the TaskManager class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_TaskManager_INCLUDED
#define Foundation_TaskManager_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Mutex.h"
#include "Poco/Task.h"
#include "Poco/AutoPtr.h"
#include "Poco/NotificationCenter.h"
#include "Poco/Timestamp.h"
#include <list>


namespace Poco {


class Notification;
class ThreadPool;
class Exception;


class Foundation_API TaskManager
	/// The TaskManager manages a collection of tasks
	/// and monitors their lifetime.
	///
	/// A TaskManager has a built-in NotificationCenter that
	/// is used to send out notifications on task progress
	/// and task states. See the TaskNotification class and its
	/// subclasses for the various events that result in a notification.
	/// To keep the number of notifications small, a TaskProgressNotification
	/// will only be sent out once in 100 milliseconds.
{
public:
	typedef AutoPtr<Task>      TaskPtr;
	typedef std::list<TaskPtr> TaskList;

	TaskManager();
		/// Creates the TaskManager, using the
		/// default ThreadPool.

	TaskManager(ThreadPool& pool);
		/// Creates the TaskManager, using the
		/// given ThreadPool.

	~TaskManager();
		/// Destroys the TaskManager.

	void start(Task* pTask);
		/// Starts the given task in a thread obtained
		/// from the thread pool.
		///
		/// The TaskManager takes ownership of the Task object
		/// and deletes it when it it finished.

	void cancelAll();
		/// Requests cancellation of all tasks.
		
	void joinAll();
		/// Waits for the completion of all the threads
		/// in the TaskManager's thread pool.
		///
		/// Note: joinAll() will wait for ALL tasks in the
		/// TaskManager's ThreadPool to complete. If the
		/// ThreadPool has threads created by other
		/// facilities, these threads must also complete
		/// before joinAll() can return.

	TaskList taskList() const;
		/// Returns a copy of the internal task list.

	int count() const;
		/// Returns the number of tasks in the internal task list.

	void addObserver(const AbstractObserver& observer);
		/// Registers an observer with the NotificationCenter.
		/// Usage:
		///     Observer<MyClass, MyNotification> obs(*this, &MyClass::handleNotification);
		///     notificationCenter.addObserver(obs);

	void removeObserver(const AbstractObserver& observer);
		/// Unregisters an observer with the NotificationCenter.

	static const int MIN_PROGRESS_NOTIFICATION_INTERVAL;

protected:
	void postNotification(const Notification::Ptr& pNf);
		/// Posts a notification to the task manager's 
		/// notification center.

	void taskStarted(Task* pTask);
	void taskProgress(Task* pTask, float progress);
	void taskCancelled(Task* pTask);
	void taskFinished(Task* pTask);
	void taskFailed(Task* pTask, const Exception& exc);

private:
	ThreadPool&        _threadPool;
	TaskList           _taskList;
	Timestamp          _lastProgressNotification;
	NotificationCenter _nc;
	mutable FastMutex  _mutex;

	friend class Task;
};


//
// inlines
//
inline int TaskManager::count() const
{
	FastMutex::ScopedLock lock(_mutex);

	return (int) _taskList.size();
}


} // namespace Poco


#endif // Foundation_TaskManager_INCLUDED
