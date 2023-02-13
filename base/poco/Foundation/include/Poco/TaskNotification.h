//
// TaskNotification.h
//
// Library: Foundation
// Package: Tasks
// Module:  Tasks
//
// Definition of the TaskNotification class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_TaskNotification_INCLUDED
#define Foundation_TaskNotification_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Notification.h"
#include "Poco/Task.h"


namespace Poco {


class Foundation_API TaskNotification: public Notification
	/// Base class for TaskManager notifications.
{
public:
	TaskNotification(Task* pTask);
		/// Creates the TaskNotification.

	Task* task() const;
		/// Returns the subject of the notification.

protected:
	virtual ~TaskNotification();
		/// Destroys the TaskNotification.

private:
	Task* _pTask;
};


class Foundation_API TaskStartedNotification: public TaskNotification
	/// This notification is posted by the TaskManager for
	/// every task that has been started.
{
public:
	TaskStartedNotification(Task* pTask);
	
protected:
	~TaskStartedNotification();
};


class Foundation_API TaskCancelledNotification: public TaskNotification
	/// This notification is posted by the TaskManager for
	/// every task that has been cancelled.
{
public:
	TaskCancelledNotification(Task* pTask);

protected:
	~TaskCancelledNotification();
};


class Foundation_API TaskFinishedNotification: public TaskNotification
	/// This notification is posted by the TaskManager for
	/// every task that has finished.
{
public:
	TaskFinishedNotification(Task* pTask);

protected:
	~TaskFinishedNotification();
};


class Foundation_API TaskFailedNotification: public TaskNotification
	/// This notification is posted by the TaskManager for
	/// every task that has failed with an exception.
{
public:
	TaskFailedNotification(Task* pTask, const Exception& exc);

	const Exception& reason() const;

protected:
	~TaskFailedNotification();
	
private:
	Exception* _pException;
};


class Foundation_API TaskProgressNotification: public TaskNotification
	/// This notification is posted by the TaskManager for
	/// a task when its progress changes.
{
public:
	TaskProgressNotification(Task* pTask, float progress);

	float progress() const;

protected:
	~TaskProgressNotification();
	
private:
	float _progress;
};


template <class C>
class TaskCustomNotification: public TaskNotification
	/// This is a template for "custom" notification.
	/// Unlike other notifications, this notification
	/// is instantiated and posted by the task itself.
	/// The purpose is to provide generic notification
	/// mechanism between the task and its observer(s).
{
public:
	TaskCustomNotification(Task* pTask, const C& custom):
		TaskNotification(pTask),
		_custom(custom)
	{
	}

	const C& custom() const
	{
		return _custom;
	}

protected:
	~TaskCustomNotification(){};
	
private:
	C _custom;
};


//
// inlines
//
inline Task* TaskNotification::task() const
{
	return _pTask;
}


inline const Exception& TaskFailedNotification::reason() const
{
	return *_pException;
}


inline float TaskProgressNotification::progress() const
{
	return _progress;
}


} // namespace Poco


#endif // Foundation_TaskNotification_INCLUDED
