//
// TaskManager.cpp
//
// $Id: //poco/1.4/Foundation/src/TaskManager.cpp#1 $
//
// Library: Foundation
// Package: Tasks
// Module:  Tasks
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/TaskManager.h"
#include "Poco/TaskNotification.h"
#include "Poco/ThreadPool.h"


namespace Poco {


const int TaskManager::MIN_PROGRESS_NOTIFICATION_INTERVAL = 100000; // 100 milliseconds


TaskManager::TaskManager():
	_threadPool(ThreadPool::defaultPool())
{
}


TaskManager::TaskManager(ThreadPool& pool):
	_threadPool(pool)
{
}


TaskManager::~TaskManager()
{
}


void TaskManager::start(Task* pTask)
{
	TaskPtr pAutoTask(pTask); // take ownership immediately
	FastMutex::ScopedLock lock(_mutex);

	pAutoTask->setOwner(this);
	pAutoTask->setState(Task::TASK_STARTING);
	_taskList.push_back(pAutoTask);
	try
	{
		_threadPool.start(*pAutoTask, pAutoTask->name());
	}
	catch (...)
	{
		// Make sure that we don't act like we own the task since
		// we never started it.  If we leave the task on our task
		// list, the size of the list is incorrect.
		_taskList.pop_back();
		throw;
	}
}


void TaskManager::cancelAll()
{
	FastMutex::ScopedLock lock(_mutex);

	for (TaskList::iterator it = _taskList.begin(); it != _taskList.end(); ++it)
	{
		(*it)->cancel();
	}
}


void TaskManager::joinAll()
{
	_threadPool.joinAll();
}


TaskManager::TaskList TaskManager::taskList() const
{
	FastMutex::ScopedLock lock(_mutex);
	
	return _taskList;
}


void TaskManager::addObserver(const AbstractObserver& observer)
{
	_nc.addObserver(observer);
}


void TaskManager::removeObserver(const AbstractObserver& observer)
{
	_nc.removeObserver(observer);
}


void TaskManager::postNotification(const Notification::Ptr& pNf)
{
	_nc.postNotification(pNf);
}


void TaskManager::taskStarted(Task* pTask)
{
	_nc.postNotification(new TaskStartedNotification(pTask));
}


void TaskManager::taskProgress(Task* pTask, float progress)
{
	FastMutex::ScopedLock lock(_mutex);

	if (_lastProgressNotification.isElapsed(MIN_PROGRESS_NOTIFICATION_INTERVAL))
	{
		_lastProgressNotification.update();
		_nc.postNotification(new TaskProgressNotification(pTask, progress));
	}
}


void TaskManager::taskCancelled(Task* pTask)
{
	_nc.postNotification(new TaskCancelledNotification(pTask));
}


void TaskManager::taskFinished(Task* pTask)
{
	_nc.postNotification(new TaskFinishedNotification(pTask));
	
	FastMutex::ScopedLock lock(_mutex);
	for (TaskList::iterator it = _taskList.begin(); it != _taskList.end(); ++it)
	{
		if (*it == pTask)
		{
			_taskList.erase(it);
			break;
		}
	}
}


void TaskManager::taskFailed(Task* pTask, const Exception& exc)
{
	_nc.postNotification(new TaskFailedNotification(pTask, exc));
}


} // namespace Poco
