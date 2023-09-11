//
// Task.cpp
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


#include "Poco/Task.h"
#include "Poco/TaskManager.h"
#include "Poco/Exception.h"

#include <array>


namespace Poco {


Task::Task(const std::string& name):
	_name(name),
	_pOwner(0),
	_progress(0),
	_state(TASK_IDLE),
	_cancelEvent(false)
{
}


Task::~Task()
{
}


void Task::cancel()
{
	_state = TASK_CANCELLING;
	_cancelEvent.set();
	if (_pOwner)
		_pOwner->taskCancelled(this);
}


void Task::reset()
{
	_progress = 0.0;
	_state    = TASK_IDLE;
	_cancelEvent.reset();
}


void Task::run()
{
	TaskManager* pOwner = getOwner();
	if (pOwner)
		pOwner->taskStarted(this);		
	try
	{
		/** Task can be already cancelled.
		 *  To prevent endless executing already cancelled task _state is assigned to TASK_RUNNING only if _state != TASK_CANCELLING
		 */
		#pragma clang diagnostic push
		#pragma clang diagnostic ignored "-Wmissing-braces"
		std::array<TaskState, 3> allowed_states{TASK_IDLE, TASK_STARTING, TASK_FINISHED};
		#pragma clang diagnostic pop
		for (auto & expected : allowed_states)
			if (_state.compare_exchange_strong(expected, TASK_RUNNING))
				break;

		if (_state == TASK_RUNNING)
		runTask();
	}
	catch (Exception& exc)
	{
		if (pOwner)
			pOwner->taskFailed(this, exc);
	}
	catch (std::exception& exc)
	{
		if (pOwner)
			pOwner->taskFailed(this, SystemException(exc.what()));
	}
	catch (...)
	{
		if (pOwner)
			pOwner->taskFailed(this, SystemException("unknown exception"));
	}
	_state = TASK_FINISHED;
	if (pOwner)
		pOwner->taskFinished(this);
}


bool Task::sleep(long milliseconds)
{
	return _cancelEvent.tryWait(milliseconds);
}


void Task::setProgress(float progress)
{
	FastMutex::ScopedLock lock(_mutex);

	if (_progress != progress)
	{
		_progress = progress;
		if (_pOwner)
			_pOwner->taskProgress(this, _progress);
	}
}


void Task::setOwner(TaskManager* pOwner)
{
	FastMutex::ScopedLock lock(_mutex);

	_pOwner = pOwner;
}


void Task::setState(TaskState state)
{
	_state = state;
}


void Task::postNotification(Notification* pNf)
{
	poco_check_ptr (pNf);

	FastMutex::ScopedLock lock(_mutex);
	
	if (_pOwner)
	{
		_pOwner->postNotification(pNf);
	}
}


} // namespace Poco
