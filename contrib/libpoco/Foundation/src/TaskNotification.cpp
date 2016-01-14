//
// TaskNotification.cpp
//
// $Id: //poco/1.4/Foundation/src/TaskNotification.cpp#1 $
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


#include "Poco/TaskNotification.h"


namespace Poco {


TaskNotification::TaskNotification(Task* pTask):
	_pTask(pTask)
{
	if (_pTask) _pTask->duplicate();
}


TaskNotification::~TaskNotification()
{
	if (_pTask) _pTask->release();
}


TaskStartedNotification::TaskStartedNotification(Task* pTask):
	TaskNotification(pTask)
{
}

	
TaskStartedNotification::~TaskStartedNotification()
{
}


TaskCancelledNotification::TaskCancelledNotification(Task* pTask):
	TaskNotification(pTask)
{
}

	
TaskCancelledNotification::~TaskCancelledNotification()
{
}


TaskFinishedNotification::TaskFinishedNotification(Task* pTask):
	TaskNotification(pTask)
{
}

	
TaskFinishedNotification::~TaskFinishedNotification()
{
}


TaskFailedNotification::TaskFailedNotification(Task* pTask, const Exception& exc):
	TaskNotification(pTask),
	_pException(exc.clone())
{
}

	
TaskFailedNotification::~TaskFailedNotification()
{
	delete _pException;
}


TaskProgressNotification::TaskProgressNotification(Task* pTask, float progress):
	TaskNotification(pTask),
	_progress(progress)
{
}

	
TaskProgressNotification::~TaskProgressNotification()
{
}


} // namespace Poco
