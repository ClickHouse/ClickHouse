//
// TimerTask.cpp
//
// Library: Util
// Package: Timer
// Module:  TimerTask
//
// Copyright (c) 2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Util/TimerTask.h"


namespace Poco {
namespace Util {


TimerTask::TimerTask():
	_lastExecution(0),
	_isCancelled(false)
{
}


TimerTask::~TimerTask()
{
}


void TimerTask::cancel()
{
	_isCancelled = true;
}


} } // namespace Poco::Util
