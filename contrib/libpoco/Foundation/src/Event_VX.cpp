//
// Event_POSIX.cpp
//
// $Id: //poco/1.4/Foundation/src/Event_VX.cpp#1 $
//
// Library: Foundation
// Package: Threading
// Module:  Event
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Event_VX.h"
#include <sysLib.h>


namespace Poco {


EventImpl::EventImpl(bool autoReset): _auto(autoReset), _state(false)
{
	_sem = semCCreate(SEM_Q_PRIORITY, 0);
	if (_sem == 0)
		throw Poco::SystemException("cannot create event");
}


EventImpl::~EventImpl()
{
	semDelete(_sem);
}


void EventImpl::setImpl()
{
	if (_auto)
	{
		if (semGive(_sem) != OK)
			throw SystemException("cannot set event");
	}
	else
	{
		_state = true;
		if (semFlush(_sem) != OK)
			throw SystemException("cannot set event");
	}
}


void EventImpl::resetImpl()
{
	_state = false;
}


void EventImpl::waitImpl()
{
	if (!_state)
	{
		if (semTake(_sem, WAIT_FOREVER) != OK)
			throw SystemException("cannot wait for event");
	}
}


bool EventImpl::waitImpl(long milliseconds)
{
	if (!_state)
	{
		int ticks = milliseconds*sysClkRateGet()/1000;
		return semTake(_sem, ticks) == OK;
	}
	else return true;
}


} // namespace Poco
