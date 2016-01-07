//
// Event_WIN32.cpp
//
// $Id: //poco/1.4/Foundation/src/Event_WIN32.cpp#1 $
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


#include "Poco/Event_WIN32.h"


namespace Poco {


EventImpl::EventImpl(bool autoReset)
{
	_event = CreateEventW(NULL, autoReset ? FALSE : TRUE, FALSE, NULL);
	if (!_event)
		throw SystemException("cannot create event");
}


EventImpl::~EventImpl()
{
	CloseHandle(_event);
}


void EventImpl::waitImpl()
{
	switch (WaitForSingleObject(_event, INFINITE))
	{
	case WAIT_OBJECT_0:
		return;
	default:
		throw SystemException("wait for event failed");
	}
}


bool EventImpl::waitImpl(long milliseconds)
{
	switch (WaitForSingleObject(_event, milliseconds + 1))
	{
	case WAIT_TIMEOUT:
		return false;
	case WAIT_OBJECT_0:
		return true;
	default:
		throw SystemException("wait for event failed");		
	}
}


} // namespace Poco
