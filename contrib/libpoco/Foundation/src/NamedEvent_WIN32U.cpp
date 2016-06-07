//
// NamedEvent_WIN32.cpp
//
// $Id: //poco/1.4/Foundation/src/NamedEvent_WIN32U.cpp#1 $
//
// Library: Foundation
// Package: Processes
// Module:  NamedEvent
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/NamedEvent_WIN32U.h"
#include "Poco/Error.h"
#include "Poco/Exception.h"
#include "Poco/Format.h"
#include "Poco/UnicodeConverter.h"


namespace Poco {


NamedEventImpl::NamedEventImpl(const std::string& name):
	_name(name)
{
	UnicodeConverter::toUTF16(_name, _uname);
	_event = CreateEventW(NULL, FALSE, FALSE, _uname.c_str());
	if (!_event)
	{
		DWORD dwRetVal = GetLastError(); 
		throw SystemException(format("cannot create named event %s [Error %d: %s]", _name, (int)dwRetVal, Error::getMessage(dwRetVal)));
	}
}


NamedEventImpl::~NamedEventImpl()
{
	CloseHandle(_event);
}


void NamedEventImpl::setImpl()
{
	if (!SetEvent(_event))
		throw SystemException("cannot signal named event", _name);
}


void NamedEventImpl::waitImpl()
{
	switch (WaitForSingleObject(_event, INFINITE))
	{
	case WAIT_OBJECT_0:
		return;
	default:
		throw SystemException("wait for named event failed", _name);
	}
}


} // namespace Poco
