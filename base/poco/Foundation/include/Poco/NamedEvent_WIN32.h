//
// NamedEvent_WIN32.h
//
// Library: Foundation
// Package: Processes
// Module:  NamedEvent
//
// Definition of the NamedEventImpl class for Windows.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_NamedEvent_WIN32_INCLUDED
#define Foundation_NamedEvent_WIN32_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/UnWindows.h"


namespace Poco {


class Foundation_API NamedEventImpl
{
protected:
	NamedEventImpl(const std::string& name);	
	~NamedEventImpl();
	void setImpl();
	void waitImpl();
	
private:
	std::string _name;
	HANDLE      _event;	
};


} // namespace Poco


#endif // Foundation_NamedEvent_WIN32_INCLUDED
