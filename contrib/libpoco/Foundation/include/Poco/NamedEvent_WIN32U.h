//
// NamedEvent_WIN32U.h
//
// $Id: //poco/1.4/Foundation/include/Poco/NamedEvent_WIN32U.h#1 $
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


#ifndef Foundation_NamedEvent_WIN32U_INCLUDED
#define Foundation_NamedEvent_WIN32U_INCLUDED


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
	std::string  _name;
	std::wstring _uname;
	HANDLE      _event;	
};


} // namespace Poco


#endif // Foundation_NamedEvent_WIN32U_INCLUDED
