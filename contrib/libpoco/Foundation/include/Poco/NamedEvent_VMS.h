//
// NamedEvent_VMS.h
//
// $Id: //poco/1.4/Foundation/include/Poco/NamedEvent_VMS.h#1 $
//
// Library: Foundation
// Package: Processes
// Module:  NamedEvent
//
// Definition of the NamedEventImpl class for OpenVMS.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_NamedEvent_VMS_INCLUDED
#define Foundation_NamedEvent_VMS_INCLUDED


#include "Poco/Foundation.h"


namespace Poco {


class Foundation_API NamedEventImpl
{
protected:
	NamedEventImpl(const std::string& name);	
	~NamedEventImpl();
	void setImpl();
	void waitImpl();
	
private:
	std::string    _name;
	unsigned short _mbxChan;
};


} // namespace Poco


#endif // Foundation_NamedEvent_VMS_INCLUDED
