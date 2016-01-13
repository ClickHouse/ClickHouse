//
// Event.cpp
//
// $Id: //poco/1.4/Foundation/src/Event.cpp#2 $
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


#include "Poco/Event.h"


#if defined(POCO_OS_FAMILY_WINDOWS)
#include "Event_WIN32.cpp"
#elif defined(POCO_VXWORKS)
#include "Event_VX.cpp"
#else
#include "Event_POSIX.cpp"
#endif


namespace Poco {


Event::Event(bool autoReset): EventImpl(autoReset)
{
}


Event::~Event()
{
}


} // namespace Poco
