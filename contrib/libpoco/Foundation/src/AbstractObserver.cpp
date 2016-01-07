//
// AbstractObserver.cpp
//
// $Id: //poco/1.4/Foundation/src/AbstractObserver.cpp#1 $
//
// Library: Foundation
// Package: Notifications
// Module:  NotificationCenter
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/AbstractObserver.h"


namespace Poco {


AbstractObserver::AbstractObserver()
{
}


AbstractObserver::AbstractObserver(const AbstractObserver& observer)
{
}


AbstractObserver::~AbstractObserver()
{
}

	
AbstractObserver& AbstractObserver::operator = (const AbstractObserver& observer)
{
	return *this;
}


} // namespace Poco
