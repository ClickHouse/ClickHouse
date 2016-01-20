//
// ThreadTarget.cpp
//
// $Id: ThreadTarget.cpp 762 2008-09-16 19:04:32Z obiltschnig $
//
// Library: Foundation
// Package: Threading
// Module:  ThreadTarget
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/ThreadTarget.h"


namespace Poco {


ThreadTarget::ThreadTarget(Callback method): _method(method)
{
}


ThreadTarget::ThreadTarget(const ThreadTarget& te): _method(te._method)
{
}


ThreadTarget& ThreadTarget::operator = (const ThreadTarget& te)
{
	_method  = te._method;
	return *this;
}


ThreadTarget::~ThreadTarget()
{
}


} // namespace Poco
