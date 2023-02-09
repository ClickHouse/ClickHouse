//
// Timezone.cpp
//
// Library: Foundation
// Package: DateTime
// Module:  Timezone
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Timezone.h"
#include <ctime>


#if defined(POCO_OS_FAMILY_WINDOWS)
#if defined(_WIN32_WCE)
#include "Timezone_WINCE.cpp"
#else
#include "Timezone_WIN32.cpp"
#endif
#elif defined(POCO_VXWORKS)
#include "Timezone_VX.cpp"
#else
#include "Timezone_UNIX.cpp"
#endif


namespace Poco {


int Timezone::tzd()
{
	return utcOffset() + dst();
}


} // namespace Poco
