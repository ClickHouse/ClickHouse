//
// NamedMutex.cpp
//
// Library: Foundation
// Package: Processes
// Module:  NamedMutex
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/NamedMutex.h"


#if defined(POCO_OS_FAMILY_WINDOWS) && defined(POCO_WIN32_UTF8)
#include "NamedMutex_WIN32U.cpp"
#elif defined(POCO_OS_FAMILY_WINDOWS)
#include "NamedMutex_WIN32.cpp"
#elif POCO_OS == POCO_OS_ANDROID
#include "NamedMutex_Android.cpp"
#elif defined(POCO_OS_FAMILY_UNIX)
#include "NamedMutex_UNIX.cpp"
#endif


namespace Poco {


NamedMutex::NamedMutex(const std::string& name):
	NamedMutexImpl(name)
{
}


NamedMutex::~NamedMutex()
{
}


} // namespace Poco
