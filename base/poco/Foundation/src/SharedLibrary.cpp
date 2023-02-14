//
// SharedLibrary.cpp
//
// Library: Foundation
// Package: SharedLibrary
// Module:  SharedLibrary
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/SharedLibrary.h"
#include "Poco/Exception.h"


#if defined(hpux) || defined(_hpux)
#include "SharedLibrary_HPUX.cpp"
#elif defined(POCO_VXWORKS)
#include "SharedLibrary_VX.cpp"
#elif defined(POCO_OS_FAMILY_UNIX)
#include "SharedLibrary_UNIX.cpp"
#elif defined(POCO_OS_FAMILY_WINDOWS) && defined(POCO_WIN32_UTF8)
#include "SharedLibrary_WIN32U.cpp"
#elif defined(POCO_OS_FAMILY_WINDOWS)
#include "SharedLibrary_WIN32.cpp"
#endif


namespace Poco {


SharedLibrary::SharedLibrary()
{
}


SharedLibrary::SharedLibrary(const std::string& path)
{
	loadImpl(path, 0);
}


SharedLibrary::SharedLibrary(const std::string& path, int flags)
{
	loadImpl(path, flags);
}


SharedLibrary::~SharedLibrary()
{
}


void SharedLibrary::load(const std::string& path)
{
	loadImpl(path, 0);
}


void SharedLibrary::load(const std::string& path, int flags)
{
	loadImpl(path, flags);
}


void SharedLibrary::unload()
{
	unloadImpl();
}


bool SharedLibrary::isLoaded() const
{
	return isLoadedImpl();
}


bool SharedLibrary::hasSymbol(const std::string& name)
{
	return findSymbolImpl(name) != 0;
}


void* SharedLibrary::getSymbol(const std::string& name)
{
	void* result = findSymbolImpl(name);
	if (result)
		return result;
	else
		throw NotFoundException(name);
}


const std::string& SharedLibrary::getPath() const
{
	return getPathImpl();
}


std::string SharedLibrary::suffix()
{
	return suffixImpl();
}


bool SharedLibrary::setSearchPath(const std::string& path)
{
	return setSearchPathImpl(path);
}


} // namespace Poco
