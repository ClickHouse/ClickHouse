//
// SharedLibrary_WIN32.cpp
//
// $Id: //poco/1.4/Foundation/src/SharedLibrary_WIN32.cpp#2 $
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


#include "Poco/SharedLibrary_WIN32.h"
#include "Poco/Path.h"
#include "Poco/UnWindows.h"


namespace Poco {


FastMutex SharedLibraryImpl::_mutex;


SharedLibraryImpl::SharedLibraryImpl()
{
	_handle = 0;
}


SharedLibraryImpl::~SharedLibraryImpl()
{
}


void SharedLibraryImpl::loadImpl(const std::string& path, int /*flags*/)
{
	FastMutex::ScopedLock lock(_mutex);

	if (_handle) throw LibraryAlreadyLoadedException(_path);
	DWORD flags(0);
	Path p(path);
	if (p.isAbsolute()) flags |= LOAD_WITH_ALTERED_SEARCH_PATH;
	_handle = LoadLibraryExA(path.c_str(), 0, flags);
	if (!_handle) throw LibraryLoadException(path);
	_path = path;
}


void SharedLibraryImpl::unloadImpl()
{
	FastMutex::ScopedLock lock(_mutex);

	if (_handle)
	{
		FreeLibrary((HMODULE) _handle);
		_handle = 0;
	}
	_path.clear();
}


bool SharedLibraryImpl::isLoadedImpl() const
{
	return _handle != 0; 
}


void* SharedLibraryImpl::findSymbolImpl(const std::string& name)
{
	FastMutex::ScopedLock lock(_mutex);

	if (_handle)
	{
		return (void*) GetProcAddress((HMODULE) _handle, name.c_str());
	}
	else return 0;
}


const std::string& SharedLibraryImpl::getPathImpl() const
{
	return _path;
}


std::string SharedLibraryImpl::suffixImpl()
{
#if defined(_DEBUG)
	return "d.dll";
#else
	return ".dll";
#endif
}


} // namespace Poco
