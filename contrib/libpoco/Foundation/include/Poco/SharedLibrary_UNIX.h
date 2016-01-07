//
// SharedLibrary_UNIX.h
//
// $Id: //poco/1.4/Foundation/include/Poco/SharedLibrary_UNIX.h#2 $
//
// Library: Foundation
// Package: SharedLibrary
// Module:  SharedLibrary
//
// Definition of the SharedLibraryImpl class for UNIX (dlopen).
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_SharedLibrary_UNIX_INCLUDED
#define Foundation_SharedLibrary_UNIX_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Mutex.h"


namespace Poco {


class Foundation_API SharedLibraryImpl
{
protected:
	enum Flags
	{
		SHLIB_GLOBAL_IMPL = 1,
		SHLIB_LOCAL_IMPL  = 2  
	};

	SharedLibraryImpl();
	~SharedLibraryImpl();
	void loadImpl(const std::string& path, int flags);
	void unloadImpl();
	bool isLoadedImpl() const;
	void* findSymbolImpl(const std::string& name);
	const std::string& getPathImpl() const;
	static std::string suffixImpl();

private:
	std::string _path;
	void* _handle;
	static FastMutex _mutex;
};


} // namespace Poco


#endif // Foundation_SharedLibrary_UNIX_INCLUDED
