//
// SharedLibrary_VMS.h
//
// $Id: //poco/1.4/Foundation/include/Poco/SharedLibrary_VMS.h#2 $
//
// Library: Foundation
// Package: SharedLibrary
// Module:  SharedLibrary
//
// Definition of the SharedLibraryImpl class for VMS (dlopen).
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_SharedLibrary_VMS_INCLUDED
#define Foundation_SharedLibrary_VMS_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Mutex.h"


namespace Poco {


class Foundation_API SharedLibraryImpl
{
protected:
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
	static FastMutex _mutex;
};


} // namespace Poco


#endif // Foundation_SharedLibrary_VMS_INCLUDED
