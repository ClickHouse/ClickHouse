//
// DirectoryIterator_UNIX.cpp
//
// $Id: //poco/1.4/Foundation/src/DirectoryIterator_UNIX.cpp#2 $
//
// Library: Foundation
// Package: Filesystem
// Module:  DirectoryIterator
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/DirectoryIterator_UNIX.h"
#if defined(POCO_VXWORKS)
#include "Poco/File_VX.h"
#else
#include "Poco/File_UNIX.h"
#endif
#include "Poco/Path.h"


namespace Poco {


DirectoryIteratorImpl::DirectoryIteratorImpl(const std::string& path): _pDir(0), _rc(1)
{
	Path p(path);
	p.makeFile();

#if defined(POCO_VXWORKS)
	_pDir = opendir(const_cast<char*>(p.toString().c_str()));
#else
	_pDir = opendir(p.toString().c_str());
#endif
	if (!_pDir) File::handleLastError(path);

	next();
}


DirectoryIteratorImpl::~DirectoryIteratorImpl()
{
	if (_pDir) closedir(_pDir);
}


const std::string& DirectoryIteratorImpl::next()
{
	do
	{
		struct dirent* pEntry = readdir(_pDir);
		if (pEntry)
			_current = pEntry->d_name;
		else
			_current.clear();
	}
	while (_current == "." || _current == "..");
	return _current;
}


} // namespace Poco
