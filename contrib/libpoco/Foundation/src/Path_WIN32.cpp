//
// Path_WIN32.cpp
//
// $Id: //poco/1.4/Foundation/src/Path_WIN32.cpp#4 $
//
// Library: Foundation
// Package: Filesystem
// Module:  Path
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Path_WIN32.h"
#include "Poco/Environment_WIN32.h"
#include "Poco/UnWindows.h"


namespace Poco {


std::string PathImpl::currentImpl()
{
	char buffer[MAX_PATH];
	DWORD n = GetCurrentDirectoryA(sizeof(buffer), buffer);
	if (n > 0 && n < sizeof(buffer))
	{
		std::string result(buffer, n);
		if (result[n - 1] != '\\')
			result.append("\\");
		return result;
	}
	else throw SystemException("Cannot get current directory");
}


std::string PathImpl::systemImpl()
{
	char buffer[MAX_PATH];
	DWORD n = GetSystemDirectoryA(buffer, sizeof(buffer));
	if (n > 0 && n < sizeof(buffer))
	{
		std::string result(buffer, n);
		if (result[n - 1] != '\\')
			result.append("\\");
		return result;
	}
	else throw SystemException("Cannot get system directory");
}


std::string PathImpl::homeImpl()
{
	std::string result;
	if (EnvironmentImpl::hasImpl("USERPROFILE"))
	{
		result = EnvironmentImpl::getImpl("USERPROFILE");
	}
	else if (EnvironmentImpl::hasImpl("HOMEDRIVE") && EnvironmentImpl::hasImpl("HOMEPATH"))
	{
		result = EnvironmentImpl::getImpl("HOMEDRIVE");
		result.append(EnvironmentImpl::getImpl("HOMEPATH"));
	}
	else
	{
		result = systemImpl();
	}

	std::string::size_type n = result.size();
	if (n > 0 && result[n - 1] != '\\')
		result.append("\\");
	return result;
}


std::string PathImpl::tempImpl()
{
	char buffer[MAX_PATH];
	DWORD n = GetTempPathA(sizeof(buffer), buffer);
	if (n > 0 && n < sizeof(buffer))
	{
		n = GetLongPathNameA(buffer, buffer, static_cast<DWORD>(sizeof buffer));
		if (n <= 0) throw SystemException("Cannot get temporary directory long path name");
		std::string result(buffer, n);
		if (result[n - 1] != '\\')
			result.append("\\");
		return result;
	}
	else throw SystemException("Cannot get temporary directory");
}


std::string PathImpl::nullImpl()
{
	return "NUL:";
}


std::string PathImpl::expandImpl(const std::string& path)
{
	char buffer[MAX_PATH];
	DWORD n = ExpandEnvironmentStringsA(path.c_str(), buffer, sizeof(buffer));
	if (n > 0 && n < sizeof(buffer))
		return std::string(buffer, n - 1);
	else
		return path;
}


void PathImpl::listRootsImpl(std::vector<std::string>& roots)
{
	roots.clear();
	char buffer[128];
	DWORD n = GetLogicalDriveStrings(sizeof(buffer) - 1, buffer);
	char* it = buffer;
	char* end = buffer + (n > sizeof(buffer) ? sizeof(buffer) : n);
	while (it < end)
	{
		std::string dev;
		while (it < end && *it) dev += *it++;
		roots.push_back(dev);
		++it;
	}
}


} // namespace Poco
