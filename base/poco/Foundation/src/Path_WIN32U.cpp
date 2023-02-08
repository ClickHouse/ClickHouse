//
// Path_WIN32U.cpp
//
// Library: Foundation
// Package: Filesystem
// Module:  Path
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Path_WIN32U.h"
#include "Poco/Environment_WIN32.h"
#include "Poco/UnicodeConverter.h"
#include "Poco/Buffer.h"
#include "Poco/Exception.h"
#include "Poco/UnWindows.h"


namespace Poco {


std::string PathImpl::currentImpl()
{
	std::string result;
	DWORD len = GetCurrentDirectoryW(0, NULL);
	if (len > 0)
	{
		Buffer<wchar_t> buffer(len);
		DWORD n = GetCurrentDirectoryW(len, buffer.begin());
		if (n > 0 && n <= len)
		{
			UnicodeConverter::toUTF8(buffer.begin(), result);
			if (result[result.size() - 1] != '\\')
				result.append("\\");
			return result;
		}
	}
	throw SystemException("Cannot get current directory");
}


std::string PathImpl::systemImpl()
{
	Buffer<wchar_t> buffer(MAX_PATH_LEN);
	DWORD n = GetSystemDirectoryW(buffer.begin(), static_cast<DWORD>(buffer.size()));
	if (n > 0)
	{
		n = GetLongPathNameW(buffer.begin(), buffer.begin(), static_cast<DWORD>(buffer.size()));
		if (n <= 0) throw SystemException("Cannot get system directory long path name");
		std::string result;
		UnicodeConverter::toUTF8(buffer.begin(), result);
		if (result[result.size() - 1] != '\\') result.append("\\");
		return result;
	}
	throw SystemException("Cannot get temporary directory path");
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


std::string PathImpl::configHomeImpl()
{
	std::string result;

	// if APPDATA environment variable no exist, return home directory instead
	try
	{
		result = EnvironmentImpl::getImpl("APPDATA");
	}
	catch (NotFoundException&)
	{
		result = homeImpl();
	}

	std::string::size_type n = result.size();
	if (n > 0 && result[n - 1] != '\\')
		result.append("\\");
	return result;
}


std::string PathImpl::dataHomeImpl()
{
	std::string result;

	// if LOCALAPPDATA environment variable no exist, return config home instead
	try
	{
		result = EnvironmentImpl::getImpl("LOCALAPPDATA");
	}
	catch (NotFoundException&)
	{
		result = configHomeImpl();
	}

	std::string::size_type n = result.size();
	if (n > 0 && result[n - 1] != '\\')
		result.append("\\");
	return result;
}


std::string PathImpl::cacheHomeImpl()
{
	return tempImpl();
}


std::string PathImpl::tempHomeImpl()
{
	return tempImpl();
}


std::string PathImpl::tempImpl()
{
	Buffer<wchar_t> buffer(MAX_PATH_LEN);
	DWORD n = GetTempPathW(static_cast<DWORD>(buffer.size()), buffer.begin());
	if (n > 0)
	{
		n = GetLongPathNameW(buffer.begin(), buffer.begin(), static_cast<DWORD>(buffer.size()));
		if (n <= 0) throw SystemException("Cannot get temporary directory long path name");
		std::string result;
		UnicodeConverter::toUTF8(buffer.begin(), result);
		if (result[result.size() - 1] != '\\')
			result.append("\\");
		return result;
	}
	throw SystemException("Cannot get temporary directory path");
}


std::string PathImpl::configImpl()
{
	std::string result;

	// if PROGRAMDATA environment variable not exist, return system directory instead
	try
	{
		result = EnvironmentImpl::getImpl("PROGRAMDATA");
	}
	catch (NotFoundException&)
	{
		result = systemImpl();
	}

	std::string::size_type n = result.size();
	if (n > 0 && result[n - 1] != '\\')
		result.append("\\");
	return result;
}


std::string PathImpl::nullImpl()
{
	return "NUL:";
}


std::string PathImpl::expandImpl(const std::string& path)
{
	std::wstring upath;
	UnicodeConverter::toUTF16(path, upath);
	Buffer<wchar_t> buffer(MAX_PATH_LEN);
	DWORD n = ExpandEnvironmentStringsW(upath.c_str(), buffer.begin(), static_cast<DWORD>(buffer.size()));
	if (n > 0 && n < buffer.size() - 1)
	{
		buffer[n + 1] = 0;
		std::string result;
		UnicodeConverter::toUTF8(buffer.begin(), result);
		return result;
	}
	else return path;
}


void PathImpl::listRootsImpl(std::vector<std::string>& roots)
{
	roots.clear();
	wchar_t buffer[128];
	DWORD n = GetLogicalDriveStringsW(sizeof(buffer)/sizeof(wchar_t) - 1, buffer);
	wchar_t* it  = buffer;
	wchar_t* end = buffer + (n > sizeof(buffer) ? sizeof(buffer) : n);
	while (it < end)
	{
		std::wstring udev;
		while (it < end && *it) udev += *it++;
		std::string dev;
		UnicodeConverter::toUTF8(udev, dev);
		roots.push_back(dev);
		++it;
	}
}


} // namespace Poco
