//
// Path_UNIX.cpp
//
// $Id: //poco/1.4/Foundation/src/Path_UNIX.cpp#3 $
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


#include "Poco/Path_UNIX.h"
#include "Poco/Exception.h"
#include "Poco/Environment_UNIX.h"
#include "Poco/Ascii.h"
#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#if !defined(POCO_VXWORKS)
#include <pwd.h>
#endif
#include <climits>


#ifndef PATH_MAX
#define PATH_MAX 1024 // fallback
#endif


namespace Poco {


std::string PathImpl::currentImpl()
{
	std::string path;
	char cwd[PATH_MAX];
	if (getcwd(cwd, sizeof(cwd)))
		path = cwd;
	else
		throw SystemException("cannot get current directory");
	std::string::size_type n = path.size();
	if (n > 0 && path[n - 1] != '/') path.append("/");
	return path;
}


std::string PathImpl::homeImpl()
{
#if defined(POCO_VXWORKS)
	if (EnvironmentImpl::hasImpl("HOME"))
		return EnvironmentImpl::getImpl("HOME");
	else
		return "/";
#else
	std::string path;
	struct passwd* pwd = getpwuid(getuid());
	if (pwd)
		path = pwd->pw_dir;
	else
	{
		pwd = getpwuid(geteuid());
		if (pwd)
			path = pwd->pw_dir;
		else
			path = EnvironmentImpl::getImpl("HOME");
	}
	std::string::size_type n = path.size();
	if (n > 0 && path[n - 1] != '/') path.append("/");
	return path;
#endif
}


std::string PathImpl::tempImpl()
{
	std::string path;
	char* tmp = getenv("TMPDIR");
	if (tmp)
	{
		path = tmp;
		std::string::size_type n = path.size();
		if (n > 0 && path[n - 1] != '/') path.append("/");
	}
	else
	{
		path = "/tmp/";
	}
	return path;
}


std::string PathImpl::nullImpl()
{
#if defined(POCO_VXWORKS)
	return "/null";
#else
	return "/dev/null";
#endif
}


std::string PathImpl::expandImpl(const std::string& path)
{
	std::string result;
	std::string::const_iterator it  = path.begin();
	std::string::const_iterator end = path.end();
	if (it != end && *it == '~')
	{
		++it;
		if (it != end && *it == '/')
		{
			result += homeImpl(); ++it;
		}
		else result += '~';
	}
	while (it != end)
	{
		if (*it == '$')
		{
			std::string var;
			++it;
			if (it != end && *it == '{')
			{
				++it;
				while (it != end && *it != '}') var += *it++;
				if (it != end) ++it;
			}
			else
			{
				while (it != end && (Ascii::isAlphaNumeric(*it) || *it == '_')) var += *it++;
			}
			char* val = getenv(var.c_str());
			if (val) result += val;
		}
		else result += *it++;
	}
	return result;
}


void PathImpl::listRootsImpl(std::vector<std::string>& roots)
{
	roots.clear();
	roots.push_back("/");
}


} // namespace Poco
