//
// Path_UNIX.cpp
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
#include <pwd.h>
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
	std::string path;
	size_t buf_size = 1024;     // Same as glibc use for getpwuid
	std::vector<char> buf(buf_size);
	struct passwd res;
	struct passwd* pwd = nullptr;

	getpwuid_r(getuid(), &res, buf.data(), buf_size, &pwd);
	if (pwd)
		path = pwd->pw_dir;
	else
	{
		getpwuid_r(getuid(), &res, buf.data(), buf_size, &pwd);
		if (pwd)
			path = pwd->pw_dir;
		else
			path = EnvironmentImpl::getImpl("HOME");
	}
	std::string::size_type n = path.size();
	if (n > 0 && path[n - 1] != '/') path.append("/");
	return path;
}


std::string PathImpl::configHomeImpl()
{
	std::string path = PathImpl::homeImpl();
	std::string::size_type n = path.size();
	if (n > 0 && path[n - 1] == '/')
#if POCO_OS == POCO_OS_MAC_OS_X
	  path.append("Library/Preferences/");
#else
	  path.append(".config/");
#endif

	return path;
}


std::string PathImpl::dataHomeImpl()
{
	std::string path = PathImpl::homeImpl();
	std::string::size_type n = path.size();
	if (n > 0 && path[n - 1] == '/')
#if POCO_OS == POCO_OS_MAC_OS_X
	  path.append("Library/Application Support/");
#else
	  path.append(".local/share/");
#endif

	return path;
}


std::string PathImpl::cacheHomeImpl()
{
	std::string path = PathImpl::homeImpl();
	std::string::size_type n = path.size();
	if (n > 0 && path[n - 1] == '/')
#if POCO_OS == POCO_OS_MAC_OS_X
	  path.append("Library/Caches/");
#else
	  path.append(".cache/");
#endif

	return path;
}


std::string PathImpl::tempHomeImpl()
{
	std::string path = PathImpl::homeImpl();
	std::string::size_type n = path.size();
	if (n > 0 && path[n - 1] == '/')
#if POCO_OS == POCO_OS_MAC_OS_X
	  path.append("Library/Caches/");
#else
	  path.append(".local/tmp/");
#endif

	return path;
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


std::string PathImpl::configImpl()
{
	std::string path;

#if POCO_OS == POCO_OS_MAC_OS_X
	  path = "/Library/Preferences/";
#else
	  path = "/etc/";
#endif
	return path;
}


std::string PathImpl::nullImpl()
{
	return "/dev/null";
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
			const char* homeEnv = getenv("HOME");
			if (homeEnv)
			{
				result += homeEnv;
				std::string::size_type resultSize = result.size();
				if (resultSize > 0 && result[resultSize - 1] != '/')
					result.append("/");
			}
			else
			{
				result += homeImpl();
			}
			++it;
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
