//
// WinRegistryConfiguration.cpp
//
// $Id: //poco/1.4/Util/src/WinRegistryConfiguration.cpp#3 $
//
// Library: Util
// Package: Windows
// Module:  WinRegistryConfiguration
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Util/WinRegistryConfiguration.h"
#include "Poco/Util/WinRegistryKey.h"
#include "Poco/NumberFormatter.h"
#include "Poco/NumberParser.h"
#include "Poco/Exception.h"


namespace Poco {
namespace Util {


WinRegistryConfiguration::WinRegistryConfiguration(const std::string& rootPath, REGSAM extraSam): _rootPath(rootPath), _extraSam(extraSam)
{
	// rootPath must end with backslash
	if (!_rootPath.empty())
	{
		if (_rootPath[_rootPath.length() - 1] != '\\')
			_rootPath += '\\';
	}
}


WinRegistryConfiguration::~WinRegistryConfiguration()
{
}


bool WinRegistryConfiguration::getRaw(const std::string& key, std::string& value) const
{
	std::string keyName;
	std::string fullPath = _rootPath + convertToRegFormat(key, keyName);
	WinRegistryKey aKey(fullPath, true, _extraSam);
	bool exists = aKey.exists(keyName);
	if (exists)
	{
		WinRegistryKey::Type type = aKey.type(keyName);

		switch (type)
		{
		case WinRegistryKey::REGT_STRING:
			value = aKey.getString(keyName);
			break;
		case WinRegistryKey::REGT_STRING_EXPAND:
			value = aKey.getStringExpand(keyName);
			break;
		case WinRegistryKey::REGT_BINARY:
			{
				std::vector<char> tmp = aKey.getBinary(keyName);
				value.assign(tmp.begin(), tmp.end());
			}
			break;
		case WinRegistryKey::REGT_DWORD:
			value = Poco::NumberFormatter::format(aKey.getInt(keyName));
			break;
#if defined(POCO_HAVE_INT64)
		case WinRegistryKey::REGT_QWORD:
			value = Poco::NumberFormatter::format(aKey.getInt64(keyName));
			break;
#endif
		default:
			exists = false;
		}
	}
	return exists;
}


void WinRegistryConfiguration::setRaw(const std::string& key, const std::string& value)
{
	std::string keyName;
	std::string fullPath = _rootPath + convertToRegFormat(key, keyName);
	WinRegistryKey aKey(fullPath, false, _extraSam);
	aKey.setString(keyName, value);
}


void WinRegistryConfiguration::enumerate(const std::string& key, Keys& range) const
{
	std::string keyName;
	std::string fullPath = _rootPath + convertToRegFormat(key, keyName);
	if (fullPath.empty())
	{
		// return all root level keys
#if defined(_WIN32_WCE)
		range.push_back("HKEY_CLASSES_ROOT");
		range.push_back("HKEY_CURRENT_USER");
		range.push_back("HKEY_LOCAL_MACHINE");
		range.push_back("HKEY_USERS");
#else
		range.push_back("HKEY_CLASSES_ROOT");
		range.push_back("HKEY_CURRENT_CONFIG");
		range.push_back("HKEY_CURRENT_USER");
		range.push_back("HKEY_LOCAL_MACHINE");
		range.push_back("HKEY_PERFORMANCE_DATA");
		range.push_back("HKEY_USERS");
#endif
	}
	else
	{
		fullPath += '\\';
		fullPath += keyName;
		WinRegistryKey aKey(fullPath, true, _extraSam);
		aKey.values(range);
		aKey.subKeys(range);
	}
}


void WinRegistryConfiguration::removeRaw(const std::string& key)
{
	throw Poco::NotImplementedException("Removing a key in a WinRegistryConfiguration");
}


std::string WinRegistryConfiguration::convertToRegFormat(const std::string& key, std::string& value) const
{
	std::size_t pos = key.rfind('.');
	if (pos == std::string::npos)
	{
		value = key;
		return std::string();
	}
	std::string prefix(key.substr(0,pos));
	value = key.substr(pos + 1);
	Poco::translateInPlace(prefix, ".", "\\");
	return prefix;
}


} } // namespace Poco::Util
