//
// IniFileConfiguration.cpp
//
// Library: Util
// Package: Configuration
// Module:  IniFileConfiguration
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Util/IniFileConfiguration.h"


#ifndef POCO_UTIL_NO_INIFILECONFIGURATION


#include "Poco/Exception.h"
#include "Poco/String.h"
#include "Poco/Path.h"
#include "Poco/FileStream.h"
#include "Poco/Ascii.h"
#include <set>


using Poco::icompare;
using Poco::trim;
using Poco::Path;


namespace Poco {
namespace Util {


IniFileConfiguration::IniFileConfiguration()
{
}


IniFileConfiguration::IniFileConfiguration(std::istream& istr)
{
	load(istr);
}

	
IniFileConfiguration::IniFileConfiguration(const std::string& path)
{
	load(path);
}


IniFileConfiguration::~IniFileConfiguration()
{
}


void IniFileConfiguration::load(std::istream& istr)
{
	_map.clear();
	_sectionKey.clear();
	while (!istr.eof())
	{
		parseLine(istr);
	}
}
	

void IniFileConfiguration::load(const std::string& path)
{
	Poco::FileInputStream istr(path);
	if (istr.good())
		load(istr);
	else
		throw Poco::OpenFileException(path);
}


bool IniFileConfiguration::getRaw(const std::string& key, std::string& value) const
{
	IStringMap::const_iterator it = _map.find(key);
	if (it != _map.end())
	{
		value = it->second;
		return true;
	}
	else return false;
}


void IniFileConfiguration::setRaw(const std::string& key, const std::string& value)
{
	_map[key] = value;
}


void IniFileConfiguration::enumerate(const std::string& key, Keys& range) const
{
	std::set<std::string> keys;
	std::string prefix = key;
	if (!prefix.empty()) prefix += '.';
	std::string::size_type psize = prefix.size();
	for (IStringMap::const_iterator it = _map.begin(); it != _map.end(); ++it)
	{
		if (icompare(it->first, psize, prefix) == 0)
		{
			std::string subKey;
			std::string::size_type end = it->first.find('.', psize);
			if (end == std::string::npos)
				subKey = it->first.substr(psize);
			else
				subKey = it->first.substr(psize, end - psize);
			if (keys.find(subKey) == keys.end())
			{
				range.push_back(subKey);
				keys.insert(subKey);
			}
		}
	}
}


void IniFileConfiguration::removeRaw(const std::string& key)
{
	std::string prefix = key;
	if (!prefix.empty()) prefix += '.';
	std::string::size_type psize = prefix.size();
	IStringMap::iterator it = _map.begin();
	IStringMap::iterator itCur;
	while (it != _map.end())
	{
		itCur = it++;
		if ((icompare(itCur->first, key) == 0) || (icompare(itCur->first, psize, prefix) == 0))
		{
			_map.erase(itCur);
		}
	}
}


bool IniFileConfiguration::ICompare::operator () (const std::string& s1, const std::string& s2) const
{
	return icompare(s1, s2) < 0;
}


void IniFileConfiguration::parseLine(std::istream& istr)
{
	static const int eof = std::char_traits<char>::eof(); 

	int c = istr.get();
	while (c != eof && Poco::Ascii::isSpace(c)) c = istr.get();
	if (c != eof)
	{
		if (c == ';')
		{
			while (c != eof && c != '\n') c = istr.get();
		}
		else if (c == '[')
		{
			std::string key;
			c = istr.get();
			while (c != eof && c != ']' && c != '\n') { key += (char) c; c = istr.get(); }
			_sectionKey = trim(key);
		}
		else
		{
			std::string key;
			while (c != eof && c != '=' && c != '\n') { key += (char) c; c = istr.get(); }
			std::string value;
			if (c == '=')
			{
				c = istr.get();
				while (c != eof && c != '\n') { value += (char) c; c = istr.get(); }
			}
			std::string fullKey = _sectionKey;
			if (!fullKey.empty()) fullKey += '.';
			fullKey.append(trim(key));
			_map[fullKey] = trim(value);
		}
	}
}


} } // namespace Poco::Util


#endif // POCO_UTIL_NO_INIFILECONFIGURATION