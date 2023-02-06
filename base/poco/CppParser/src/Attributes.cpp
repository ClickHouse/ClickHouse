//
// Attributes.cpp
//
// Library: CppParser
// Package: Attributes
// Module:  Attributes
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/CppParser/Attributes.h"
#include "Poco/NumberParser.h"
#include "Poco/Exception.h"
#include <algorithm>


using Poco::NumberParser;


namespace Poco {
namespace CppParser {


Attributes::Attributes()
{
}


Attributes::Attributes(const Attributes& attrs)
{
	_map = attrs._map;
}


Attributes::~Attributes()
{
}


Attributes& Attributes::operator = (const Attributes& attrs)
{
	AttrMap map(attrs._map);
	std::swap(_map, map);
	return *this;
}

	
bool Attributes::has(const std::string& name) const
{
	return _map.find(name) != _map.end();
}

	
std::string Attributes::getString(const std::string& name) const
{
	AttrMap::const_iterator it = _map.find(name);
	if (it != _map.end())
		return it->second;
	else
		throw Poco::NotFoundException(name);
}

	
std::string Attributes::getString(const std::string& name, const std::string& defaultValue) const
{
	AttrMap::const_iterator it = _map.find(name);
	if (it != _map.end())
		return it->second;
	else
		return defaultValue;
}


int Attributes::getInt(const std::string& name) const
{
	AttrMap::const_iterator it = _map.find(name);
	if (it != _map.end())
		return NumberParser::parse(it->second);
	else
		throw Poco::NotFoundException(name);
}

	
int Attributes::getInt(const std::string& name, int defaultValue) const
{
	AttrMap::const_iterator it = _map.find(name);
	if (it != _map.end())
		return NumberParser::parse(it->second);
	else
		return defaultValue;
}


bool Attributes::getBool(const std::string& name) const
{
	AttrMap::const_iterator it = _map.find(name);
	if (it != _map.end())
		return it->second != "false";
	else
		throw Poco::NotFoundException(name);
}


bool Attributes::getBool(const std::string& name, bool defaultValue) const
{
	AttrMap::const_iterator it = _map.find(name);
	if (it != _map.end())
		return it->second != "false";
	else
		return defaultValue;
}


void Attributes::set(const std::string& name, const std::string& value)
{
	_map[name] = value;
}


void Attributes::remove(const std::string& name)
{
	AttrMap::iterator it = _map.find(name);
	if (it != _map.end())
		_map.erase(it);
}


const std::string& Attributes::operator [] (const std::string& name) const
{
	AttrMap::const_iterator it = _map.find(name);
	if (it != _map.end())
		return it->second;
	else
		throw Poco::NotFoundException(name);
}


std::string& Attributes::operator [] (const std::string& name)
{
	return _map[name];
}


void Attributes::clear()
{
	_map.clear();
}


} } // namespace Poco::CppParser
