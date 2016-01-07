//
// NameValueCollection.cpp
//
// $Id: //poco/1.4/Net/src/NameValueCollection.cpp#1 $
//
// Library: Net
// Package: Messages
// Module:  NameValueCollection
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/NameValueCollection.h"
#include "Poco/Exception.h"
#include <algorithm>


using Poco::NotFoundException;


namespace Poco {
namespace Net {


NameValueCollection::NameValueCollection()
{
}


NameValueCollection::NameValueCollection(const NameValueCollection& nvc):
	_map(nvc._map)
{
}


NameValueCollection::~NameValueCollection()
{
}


NameValueCollection& NameValueCollection::operator = (const NameValueCollection& nvc)
{
	if (&nvc != this)
	{
		_map = nvc._map;
	}
	return *this;
}


void NameValueCollection::swap(NameValueCollection& nvc)
{
	std::swap(_map, nvc._map);
}

	
const std::string& NameValueCollection::operator [] (const std::string& name) const
{
	ConstIterator it = _map.find(name);
	if (it != _map.end())
		return it->second;
	else
		throw NotFoundException(name);
}

	
void NameValueCollection::set(const std::string& name, const std::string& value)	
{
	Iterator it = _map.find(name);
	if (it != _map.end())
		it->second = value;
	else
		_map.insert(HeaderMap::ValueType(name, value));
}

	
void NameValueCollection::add(const std::string& name, const std::string& value)
{
	_map.insert(HeaderMap::ValueType(name, value));
}

	
const std::string& NameValueCollection::get(const std::string& name) const
{
	ConstIterator it = _map.find(name);
	if (it != _map.end())
		return it->second;
	else
		throw NotFoundException(name);
}


const std::string& NameValueCollection::get(const std::string& name, const std::string& defaultValue) const
{
	ConstIterator it = _map.find(name);
	if (it != _map.end())
		return it->second;
	else
		return defaultValue;
}


bool NameValueCollection::has(const std::string& name) const
{
	return _map.find(name) != _map.end();
}


NameValueCollection::ConstIterator NameValueCollection::find(const std::string& name) const
{
	return _map.find(name);
}

	
NameValueCollection::ConstIterator NameValueCollection::begin() const
{
	return _map.begin();
}

	
NameValueCollection::ConstIterator NameValueCollection::end() const
{
	return _map.end();
}

	
bool NameValueCollection::empty() const
{
	return _map.empty();
}


std::size_t NameValueCollection::size() const
{
	return _map.size();
}


void NameValueCollection::erase(const std::string& name)
{
	_map.erase(name);
}


void NameValueCollection::clear()
{
	_map.clear();
}


} } // namespace Poco::Net
