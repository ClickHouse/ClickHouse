//
// Object.cpp
//
// $Id$
//
// Library: JSON
// Package: JSON
// Module:  Object
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/JSON/Object.h"
#include <iostream>
#include <sstream>


using Poco::Dynamic::Var;


namespace Poco {
namespace JSON {


Object::Object(bool preserveInsertionOrder): _preserveInsOrder(preserveInsertionOrder)
{
}


Object::Object(const Object& copy) : _values(copy._values),
	_keys(copy._keys),
	_preserveInsOrder(copy._preserveInsOrder),
	_pStruct(0)
{
}


Object::~Object()
{
}


Var Object::get(const std::string& key) const
{
	ValueMap::const_iterator it = _values.find(key);
	if (it != _values.end())
	{
		return it->second;
	}

	return Var();
}


Array::Ptr Object::getArray(const std::string& key) const
{
	ValueMap::const_iterator it = _values.find(key);
	if ((it != _values.end()) && (it->second.type() == typeid(Array::Ptr)))
	{
		return it->second.extract<Array::Ptr>();
	}

	return 0;
}


Object::Ptr Object::getObject(const std::string& key) const
{
	ValueMap::const_iterator it = _values.find(key);
	if ((it != _values.end()) && (it->second.type() == typeid(Object::Ptr)))
	{
		return it->second.extract<Object::Ptr>();
	}

	return 0;
}


void Object::getNames(std::vector<std::string>& names) const
{
	names.clear();
	for(ValueMap::const_iterator it = _values.begin(); it != _values.end(); ++it)
	{
		names.push_back(it->first);
	}
}


void Object::stringify(std::ostream& out, unsigned int indent, int step) const
{
	if (step < 0) step = indent;

	if(!_preserveInsOrder)
		doStringify(_values, out, indent, step);
	else
		doStringify(_keys, out, indent, step);
}


const std::string& Object::getKey(KeyPtrList::const_iterator& iter) const
{
	ValueMap::const_iterator it = _values.begin();
	ValueMap::const_iterator end = _values.end();
	for (; it != end; ++it)
	{
		if (it->first == **iter) return it->first;
	}

	throw NotFoundException(**iter);
}


void Object::set(const std::string& key, const Dynamic::Var& value)
{
	std::pair<ValueMap::iterator, bool> ret = _values.insert(ValueMap::value_type(key, value));
	if (_preserveInsOrder)
	{
		KeyPtrList::iterator it = _keys.begin();
		KeyPtrList::iterator end = _keys.end();
		for (; it != end; ++it)
		{
			if (key == **it) return;
		}
		_keys.push_back(&ret.first->first);
	}
}


Poco::DynamicStruct Object::makeStruct(const Object::Ptr& obj)
{
	Poco::DynamicStruct ds;

	ConstIterator it  = obj->begin();
	ConstIterator end = obj->end();
	for (; it != end; ++it)
	{
		if (obj->isObject(it))
		{
			Object::Ptr pObj = obj->getObject(it->first);
			DynamicStruct str = makeStruct(pObj);
			ds.insert(it->first, str);
		}
		else if (obj->isArray(it))
		{
			Array::Ptr pArr = obj->getArray(it->first);
			std::vector<Poco::Dynamic::Var> v = Poco::JSON::Array::makeArray(pArr);
			ds.insert(it->first, v);
		}
		else
			ds.insert(it->first, it->second);
	}

	return ds;
}


Object::operator const Poco::DynamicStruct& () const
{
	if (!_pStruct)
	{
		ValueMap::const_iterator it = _values.begin();
		ValueMap::const_iterator end = _values.end();
		_pStruct = new Poco::DynamicStruct;
		for (; it != end; ++it)
		{
			if (isObject(it))
			{
				_pStruct->insert(it->first, makeStruct(getObject(it->first)));
			}
			else if (isArray(it))
			{
				_pStruct->insert(it->first, Poco::JSON::Array::makeArray(getArray(it->first)));
			}
			else
			{
				_pStruct->insert(it->first, it->second);
			}
		}
	}

	return *_pStruct;
}


void Object::clear()
{
	_values.clear();
	_keys.clear();
	_pStruct = 0;
}


} } // namespace Poco::JSON
