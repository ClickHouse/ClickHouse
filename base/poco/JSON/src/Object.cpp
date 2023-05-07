//
// Object.cpp
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


Object::Object(int options):
	_preserveInsOrder((options & Poco::JSON_PRESERVE_KEY_ORDER) != 0),
	_escapeUnicode((options & Poco::JSON_ESCAPE_UNICODE) != 0),
	_modified(false)
{
}


Object::Object(const Object& other) : _values(other._values),
	_preserveInsOrder(other._preserveInsOrder),
	_escapeUnicode(other._escapeUnicode),
	_pStruct(!other._modified ? other._pStruct : 0),
	_modified(other._modified)
{
	syncKeys(other._keys);
}


#ifdef POCO_ENABLE_CPP11


Object::Object(Object&& other) :
	_values(std::move(other._values)),
	_keys(std::move(other._keys)),
	_preserveInsOrder(other._preserveInsOrder),
	_escapeUnicode(other._escapeUnicode),
	_pStruct(!other._modified ? other._pStruct : 0),
	_modified(other._modified)
{
	other.clear();
}


Object &Object::operator= (Object &&other)
{
	if (&other != this)
	{
		_values = other._values;
		_preserveInsOrder = other._preserveInsOrder;
		syncKeys(other._keys);
		_escapeUnicode = other._escapeUnicode;
		_pStruct = !other._modified ? other._pStruct : 0;
		_modified = other._modified;
		other.clear();
	}
	return *this;
}


#endif // POCO_ENABLE_CPP11


Object::~Object()
{
}


Object &Object::operator= (const Object &other)
{
	if (&other != this)
	{
		_values = other._values;
		_keys = other._keys;
		_preserveInsOrder = other._preserveInsOrder;
		_escapeUnicode = other._escapeUnicode;
		_pStruct = !other._modified ? other._pStruct : 0;
		_modified = other._modified;
	}
	return *this;
}


void Object::syncKeys(const KeyList& keys)
{
	if(_preserveInsOrder)
	{
		// update iterators in _keys to point to copied _values
		for(KeyList::const_iterator it = keys.begin(); it != keys.end(); ++it)
		{
			ValueMap::const_iterator itv = _values.find((*it)->first);
			poco_assert (itv != _values.end());
			_keys.push_back(itv);
		}
	}
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


void Object::getNames(NameList& names) const
{
	names.clear();
	if (_preserveInsOrder)
	{
		for(KeyList::const_iterator it = _keys.begin(); it != _keys.end(); ++it)
		{
			names.push_back((*it)->first);
		}
	}
	else
	{
		for(ValueMap::const_iterator it = _values.begin(); it != _values.end(); ++it)
		{
			names.push_back(it->first);
		}
	}
}


Object::NameList Object::getNames() const
{
	NameList names;
	getNames(names);
	return names;
}


void Object::stringify(std::ostream& out, unsigned int indent, int step) const
{
	if (step < 0) step = indent;

	if (!_preserveInsOrder)
		doStringify(_values, out, indent, step);
	else
		doStringify(_keys, out, indent, step);
}


const std::string& Object::getKey(KeyList::const_iterator& iter) const
{
	ValueMap::const_iterator it = _values.begin();
	ValueMap::const_iterator end = _values.end();
	for (; it != end; ++it)
	{
		if (it == *iter) return it->first;
	}

	throw NotFoundException((*iter)->first);
}


void Object::set(const std::string& key, const Dynamic::Var& value)
{
	std::pair<ValueMap::iterator, bool> ret = _values.insert(ValueMap::value_type(key, value));
	if (!ret.second) ret.first->second = value;
	if (_preserveInsOrder)
	{
		KeyList::iterator it = _keys.begin();
		KeyList::iterator end = _keys.end();
		for (; it != end; ++it)
		{
			if (key == (*it)->first) return;
		}
		_keys.push_back(ret.first);
	}
	_modified = true;
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


void Object::resetDynStruct() const
{
	if (!_pStruct)
		_pStruct = new Poco::DynamicStruct;
	else
		_pStruct->clear();
}


Object::operator const Poco::DynamicStruct& () const
{
	if (!_values.size())
	{
		resetDynStruct();
	}
	else if (_modified)
	{
		ValueMap::const_iterator it = _values.begin();
		ValueMap::const_iterator end = _values.end();
		resetDynStruct();
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
	_modified = true;
}


} } // namespace Poco::JSON
