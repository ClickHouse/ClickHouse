//
// Array.cpp
//
// $Id$
//
// Library: JSON
// Package: JSON
// Module:  Array
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/JSON/Array.h"
#include "Poco/JSON/Object.h"
#include "Poco/JSON/Stringifier.h"


using Poco::Dynamic::Var;


namespace Poco {
namespace JSON {


Array::Array()
{
}


Array::Array(const Array& copy) : _values(copy._values)
{
}


Array::~Array()
{
}


Var Array::get(unsigned int index) const
{
	Var value;
	try
	{
		value = _values.at(index);
	}
	catch(std::out_of_range&)
	{
		//Ignore, we return an empty value
	}
	return value;
}


Array::Ptr Array::getArray(unsigned int index) const
{
	Array::Ptr result;

	Var value = get(index);
	if ( value.type() == typeid(Array::Ptr) )
	{
		result = value.extract<Array::Ptr>();
	}
	return result;
}


Object::Ptr Array::getObject(unsigned int index) const
{
	Object::Ptr result;

	Var value = get(index);
	if ( value.type() == typeid(Object::Ptr) )
	{
		result = value.extract<Object::Ptr>();
	}
	return result;
}


bool Array::isNull(unsigned int index) const
{
	if ( index < _values.size() )
	{
		Dynamic::Var value = _values[index];
		return value.isEmpty();
	}
	return true;
}


bool Array::isObject(unsigned int index) const
{
	Var value = get(index);
	return isObject(value);
}


bool Array::isObject(const Dynamic::Var& value) const
{
	return value.type() == typeid(Object::Ptr);
}


bool Array::isObject(ConstIterator& it) const
{
	return it!= end() && isObject(*it);
}


void Array::stringify(std::ostream& out, unsigned int indent, int step) const
{
	if (step == -1) step = indent;

	out << "[";

	if (indent > 0) out << std::endl;

	for (ValueVec::const_iterator it = _values.begin(); it != _values.end();)
	{
		for(int i = 0; i < indent; i++) out << ' ';

		Stringifier::stringify(*it, out, indent + step, step);

		if ( ++it != _values.end() )
		{
			out << ",";
			if (step > 0) out << '\n';
		}
	}

	if (step > 0) out << '\n';

	if (indent >= step) indent -= step;

	for (int i = 0; i < indent; i++)
		out << ' ';

	out << "]";
}


Array::operator const Poco::Dynamic::Array& () const
{
	if (!_pArray)
	{
		ValueVec::const_iterator it = _values.begin();
		ValueVec::const_iterator end = _values.end();
		_pArray = new Poco::Dynamic::Array;
		int index = 0;
		for (; it != end; ++it, ++index)
		{
			if (isObject(it))
			{
				_pArray->insert(_pArray->end(), Poco::JSON::Object::makeStruct(getObject(index)));
			}
			else if (isArray(it))
			{
				_pArray->insert(_pArray->end(), makeArray(getArray(index)));
			}
			else
			{
				_pArray->insert(_pArray->end(), *it);
			}
		}
	}

	return *_pArray;
}


Poco::Dynamic::Array Array::makeArray(const JSON::Array::Ptr& arr)
{
	Poco::Dynamic::Array vec;

	JSON::Array::ConstIterator it  = arr->begin();
	JSON::Array::ConstIterator end = arr->end();
	int index = 0;
	for (; it != end; ++it, ++index)
	{
		if (arr->isObject(it))
		{
			Object::Ptr pObj = arr->getObject(index);
			DynamicStruct str = Poco::JSON::Object::makeStruct(pObj);
			vec.insert(vec.end(), str);
		}
		else if (arr->isArray(it))
		{
			Array::Ptr pArr = arr->getArray(index);
			std::vector<Poco::Dynamic::Var> v = makeArray(pArr);
			vec.insert(vec.end(), v);
		}
		else
			vec.insert(vec.end(), *it);
	}

	return vec;
}


void Array::clear()
{
	_values.clear();
	_pArray = 0;
}


} } // namespace Poco::JSON
