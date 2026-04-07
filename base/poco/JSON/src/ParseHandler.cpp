//
// ParseHandler.cpp
//
// Library: JSON
// Package: JSON
// Module:  ParseHandler
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/JSON/ParseHandler.h"
#include "Poco/JSON/Object.h"
#include "Poco/JSON/JSONException.h"


using Poco::Dynamic::Var;


namespace Poco {
namespace JSON {


ParseHandler::ParseHandler(bool preserveObjectOrder) : Handler(),
	_preserveObjectOrder(preserveObjectOrder)
{
}


ParseHandler::~ParseHandler()
{
}


void ParseHandler::reset()
{
	while (!_stack.empty()) _stack.pop();
	_key = "";
	_result.empty();
}


void ParseHandler::startObject()
{
	Object::Ptr newObj = new Object(_preserveObjectOrder);
	if (_stack.empty()) // The first object
	{
		_result = newObj;
	}
	else
	{
		Var parent = _stack.top();

		if (parent.type() == typeid(Array::Ptr))
		{
			Array::Ptr arr = parent.extract<Array::Ptr>();
			arr->add(newObj);
		}
		else if (parent.type() == typeid(Object::Ptr))
		{
			poco_assert_dbg(!_key.empty());
			Object::Ptr obj = parent.extract<Object::Ptr>();
			obj->set(_key, newObj);
			_key.clear();
		}
	}

	_stack.push(newObj);
}


void ParseHandler::endObject()
{
	if (!_stack.empty()) _stack.pop();
}


void ParseHandler::startArray()
{
	Array::Ptr newArr = new Array();

	if (_stack.empty()) // The first array
	{
		_result = newArr;
	}
	else
	{
		Var parent = _stack.top();

		if (parent.type() == typeid(Array::Ptr))
		{
			Array::Ptr arr = parent.extract<Array::Ptr>();
			arr->add(newArr);
		}
		else if (parent.type() == typeid(Object::Ptr))
		{
			poco_assert_dbg(!_key.empty());
			Object::Ptr obj = parent.extract<Object::Ptr>();
			obj->set(_key, newArr);
			_key.clear();
		}
	}

	_stack.push(newArr);
}


void ParseHandler::endArray()
{
	if (!_stack.empty()) _stack.pop();
}


void ParseHandler::key(const std::string& k)
{
	_key = k;
}


void ParseHandler::setValue(const Var& value)
{
	if (_stack.size())
	{
		Var parent = _stack.top();

		if (parent.type() == typeid(Array::Ptr))
		{
			Array::Ptr arr = parent.extract<Array::Ptr>();
			arr->add(value);
		}
		else if (parent.type() == typeid(Object::Ptr))
		{
			Object::Ptr obj = parent.extract<Object::Ptr>();
			obj->set(_key, value);
			_key.clear();
		}
	}
	else
	{
		throw JSONException("Attempt to set value on an empty stack");
	}
}


} } // namespace Poco::JSON
