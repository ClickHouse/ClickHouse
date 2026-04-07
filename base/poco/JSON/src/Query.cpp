//
// Query.cpp
//
// Library: JSON
// Package: JSON
// Module:  Query
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/JSON/Query.h"
#include "Poco/StringTokenizer.h"
#include "Poco/RegularExpression.h"
#include "Poco/NumberParser.h"
#include <sstream>


using Poco::Dynamic::Var;


namespace Poco {
namespace JSON {


Query::Query(const Var& source): _source(source)
{
	if (!source.isEmpty() &&
		source.type() != typeid(Object) &&
		source.type() != typeid(Object::Ptr) &&
		source.type() != typeid(Array) &&
		source.type() != typeid(Array::Ptr))
		throw InvalidArgumentException("Only JSON Object, Array or pointers thereof allowed.");
}


Query::~Query()
{
}


Object::Ptr Query::findObject(const std::string& path) const
{
	Var result = find(path);

	if (result.type() == typeid(Object::Ptr))
		return result.extract<Object::Ptr>();
	else if (result.type() == typeid(Object))
		return new Object(result.extract<Object>());

	return 0;
}


Object& Query::findObject(const std::string& path, Object& obj) const
{
	obj.clear();

	Var result = find(path);

	if (result.type() == typeid(Object::Ptr))
		obj = *result.extract<Object::Ptr>();
	else if (result.type() == typeid(Object))
		obj = result.extract<Object>();
	
	return obj;
}


Array::Ptr Query::findArray(const std::string& path) const
{
	Var result = find(path);

	if (result.type() == typeid(Array::Ptr))
		return result.extract<Array::Ptr>();
	else if (result.type() == typeid(Array))
		return new Array(result.extract<Array>());

	return 0;
}


Array& Query::findArray(const std::string& path, Array& arr) const
{
	arr.clear();

	Var result = find(path);

	if (result.type() == typeid(Array::Ptr))
		arr = *result.extract<Array::Ptr>();
	else if (result.type() == typeid(Array))
		arr = result.extract<Array>();
	
	return arr;
}


Var Query::find(const std::string& path) const
{
	Var result = _source;
	StringTokenizer tokenizer(path, ".");
	for (StringTokenizer::Iterator token = tokenizer.begin(); token != tokenizer.end(); token++)
	{
		if (!result.isEmpty())
		{
			std::vector<int> indexes;
			RegularExpression::MatchVec matches;
			int firstOffset = -1;
			int offset = 0;
			RegularExpression regex("\\[([0-9]+)\\]");
			while (regex.match(*token, offset, matches) > 0)
			{
				if (firstOffset == -1)
				{
					firstOffset = static_cast<int>(matches[0].offset);
				}
				std::string num = token->substr(matches[1].offset, matches[1].length);
				indexes.push_back(NumberParser::parse(num));
				offset = static_cast<int>(matches[0].offset + matches[0].length);
			}

			std::string name(*token);
			if (firstOffset != -1)
			{
				name = name.substr(0, firstOffset);
			}

			if (name.length() > 0)
			{
				if (result.type() == typeid(Object::Ptr))
				{
					Object::Ptr o = result.extract<Object::Ptr>();
					result = o->get(name);
				}
				else if (result.type() == typeid(Object))
				{
					Object o = result.extract<Object>();
					result = o.get(name);
				}
				else
					result.empty();

			}

			if (!result.isEmpty() && !indexes.empty())
			{
				for (std::vector<int>::iterator it = indexes.begin(); it != indexes.end(); ++it)
				{
					if (result.type() == typeid(Array::Ptr))
					{
						Array::Ptr array = result.extract<Array::Ptr>();
						result = array->get(*it);
						if (result.isEmpty()) break;
					}
					else if (result.type() == typeid(Array))
					{
						Array array = result.extract<Array>();
						result = array.get(*it);
						if (result.isEmpty()) break;
					}
				}
			}
		}
	}
	return result;
}


} } // namespace Poco::JSON
