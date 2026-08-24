//
// JSONConfiguration.cpp
//
// Library: Util
// Package: JSON
// Module:  JSONConfiguration
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//



#include "Poco/Util/JSONConfiguration.h"


#ifndef POCO_UTIL_NO_JSONCONFIGURATION


#include "Poco/FileStream.h"
#include "Poco/StringTokenizer.h"
#include "Poco/JSON/Parser.h"
#include "Poco/JSON/Query.h"
#include "Poco/RegularExpression.h"
#include "Poco/NumberParser.h"


namespace Poco {
namespace Util {


JSONConfiguration::JSONConfiguration() : _object(new JSON::Object())
{
}


JSONConfiguration::JSONConfiguration(const std::string& path)
{
	load(path);
}


JSONConfiguration::JSONConfiguration(std::istream& istr)
{
	load(istr);
}


JSONConfiguration::JSONConfiguration(const JSON::Object::Ptr& object) : _object(object)
{
}


JSONConfiguration::~JSONConfiguration()
{
}


void JSONConfiguration::load(const std::string& path)
{
	Poco::FileInputStream fis(path);
	load(fis);
}


void JSONConfiguration::load(std::istream& istr)
{
	JSON::Parser parser;
	parser.parse(istr);
	DynamicAny result = parser.result();
	if ( result.type() == typeid(JSON::Object::Ptr) )
	{
		_object = result.extract<JSON::Object::Ptr>();
	}
}


void JSONConfiguration::loadEmpty(const std::string& root)
{
	_object = new JSON::Object();
	JSON::Object::Ptr rootObject = new JSON::Object();
	_object->set(root, rootObject);
}


bool JSONConfiguration::getRaw(const std::string & key, std::string & value) const
{
	JSON::Query query(_object);
	Poco::DynamicAny result = query.find(key);
	if ( ! result.isEmpty() )
	{
		value = result.convert<std::string>();
		return true;
	}
	return false;
}


void JSONConfiguration::getIndexes(std::string& name, std::vector<int>& indexes)
{
	indexes.clear();

	RegularExpression::MatchVec matches;
	int firstOffset = -1;
	int offset = 0;
	RegularExpression regex("\\[([0-9]+)\\]");
	while(regex.match(name, offset, matches) > 0 )
	{
		if ( firstOffset == -1 )
		{
			firstOffset = static_cast<int>(matches[0].offset);
		}
		std::string num = name.substr(matches[1].offset, matches[1].length);
		indexes.push_back(NumberParser::parse(num));
		offset = static_cast<int>(matches[0].offset + matches[0].length);
	}

	if ( firstOffset != -1 )
	{
		name = name.substr(0, firstOffset);
	}
}


JSON::Object::Ptr JSONConfiguration::findStart(const std::string& key, std::string& lastPart)
{
	JSON::Object::Ptr currentObject = _object;

	StringTokenizer tokenizer(key, ".");
	lastPart = tokenizer[tokenizer.count() - 1];

	for(int i = 0; i < tokenizer.count() - 1; ++i)
	{
		std::vector<int> indexes;
		std::string name = tokenizer[i];
		getIndexes(name, indexes);

		DynamicAny result = currentObject->get(name);

		if ( result.isEmpty() ) // Not found
		{
			if ( indexes.empty() ) // We want an object, create it
			{
				JSON::Object::Ptr newObject = new JSON::Object();
				currentObject->set(name, newObject);
				currentObject = newObject;
			}
			else // We need an array
			{
				JSON::Array::Ptr newArray;
				JSON::Array::Ptr parentArray;
				JSON::Array::Ptr topArray;
				for(std::vector<int>::iterator it = indexes.begin(); it != indexes.end(); ++it)
				{
					newArray = new JSON::Array();
					if ( topArray.isNull() )
					{
						topArray = newArray;
					}

					if ( ! parentArray.isNull() )
					{
						parentArray->add(newArray);
					}

					for(int i = 0; i <= *it - 1; ++i)
					{
						Poco::DynamicAny nullValue;
						newArray->add(nullValue);
					}

					parentArray = newArray;
				}

				currentObject->set(name, topArray);
				currentObject = new JSON::Object();
				newArray->add(currentObject);
			}
		}
		else // We have a value
		{
			if ( indexes.empty() ) // We want an object
			{
				if ( result.type() == typeid(JSON::Object::Ptr) )
				{
					currentObject = result.extract<JSON::Object::Ptr>();
				}
				else
				{
					throw SyntaxException("Expected a JSON object");
				}
			}
			else
			{
				if ( result.type() == typeid(JSON::Array::Ptr) )
				{
					JSON::Array::Ptr arr = result.extract<JSON::Array::Ptr>();

					for(std::vector<int>::iterator it = indexes.begin(); it != indexes.end() - 1; ++it)
					{
						JSON::Array::Ptr currentArray = arr;
						arr = arr->getArray(*it);
						if ( arr.isNull() )
						{
							arr = new JSON::Array();
							currentArray->add(arr);
						}
					}

					result = arr->get(*indexes.rbegin());
					if ( result.isEmpty() ) // Index doesn't exist
					{
						JSON::Object::Ptr newObject = new JSON::Object();
						arr->add(newObject);
						currentObject = newObject;
					}
					else // Index is available
					{
						if ( result.type() == typeid(JSON::Object::Ptr) )
						{
							currentObject = result.extract<JSON::Object::Ptr>();
						}
						else
						{
							throw SyntaxException("Expected a JSON object");
						}
					}
				}
				else
				{
					throw SyntaxException("Expected a JSON array");
				}
			}
		}
	}
	return currentObject;
}


void JSONConfiguration::setValue(const std::string& key, const Poco::DynamicAny& value)
{

	std::string sValue;
	
	value.convert<std::string>(sValue);
	KeyValue kv(key, sValue);
	
	if (eventsEnabled())
	{
		propertyChanging(this, kv);
	}
	
	std::string lastPart;
	JSON::Object::Ptr parentObject = findStart(key, lastPart);

	std::vector<int> indexes;
	getIndexes(lastPart, indexes);

	if ( indexes.empty() ) // No Array
	{
		parentObject->set(lastPart, value);
	}
	else
	{
		DynamicAny result = parentObject->get(lastPart);
		if ( result.isEmpty() )
		{
			result = JSON::Array::Ptr(new JSON::Array());
			parentObject->set(lastPart, result);
		}
		else if ( result.type() != typeid(JSON::Array::Ptr) )
		{
			throw SyntaxException("Expected a JSON array");
		}

		JSON::Array::Ptr arr = result.extract<JSON::Array::Ptr>();
		for(std::vector<int>::iterator it = indexes.begin(); it != indexes.end() - 1; ++it)
		{
			JSON::Array::Ptr nextArray = arr->getArray(*it);
			if ( nextArray.isNull()  )
			{
				for(int i = static_cast<int>(arr->size()); i <= *it; ++i)
				{
					Poco::DynamicAny nullValue;
					arr->add(nullValue);
				}
				nextArray = new JSON::Array();
				arr->add(nextArray);
			}
			arr = nextArray;
		}
		arr->set(indexes.back(), value);
	}

	if (eventsEnabled())
	{
		propertyChanged(this, kv);
	}
}


void JSONConfiguration::setString(const std::string& key, const std::string& value)
{
	setValue(key, value);
}


void JSONConfiguration::setRaw(const std::string& key, const std::string& value)
{
	setValue(key, value);
}


void JSONConfiguration::setInt(const std::string& key, int value)
{
	setValue(key, value);
}


void JSONConfiguration::setBool(const std::string& key, bool value)
{
	setValue(key, value);
}


void JSONConfiguration::setDouble(const std::string& key, double value)
{
	setValue(key, value);
}


void JSONConfiguration::enumerate(const std::string& key, Keys& range) const
{
	JSON::Query query(_object);
	Poco::DynamicAny result = query.find(key);
	if ( result.type() == typeid(JSON::Object::Ptr) )
	{
		JSON::Object::Ptr object = result.extract<JSON::Object::Ptr>();
		object->getNames(range);
	}
}


void JSONConfiguration::save(std::ostream& ostr, unsigned int indent) const
{
	_object->stringify(ostr, indent);
}


void JSONConfiguration::removeRaw(const std::string& key)

{
	
	std::string lastPart;
	JSON::Object::Ptr parentObject = findStart(key, lastPart);
	std::vector<int> indexes;
	getIndexes(lastPart, indexes);

	if ( indexes.empty() ) // No Array
	{
		parentObject->remove(lastPart);
	}
	else
	{
		DynamicAny result = parentObject->get(lastPart);
		if (!result.isEmpty() && result.type() == typeid(JSON::Array::Ptr))
		{

			JSON::Array::Ptr arr = result.extract<JSON::Array::Ptr>();
			for(std::vector<int>::iterator it = indexes.begin(); it != indexes.end() - 1; ++it)
			{
				arr = arr->getArray(*it);
			}
			arr->remove(indexes.back());
		}
	}

}


} } // namespace Poco::Util


#endif // POCO_UTIL_NO_JSONCONFIGURATION
