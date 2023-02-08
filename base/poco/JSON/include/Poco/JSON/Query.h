//
// Query.h
//
// Library: JSON
// Package: JSON
// Module:  Query
//
// Definition of the Query class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef JSON_JSONQuery_INCLUDED
#define JSON_JSONQuery_INCLUDED


#include "Poco/JSON/JSON.h"
#include "Poco/JSON/Object.h"
#include "Poco/JSON/Array.h"


namespace Poco {
namespace JSON {


class JSON_API Query
	/// Class that can be used to search for a value in a JSON object or array.
{
public:
	Query(const Dynamic::Var& source);
		/// Creates a Query/
		///
		/// Source must be JSON Object, Array, Object::Ptr,
		/// Array::Ptr or empty Var. Any other type will trigger throwing of 
		/// InvalidArgumentException.
		///
		/// Creating Query holding Ptr will typically result in faster
		/// performance.

	virtual ~Query();
		/// Destroys the Query.

	Object::Ptr findObject(const std::string& path) const;
		/// Search for an object. 
		///
		/// When the object can't be found, a zero Ptr is returned; 
		/// otherwise, a shared pointer to internally held object
		/// is returned.
		/// If object (as opposed to a pointer to object) is held
		/// internally, a shared pointer to new (heap-allocated) Object is
		/// returned; this may be expensive operation.

	Object& findObject(const std::string& path, Object& obj) const;
		/// Search for an object. 
		///
		/// If object is found, it is assigned to the
		/// Object through the reference passed in. When the object can't be 
		/// found, the provided Object is emptied and returned.

	Array::Ptr findArray(const std::string& path) const;
		/// Search for an array. 
		///
		/// When the array can't be found, a zero Ptr is returned; 
		/// otherwise, a shared pointer to internally held array
		/// is returned.
		/// If array (as opposed to a pointer to array) is held
		/// internally, a shared pointer to new (heap-allocated) Object is
		/// returned; this may be expensive operation.

	Array& findArray(const std::string& path, Array& obj) const;
		/// Search for an array. 
		///
		/// If array is found, it is assigned to the
		/// Object through the reference passed in. When the array can't be 
		/// found, the provided Object is emptied and returned.

	Dynamic::Var find(const std::string& path) const;
		/// Searches a value.
		///
		/// Example: "person.children[0].name" will return the
		/// the name of the first child. When the value can't be found
		/// an empty value is returned.

	template<typename T>
	T findValue(const std::string& path, const T& def) const
		/// Searches for a value will convert it to the given type.
		/// When the value can't be found or has an invalid type
		/// the default value will be returned.
	{
		T result = def;
		Dynamic::Var value = find(path);
		if (!value.isEmpty())
		{
			try
			{
				result = value.convert<T>();
			}
			catch (...) 
			{ 
			}
		}
		return result;
	}

	std::string findValue(const char* path, const char* def) const
		/// Searches for a value will convert it to the given type.
		/// When the value can't be found or has an invalid type
		/// the default value will be returned.
	{
		return findValue<std::string>(path, def);
	}

private:
	Dynamic::Var _source;
};


} } // namespace Poco::JSON


#endif // JSON_JSONQuery_INCLUDED
