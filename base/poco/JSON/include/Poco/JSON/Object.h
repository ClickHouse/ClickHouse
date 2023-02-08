//
// Object.h
//
// Library: JSON
// Package: JSON
// Module:  Object
//
// Definition of the Object class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef JSON_Object_INCLUDED
#define JSON_Object_INCLUDED


#include "Poco/JSON/JSON.h"
#include "Poco/JSON/Array.h"
#include "Poco/JSON/Stringifier.h"
#include "Poco/JSONString.h"
#include "Poco/SharedPtr.h"
#include "Poco/Dynamic/Var.h"
#include "Poco/Dynamic/Struct.h"
#include "Poco/Nullable.h"
#include <map>
#include <vector>
#include <deque>
#include <iostream>
#include <sstream>


namespace Poco {
namespace JSON {


class JSON_API Object
	/// Represents a JSON object. Object provides a representation based on
	/// shared pointers and optimized for performance. It is possible to
	/// convert Object to DynamicStruct. Conversion requires copying and therefore
	/// has performance penalty; the benefit is in improved syntax, eg:
	///
	///    std::string json = "{ \"test\" : { \"property\" : \"value\" } }";
	///    Parser parser;
	///    Var result = parser.parse(json);
	///
	///    // use pointers to avoid copying
	///    Object::Ptr object = result.extract<Object::Ptr>();
	///    Var test = object->get("test"); // holds { "property" : "value" }
	///    Object::Ptr subObject = test.extract<Object::Ptr>();
	///    test = subObject->get("property");
	///    std::string val = test.toString(); // val holds "value"
	///
	///    // copy/convert to Poco::DynamicStruct
	///    Poco::DynamicStruct ds = *object;
	///    val = ds["test"]["property"]; // val holds "value"
	///
{
public:
	typedef SharedPtr<Object>                   Ptr;
	typedef std::map<std::string, Dynamic::Var> ValueMap;
	typedef ValueMap::value_type                ValueType;
	typedef ValueMap::iterator                  Iterator;
	typedef ValueMap::const_iterator            ConstIterator;
	typedef std::vector<std::string>            NameList;

	explicit Object(int options = 0);
		/// Creates an empty Object.
		///
		/// If JSON_PRESERVE_KEY_ORDER is specified, the object will
		/// preserve the items insertion order. Otherwise, items will be
		/// sorted by keys.
		///
		/// If JSON_ESCAPE_UNICODE is specified, when the object is
		/// stringified, all unicode characters will be escaped in the
		/// resulting string.

	Object(const Object& copy);
		/// Creates an Object by copying another one.
		///
		/// Struct is not copied to keep the operation as
		/// efficient as possible (when needed, it will be generated upon request).

#ifdef POCO_ENABLE_CPP11

	Object(Object&& other);
		/// Move constructor

	Object &operator =(Object &&other);
		// Move asignment operator

#endif // POCO_ENABLE_CPP11

	virtual ~Object();
		/// Destroys the Object.

	Object &operator =(const Object &other);
		// Assignment operator

	void setEscapeUnicode(bool escape = true);
		/// Sets the flag for escaping unicode.

	bool getEscapeUnicode() const;
		/// Returns the flag for escaping unicode.

	Iterator begin();
		/// Returns begin iterator for values.

	ConstIterator begin() const;
		/// Returns const begin iterator for values.

	Iterator end();
		/// Returns end iterator for values.

	ConstIterator end() const;
		/// Returns const end iterator for values.

	Dynamic::Var get(const std::string& key) const;
		/// Retrieves a property. An empty value is
		/// returned when the property doesn't exist.

	Array::Ptr getArray(const std::string& key) const;
		/// Returns a SharedPtr to an array when the property
		/// is an array. An empty SharedPtr is returned when
		/// the element doesn't exist or is not an array.

	Object::Ptr getObject(const std::string& key) const;
		/// Returns a SharedPtr to an object when the property
		/// is an object. An empty SharedPtr is returned when
		/// the property doesn't exist or is not an object

	template<typename T>
	T getValue(const std::string& key) const
		/// Retrieves the property with the given name and will
		/// try to convert the value to the given template type.
		/// The convert<T>() method of Var is called
		/// which can also throw exceptions for invalid values.
		/// Note: This will not work for an array or an object.
	{
		Dynamic::Var value = get(key);
		return value.convert<T>();
	}

	template<typename T>
	Poco::Nullable<T> getNullableValue(const std::string& key) const
		/// Retrieves the property with the given name and will
		/// try to convert the value to the given template type.
		///
		/// The convert<T> method of Var is called
		/// which can also throw exceptions for invalid values.
		/// Note: This will not work for an array or an object.
	{
		if (isNull(key))
			return Poco::Nullable<T>();

		Dynamic::Var value = get(key);
		return value.convert<T>();
	}

	void getNames(NameList& names) const;
		/// Fills the supplied vector with all property names.

	NameList getNames() const;
		/// Returns all property names.

	bool has(const std::string& key) const;
		/// Returns true when the given property exists.

	bool isArray(const std::string& key) const;
		/// Returns true when the given property contains an array.

	bool isArray(ConstIterator& it) const;
		/// Returns true when the given property contains an array.

	bool isNull(const std::string& key) const;
		/// Returns true when the given property contains a null value.

	bool isObject(const std::string& key) const;
		/// Returns true when the given property contains an object.

	bool isObject(ConstIterator& it) const;
		/// Returns true when the given property contains an object.

	template<typename T>
	T optValue(const std::string& key, const T& def) const
		/// Returns the value of a property when the property exists
		/// and can be converted to the given type. Otherwise
		/// def will be returned.
	{
		T value = def;
		ValueMap::const_iterator it = _values.find(key);
		if (it != _values.end() && ! it->second.isEmpty())
		{
			try
			{
				value = it->second.convert<T>();
			}
			catch (...)
			{
				// The default value will be returned
			}
		}
		return value;
	}

	std::size_t size() const;
		/// Returns the number of properties.

	void set(const std::string& key, const Dynamic::Var& value);
		/// Sets a new value.

	void stringify(std::ostream& out, unsigned int indent = 0, int step = -1) const;
		/// Prints the object to out stream.
		///
		/// When indent is 0, the object will be printed on a single
		/// line without indentation.

	void remove(const std::string& key);
		/// Removes the property with the given key.

	static Poco::DynamicStruct makeStruct(const Object::Ptr& obj);
		/// Utility function for creation of struct.

	operator const Poco::DynamicStruct& () const;
		/// Cast operator to Poco::DynamiStruct.

	void clear();
		/// Clears the contents of the object.
		///
		/// Insertion order preservation property is left intact.

private:
	typedef std::deque<ValueMap::const_iterator>  KeyList;
	typedef Poco::DynamicStruct::Ptr              StructPtr;

	void resetDynStruct() const;
	void syncKeys(const KeyList& keys);

	template <typename C>
	void doStringify(const C& container, std::ostream& out, unsigned int indent, unsigned int step) const
	{
		int options = Poco::JSON_WRAP_STRINGS;
		options |= _escapeUnicode ? Poco::JSON_ESCAPE_UNICODE : 0;

		out << '{';

		if (indent > 0) out << std::endl;

		typename C::const_iterator it = container.begin();
		typename C::const_iterator end = container.end();
		for (; it != end;)
		{
			for (unsigned int i = 0; i < indent; i++) out << ' ';

			Stringifier::stringify(getKey(it), out, indent, step, options);
			out << ((indent > 0) ? " : " : ":");

			Stringifier::stringify(getValue(it), out, indent + step, step, options);

			if (++it != container.end()) out << ',';

			if (step > 0) out << std::endl;
		}

		if (indent >= step) indent -= step;

		for (unsigned int i = 0; i < indent; i++) out << ' ';

		out << '}';
	}

	const std::string& getKey(ValueMap::const_iterator& it) const;
	const Dynamic::Var& getValue(ValueMap::const_iterator& it) const;
	const std::string& getKey(KeyList::const_iterator& it) const;
	const Dynamic::Var& getValue(KeyList::const_iterator& it) const;

	ValueMap          _values;
	KeyList           _keys;
	bool              _preserveInsOrder;
	// Note:
	//  The reason for this flag (rather than as argument to stringify()) is
	//  because Object can be returned stringified from Dynamic::Var::toString(),
	//  so it must know whether to escape unicode or not.
	bool              _escapeUnicode;
	mutable StructPtr _pStruct;
	mutable bool      _modified;
};


//
// inlines
//

inline void Object::setEscapeUnicode(bool escape)
{
	_escapeUnicode = escape;
}


inline bool Object::getEscapeUnicode() const
{
	return _escapeUnicode;
}


inline Object::Iterator Object::begin()
{
	return _values.begin();
}


inline Object::ConstIterator Object::begin() const
{
	return _values.begin();
}


inline Object::Iterator Object::end()
{
	return _values.end();
}


inline Object::ConstIterator Object::end() const
{
	return _values.end();
}


inline bool Object::has(const std::string& key) const
{
	ValueMap::const_iterator it = _values.find(key);
	return it != _values.end();
}


inline bool Object::isArray(const std::string& key) const
{
	ValueMap::const_iterator it = _values.find(key);
	return isArray(it);
}


inline bool Object::isArray(ConstIterator& it) const
{
	return it != _values.end() && (it->second.type() == typeid(Array::Ptr) || it->second.type() == typeid(Array));
}


inline bool Object::isNull(const std::string& key) const
{
	ValueMap::const_iterator it = _values.find(key);
	return it == _values.end() || it->second.isEmpty();
}


inline bool Object::isObject(const std::string& key) const
{
	ValueMap::const_iterator it = _values.find(key);
	return isObject(it);
}


inline bool Object::isObject(ConstIterator& it) const
{
	return it != _values.end() && (it->second.type() == typeid(Object::Ptr) || it->second.type() == typeid(Object));
}


inline std::size_t Object::size() const
{
	return static_cast<std::size_t>(_values.size());
}


inline void Object::remove(const std::string& key)
{
	_values.erase(key);
	if (_preserveInsOrder)
	{
		KeyList::iterator it = _keys.begin();
		KeyList::iterator end = _keys.end();
		for (; it != end; ++it)
		{
			if (key == (*it)->first)
			{
				_keys.erase(it);
				break;
			}
		}
	}
	_modified = true;
}


inline const std::string& Object::getKey(ValueMap::const_iterator& it) const
{
	return it->first;
}


inline const Dynamic::Var& Object::getValue(ValueMap::const_iterator& it) const
{
	return it->second;
}


inline const Dynamic::Var& Object::getValue(KeyList::const_iterator& it) const
{
	ValueMap::const_iterator itv = _values.find((*it)->first);
	if (itv != _values.end())
		return itv->second;
	else
		throw Poco::NotFoundException();
}


} } // namespace Poco::JSON


namespace Poco {
namespace Dynamic {


template <>
class VarHolderImpl<JSON::Object::Ptr>: public VarHolder
{
public:
	VarHolderImpl(const JSON::Object::Ptr& val): _val(val)
	{
	}

	~VarHolderImpl()
	{
	}

	const std::type_info& type() const
	{
		return typeid(JSON::Object::Ptr);
	}

	void convert(Int8&) const
	{
		throw BadCastException();
	}

	void convert(Int16&) const
	{
		throw BadCastException();
	}

	void convert(Int32&) const
	{
		throw BadCastException();
	}

	void convert(Int64&) const
	{
		throw BadCastException();
	}

	void convert(UInt8&) const
	{
		throw BadCastException();
	}

	void convert(UInt16&) const
	{
		throw BadCastException();
	}

	void convert(UInt32&) const
	{
		throw BadCastException();
	}

	void convert(UInt64&) const
	{
		throw BadCastException();
	}

	void convert(bool& value) const
	{
		value = !_val.isNull() && _val->size() > 0;
	}

	void convert(float&) const
	{
		throw BadCastException();
	}

	void convert(double&) const
	{
		throw BadCastException();
	}

	void convert(char&) const
	{
		throw BadCastException();
	}

	void convert(std::string& s) const
	{
		std::ostringstream oss;
		_val->stringify(oss, 2);
		s = oss.str();
	}

	void convert(DateTime& /*val*/) const
	{
		//TODO: val = _val;
		throw NotImplementedException("Conversion not implemented: JSON:Object => DateTime");
	}

	void convert(LocalDateTime& /*ldt*/) const
	{
		//TODO: ldt = _val.timestamp();
		throw NotImplementedException("Conversion not implemented: JSON:Object => LocalDateTime");
	}

	void convert(Timestamp& /*ts*/) const
	{
		//TODO: ts = _val.timestamp();
		throw NotImplementedException("Conversion not implemented: JSON:Object => Timestamp");
	}

	VarHolder* clone(Placeholder<VarHolder>* pVarHolder = 0) const
	{
		return cloneHolder(pVarHolder, _val);
	}

	const JSON::Object::Ptr& value() const
	{
		return _val;
	}

	bool isArray() const
	{
		return false;
	}

	bool isInteger() const
	{
		return false;
	}

	bool isSigned() const
	{
		return false;
	}

	bool isNumeric() const
	{
		return false;
	}

	bool isString() const
	{
		return false;
	}

private:
	JSON::Object::Ptr _val;
};


template <>
class VarHolderImpl<JSON::Object>: public VarHolder
{
public:
	VarHolderImpl(const JSON::Object& val): _val(val)
	{
	}

	~VarHolderImpl()
	{
	}

	const std::type_info& type() const
	{
		return typeid(JSON::Object);
	}

	void convert(Int8&) const
	{
		throw BadCastException();
	}

	void convert(Int16&) const
	{
		throw BadCastException();
	}

	void convert(Int32&) const
	{
		throw BadCastException();
	}

	void convert(Int64&) const
	{
		throw BadCastException();
	}

	void convert(UInt8&) const
	{
		throw BadCastException();
	}

	void convert(UInt16&) const
	{
		throw BadCastException();
	}

	void convert(UInt32&) const
	{
		throw BadCastException();
	}

	void convert(UInt64&) const
	{
		throw BadCastException();
	}

	void convert(bool& value) const
	{
		value = _val.size() > 0;
	}

	void convert(float&) const
	{
		throw BadCastException();
	}

	void convert(double&) const
	{
		throw BadCastException();
	}

	void convert(char&) const
	{
		throw BadCastException();
	}

	void convert(std::string& s) const
	{
		std::ostringstream oss;
		_val.stringify(oss, 2);
		s = oss.str();
	}

	void convert(DateTime& /*val*/) const
	{
		//TODO: val = _val;
		throw NotImplementedException("Conversion not implemented: JSON:Object => DateTime");
	}

	void convert(LocalDateTime& /*ldt*/) const
	{
		//TODO: ldt = _val.timestamp();
		throw NotImplementedException("Conversion not implemented: JSON:Object => LocalDateTime");
	}

	void convert(Timestamp& /*ts*/) const
	{
		//TODO: ts = _val.timestamp();
		throw NotImplementedException("Conversion not implemented: JSON:Object => Timestamp");
	}

	VarHolder* clone(Placeholder<VarHolder>* pVarHolder = 0) const
	{
		return cloneHolder(pVarHolder, _val);
	}

	const JSON::Object& value() const
	{
		return _val;
	}

	bool isArray() const
	{
		return false;
	}

	bool isInteger() const
	{
		return false;
	}

	bool isSigned() const
	{
		return false;
	}

	bool isNumeric() const
	{
		return false;
	}

	bool isString() const
	{
		return false;
	}

private:
	JSON::Object _val;
};


} } // namespace Poco::Dynamic


#endif // JSON_Object_INCLUDED
