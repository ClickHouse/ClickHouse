//
// Struct.h
//
// Library: Foundation
// Package: Dynamic
// Module:  Struct
//
// Definition of the Struct class.
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_Struct_INCLUDED
#define Foundation_Struct_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Dynamic/Var.h"
#include "Poco/Dynamic/VarHolder.h"
#include "Poco/SharedPtr.h"
#include <map>
#include <set>


namespace Poco {
namespace Dynamic {


template <typename K>
class Struct
	/// Struct allows to define a named collection of Var objects.
{
public:
	typedef typename std::map<K, Var> Data;
	typedef typename std::set<K> NameSet;
	typedef typename Data::iterator Iterator;
	typedef typename Data::const_iterator ConstIterator;
	typedef typename Struct<K>::Data::value_type ValueType;
	typedef typename Struct<K>::Data::size_type SizeType;
	typedef typename std::pair<typename Struct<K>::Iterator, bool> InsRetVal;
	typedef typename Poco::SharedPtr<Struct<K> > Ptr;

	Struct(): _data()
		/// Creates an empty Struct
	{
	}

	Struct(const Data& val): _data(val)
		/// Creates the Struct from the given value.
	{
	}

	template <typename T>
	Struct(const std::map<K, T>& val)
	{
		typedef typename std::map<K, T>::const_iterator MapConstIterator;

		MapConstIterator it = val.begin();
		MapConstIterator end = val.end();
		for (; it != end; ++it) _data.insert(ValueType(it->first, Var(it->second)));
	}

	virtual ~Struct()
		/// Destroys the Struct.
	{
	}

	inline Var& operator [] (const K& name)
		/// Returns the Var with the given name, creates an entry if not found.
	{
		return _data[name];
	}

	const Var& operator [] (const K& name) const
		/// Returns the Var with the given name, throws a
		/// NotFoundException if the data member is not found.
	{
		ConstIterator it = find(name);
		if (it == end()) throw NotFoundException(name);
		return it->second;
	}

	inline bool contains(const K& name) const
		/// Returns true if the Struct contains a member with the given name
	{
		return find(name) != end();
	}

	inline Iterator find(const K& name)
		/// Returns an iterator, pointing to the <name,Var> pair containing
		/// the element, or it returns end() if the member was not found
	{
		return _data.find(name);
	}

	inline ConstIterator find(const K& name) const
		/// Returns a const iterator, pointing to the <name,Var> pair containing
		/// the element, or it returns end() if the member was not found
	{
		return _data.find(name);
	}

	inline Iterator end()
		/// Returns the end iterator for the Struct
	{
		return _data.end();
	}

	inline ConstIterator end() const
		/// Returns the end const iterator for the Struct
	{
		return _data.end();
	}

	inline Iterator begin()
		/// Returns the begin iterator for the Struct
	{
		return _data.begin();
	}

	inline ConstIterator begin() const
		/// Returns the begin const iterator for the Struct
	{
		return _data.begin();
	}

	template <typename T>
	inline InsRetVal insert(const K& key, const T& value)
		/// Inserts a <name, Var> pair into the Struct,
		/// returns a pair containing the iterator and a boolean which
		/// indicates success or not (is true, when insert succeeded, false,
		/// when already another element was present, in this case Iterator
		/// points to that other element)
	{
		// fix: SunPro C++ is silly ...
		ValueType valueType(key, value);
		return insert(valueType);
	}

	inline InsRetVal insert(const ValueType& aPair)
		/// Inserts a <name, Var> pair into the Struct,
		/// returns a pair containing the iterator and a boolean which
		/// indicates success or not (is true, when insert succeeded, false,
		/// when already another element was present, in this case Iterator
		/// points to that other element)
	{
		return _data.insert(aPair);
	}

	inline SizeType erase(const K& key)
		/// Erases the element if found, returns number of elements deleted
	{
		return _data.erase(key);
	}

	inline void erase(Iterator& it)
		/// Erases the element at the given position
	{
		_data.erase(it);
	}

	inline bool empty() const
		/// Returns true if the Struct doesn't contain any members
	{
		return _data.empty();
	}

	inline void clear()
		/// Clears the Struct contents
	{
		_data.clear();
	}

	SizeType size() const
		/// Returns the number of members the Struct contains
	{
		return _data.size();
	}

	inline NameSet members() const
		/// Returns a sorted collection containing all member names
	{
		NameSet keys;
		ConstIterator it = begin();
		ConstIterator itEnd = end();
		for (; it != itEnd; ++it) keys.insert(it->first);
		return keys;
	}

	std::string toString()
	{
		std::string str;
		Var(*this).convert<std::string>(str);
		return str;
	}

private:
	Data _data;
};


template <>
class VarHolderImpl<Struct<std::string> >: public VarHolder
{
public:
	VarHolderImpl(const Struct<std::string>& val): _val(val)
	{
	}

	~VarHolderImpl()
	{
	}
	
	const std::type_info& type() const
	{
		return typeid(Struct<std::string>);
	}

	void convert(Int8&) const
	{
		throw BadCastException("Cannot cast Struct type to Int8");
	}

	void convert(Int16&) const
	{
		throw BadCastException("Cannot cast Struct type to Int16");
	}
	
	void convert(Int32&) const
	{
		throw BadCastException("Cannot cast Struct type to Int32");
	}

	void convert(Int64&) const
	{
		throw BadCastException("Cannot cast Struct type to Int64");
	}

	void convert(UInt8&) const
	{
		throw BadCastException("Cannot cast Struct type to UInt8");
	}

	void convert(UInt16&) const
	{
		throw BadCastException("Cannot cast Struct type to UInt16");
	}
	
	void convert(UInt32&) const
	{
		throw BadCastException("Cannot cast Struct type to UInt32");
	}

	void convert(UInt64&) const
	{
		throw BadCastException("Cannot cast Struct type to UInt64");
	}

	void convert(bool&) const
	{
		throw BadCastException("Cannot cast Struct type to bool");
	}

	void convert(float&) const
	{
		throw BadCastException("Cannot cast Struct type to float");
	}

	void convert(double&) const
	{
		throw BadCastException("Cannot cast Struct type to double");
	}

	void convert(char&) const
	{
		throw BadCastException("Cannot cast Struct type to char");
	}

	void convert(std::string& val) const
	{
		val.append("{ ");
		Struct<std::string>::ConstIterator it = _val.begin();
		Struct<std::string>::ConstIterator itEnd = _val.end();
		if (!_val.empty())
		{
			Var key(it->first);
			Impl::appendJSONKey(val, key);
			val.append(" : ");
			Impl::appendJSONValue(val, it->second);
			++it;
		}
		for (; it != itEnd; ++it)
		{
			val.append(", ");
			Var key(it->first);
			Impl::appendJSONKey(val, key);
			val.append(" : ");
			Impl::appendJSONValue(val, it->second);
		}
		val.append(" }");
	}

	void convert(Poco::DateTime&) const
	{
		throw BadCastException("Struct -> Poco::DateTime");
	}

	void convert(Poco::LocalDateTime&) const
	{
		throw BadCastException("Struct -> Poco::LocalDateTime");
	}

	void convert(Poco::Timestamp&) const
	{
		throw BadCastException("Struct -> Poco::Timestamp");
	}

	VarHolder* clone(Placeholder<VarHolder>* pVarHolder = 0) const
	{
		return cloneHolder(pVarHolder, _val);
	}
	
	const Struct<std::string>& value() const
	{
		return _val;
	}

	bool isArray() const
	{
		return false;
	}

	bool isStruct() const
	{
		return true;
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
	
	std::size_t size() const
	{
		return _val.size();
	}

	Var& operator [] (const std::string& name)
	{
		return _val[name];
	}

	const Var& operator [] (const std::string& name) const
	{
		return _val[name];
	}

private:
	Struct<std::string> _val;
};


template <>
class VarHolderImpl<Struct<int> >: public VarHolder
{
public:
	VarHolderImpl(const Struct<int>& val): _val(val)
	{
	}

	~VarHolderImpl()
	{
	}
	
	const std::type_info& type() const
	{
		return typeid(Struct<int>);
	}

	void convert(Int8&) const
	{
		throw BadCastException("Cannot cast Struct type to Int8");
	}

	void convert(Int16&) const
	{
		throw BadCastException("Cannot cast Struct type to Int16");
	}
	
	void convert(Int32&) const
	{
		throw BadCastException("Cannot cast Struct type to Int32");
	}

	void convert(Int64&) const
	{
		throw BadCastException("Cannot cast Struct type to Int64");
	}

	void convert(UInt8&) const
	{
		throw BadCastException("Cannot cast Struct type to UInt8");
	}

	void convert(UInt16&) const
	{
		throw BadCastException("Cannot cast Struct type to UInt16");
	}
	
	void convert(UInt32&) const
	{
		throw BadCastException("Cannot cast Struct type to UInt32");
	}

	void convert(UInt64&) const
	{
		throw BadCastException("Cannot cast Struct type to UInt64");
	}

	void convert(bool&) const
	{
		throw BadCastException("Cannot cast Struct type to bool");
	}

	void convert(float&) const
	{
		throw BadCastException("Cannot cast Struct type to float");
	}

	void convert(double&) const
	{
		throw BadCastException("Cannot cast Struct type to double");
	}

	void convert(char&) const
	{
		throw BadCastException("Cannot cast Struct type to char");
	}

	void convert(std::string& val) const
	{
		val.append("{ ");
		Struct<int>::ConstIterator it = _val.begin();
		Struct<int>::ConstIterator itEnd = _val.end();
		if (!_val.empty())
		{
			Var key(it->first);
			Impl::appendJSONKey(val, key);
			val.append(" : ");
			Impl::appendJSONValue(val, it->second);
			++it;
		}
		for (; it != itEnd; ++it)
		{
			val.append(", ");
			Var key(it->first);
			Impl::appendJSONKey(val, key);
			val.append(" : ");
			Impl::appendJSONValue(val, it->second);
		}
		val.append(" }");	
	}

	void convert(Poco::DateTime&) const
	{
		throw BadCastException("Struct -> Poco::DateTime");
	}

	void convert(Poco::LocalDateTime&) const
	{
		throw BadCastException("Struct -> Poco::LocalDateTime");
	}

	void convert(Poco::Timestamp&) const
	{
		throw BadCastException("Struct -> Poco::Timestamp");
	}

	VarHolder* clone(Placeholder<VarHolder>* pVarHolder = 0) const
	{
		return cloneHolder(pVarHolder, _val);
	}
	
	const Struct<int>& value() const
	{
		return _val;
	}

	bool isArray() const
	{
		return false;
	}

	bool isStruct() const
	{
		return true;
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

	std::size_t size() const
	{
		return _val.size();
	}

	Var& operator [] (int name)
	{
		return _val[name];
	}

	const Var& operator [] (int name) const
	{
		return _val[name];
	}

private:
	Struct<int> _val;
};


} // namespace Dynamic


typedef Dynamic::Struct<std::string> DynamicStruct;


} // namespace Poco


#endif // Foundation_Struct_INCLUDED
