//
// ListMap.h
//
// Library: Foundation
// Package: Hashing
// Module:  ListMap
//
// Definition of the ListMap class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_ListMap_INCLUDED
#define Foundation_ListMap_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/String.h"
#include "Poco/Exception.h"
#include <list>
#include <utility>


namespace Poco {


template <class Key, class Mapped, class Container = std::list<std::pair<Key, Mapped> >, bool CaseSensitive = false >
class ListMap
	/// This class implements a multimap in terms of a sequential container.
	/// The use for this type of associative container is wherever automatic
	/// ordering of elements is not desirable. Naturally, this container will
	/// have inferior data retrieval performance and it is not recommended for
	/// use with large datasets. The main purpose within POCO is for Internet
	/// messages (email message, http headers etc), to prevent automatic 
	/// header entry reordering.
{
public:
	typedef Key                 KeyType;
	typedef Mapped              MappedType;
	typedef Mapped&             Reference;
	typedef const Mapped&       ConstReference;
	typedef Mapped*             Pointer;
	typedef const Mapped*       ConstPointer;
	
	typedef typename Container::value_type     ValueType;
	typedef typename Container::size_type      SizeType;
	typedef typename Container::iterator       Iterator;
	typedef typename Container::const_iterator ConstIterator;
	
	ListMap()
		/// Creates an empty ListMap.
	{
	}
	
	ListMap(std::size_t initialReserve):
		_list(initialReserve)
		/// Creates the ListMap with room for initialReserve entries.
	{
	}
	
	ListMap& operator = (const ListMap& map)
		/// Assigns another ListMap.
	{
		ListMap tmp(map);
		swap(tmp);
		return *this;
	}
	
	void swap(ListMap& map)
		/// Swaps the ListMap with another one.
	{
		_list.swap(map._list);
	}
	
	ConstIterator begin() const
		/// Returns the beginning of the map.
	{
		return _list.begin();
	}
	
	ConstIterator end() const
		/// Returns the end of the map.
	{
		return _list.end();
	}
	
	Iterator begin()
		/// Returns the beginning of the map.
	{
		return _list.begin();
	}
	
	Iterator end()
		/// Returns the end of the map.
	{
		return _list.end();
	}
	
	ConstIterator find(const KeyType& key) const
		/// Finds the first occurrence of the key and
		/// returns iterator pointing to the found entry
		/// or iterator pointing to the end if entry is
		/// not found.
	{
		typename Container::const_iterator it = _list.begin();
		typename Container::const_iterator itEnd = _list.end();
		for(; it != itEnd; ++it)
		{
			if (isEqual(it->first, key)) return it;
		}
		return itEnd;
	}

	Iterator find(const KeyType& key)
		/// Finds the first occurrence of the key and
		/// returns iterator pointing to the found entry
		/// or iterator pointing to the end if entry is
		/// not found.
	{
		typename Container::iterator it = _list.begin();
		typename Container::iterator itEnd = _list.end();
		for(; it != itEnd; ++it)
		{
			if (isEqual(it->first, key)) return it;
		}
		return itEnd;
	}

	Iterator insert(const ValueType& val)
		/// Inserts the value into the map. If one or more values 
		/// already exist, new value is inserted at the end of the 
		/// block. Thus, all the equal value entries are located
		/// sequentially at all times.
		/// Returns iterator pointing to the newly inserted value 
	{
		Iterator it = find(val.first);
		while (it != _list.end() && isEqual(it->first, val.first)) ++it;
		return _list.insert(it, val);
	}
	
	void erase(Iterator it)
	{
		_list.erase(it);
	}
	
	SizeType erase(const KeyType& key)
	{
		SizeType count = 0;
		Iterator it = find(key);
		bool removed = false;
		while (it != _list.end())
		{
			if (isEqual(it->first, key))
			{
				++count;
				it = _list.erase(it);
				removed = true;
			}
			else
			{
				if (removed) return count;
				++it;
			}
		}
		return count;
	}
	
	void clear()
	{
		_list.clear();
	}

	std::size_t size() const
	{
		return _list.size();
	}

	bool empty() const
	{
		return _list.empty();
	}

	ConstReference operator [] (const KeyType& key) const
	{
		ConstIterator it = find(key);
		if (it != _list.end())
			return it->second;
		else
			throw NotFoundException();
	}

	Reference operator [] (const KeyType& key)
	{
		Iterator it = find(key);
		if (it != _list.end())
			return it->second;
		else
		{
			ValueType value(key, Mapped());
			Iterator itInsert = insert(value);
			return itInsert->second;
		}
	}

private:
	template <typename T1, typename T2>
	bool isEqual(T1 val1, T2 val2) const
	{
		return val1 == val2;
	}

	bool isEqual(const std::string& s1, const std::string& s2) const
	{
		if (!CaseSensitive)
			return Poco::icompare(s1, s2) == 0;
		else
			return s1 == s2;
	}

	bool isEqual(const std::string& s1, const char* s2) const
	{
		return isEqual(s1, std::string(s2));
	}

	bool isEqual(const char* s1, const std::string& s2) const
	{
		return isEqual(std::string(s1), s2);
	}

	bool isEqual(const char* s1, const char* s2) const
	{
		return isEqual(std::string(s1), std::string(s2));
	}

	Container _list;
};


} // namespace Poco


#endif // Foundation_ListMap_INCLUDED
