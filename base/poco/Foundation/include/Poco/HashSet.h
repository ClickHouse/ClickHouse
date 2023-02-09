//
// HashSet.h
//
// Library: Foundation
// Package: Hashing
// Module:  HashSet
//
// Definition of the HashSet class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_HashSet_INCLUDED
#define Foundation_HashSet_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/LinearHashTable.h"


namespace Poco {


template <class Value, class HashFunc = Hash<Value> >
class HashSet
	/// This class implements a set using a LinearHashTable.
	///
	/// A HashSet can be used just like a std::set.
{
public:
	typedef Value        ValueType;
	typedef Value&       Reference;
	typedef const Value& ConstReference;
	typedef Value*       Pointer;
	typedef const Value* ConstPointer;
	typedef HashFunc     Hash;
	
	typedef LinearHashTable<ValueType, Hash> HashTable;
	
	typedef typename HashTable::Iterator      Iterator;
	typedef typename HashTable::ConstIterator ConstIterator;

	HashSet()
		/// Creates an empty HashSet.
	{
	}

	HashSet(std::size_t initialReserve): 
		_table(initialReserve)
		/// Creates the HashSet, using the given initialReserve.
	{
	}
	
	HashSet(const HashSet& set):
		_table(set._table)
		/// Creates the HashSet by copying another one.
	{
	}
	
	~HashSet()
		/// Destroys the HashSet.
	{
	}
	
	HashSet& operator = (const HashSet& table)
		/// Assigns another HashSet.
	{
		HashSet tmp(table);
		swap(tmp);
		return *this;
	}
	
	void swap(HashSet& set)
		/// Swaps the HashSet with another one.
	{
		_table.swap(set._table);
	}
	
	ConstIterator begin() const
		/// Returns an iterator pointing to the first entry, if one exists.
	{
		return _table.begin();
	}
	
	ConstIterator end() const
		/// Returns an iterator pointing to the end of the table.
	{
		return _table.end();
	}
	
	Iterator begin()
		/// Returns an iterator pointing to the first entry, if one exists.
	{
		return _table.begin();
	}
	
	Iterator end()
		/// Returns an iterator pointing to the end of the table.
	{
		return _table.end();
	}
		
	ConstIterator find(const ValueType& value) const
		/// Finds an entry in the table.
	{
		return _table.find(value);
	}

	Iterator find(const ValueType& value)
		/// Finds an entry in the table.
	{
		return _table.find(value);
	}
	
	std::size_t count(const ValueType& value) const
		/// Returns the number of elements with the given
		/// value, with is either 1 or 0.
	{
		return _table.count(value);
	}
	
	std::pair<Iterator, bool> insert(const ValueType& value)
		/// Inserts an element into the set.
		///
		/// If the element already exists in the set,
		/// a pair(iterator, false) with iterator pointing to the 
		/// existing element is returned.
		/// Otherwise, the element is inserted an a 
		/// pair(iterator, true) with iterator
		/// pointing to the new element is returned.
	{
		return _table.insert(value);
	}
	
	void erase(Iterator it)
		/// Erases the element pointed to by it.
	{
		_table.erase(it);
	}
	
	void erase(const ValueType& value)
		/// Erases the element with the given value, if it exists.
	{
		_table.erase(value);
	}
	
	void clear()
		/// Erases all elements.
	{
		_table.clear();
	}
	
	std::size_t size() const
		/// Returns the number of elements in the table.
	{
		return _table.size();
	}
	
	bool empty() const
		/// Returns true iff the table is empty.
	{
		return _table.empty();
	}

private:
	HashTable _table;
};


} // namespace Poco


#endif // Foundation_HashSet_INCLUDED
