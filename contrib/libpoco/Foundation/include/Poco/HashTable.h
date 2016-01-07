//
// HashTable.h
//
// $Id: //poco/1.4/Foundation/include/Poco/HashTable.h#1 $
//
// Library: Foundation
// Package: Hashing
// Module:  HashTable
//
// Definition of the HashTable class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_HashTable_INCLUDED
#define Foundation_HashTable_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Exception.h"
#include "Poco/HashFunction.h"
#include "Poco/HashStatistic.h"
#include <vector>
#include <map>
#include <cstddef>
#include <cstring>


namespace Poco {


//@ deprecated
template <class Key, class Value, class KeyHashFunction = HashFunction<Key> >
class HashTable
	/// A HashTable stores a key value pair that can be looked up via a hashed key.
	///
	/// Collision handling is done via overflow maps(!). With small hash tables performance of this
	/// data struct will be closer to that a map than a hash table, i.e. slower. On the plus side,
	/// this class offers remove operations. Also HashTable full errors are not possible. If a fast
	/// HashTable implementation is needed and the remove operation is not required, use SimpleHashTable
	/// instead.
	///
	/// This class is NOT thread safe.
{
public:
	typedef std::map<Key, Value> HashEntryMap;
	typedef HashEntryMap**       HashTableVector;

	typedef typename HashEntryMap::const_iterator ConstIterator;
	typedef typename HashEntryMap::iterator Iterator;

	HashTable(UInt32 initialSize = 251): 
		_entries(0), 
		_size(0), 
		_maxCapacity(initialSize)
		/// Creates the HashTable.
	{
		_entries = new HashEntryMap*[initialSize];
		memset(_entries, '\0', sizeof(HashEntryMap*)*initialSize);
	}

	HashTable(const HashTable& ht):
		_entries(new HashEntryMap*[ht._maxCapacity]),
		_size(ht._size),
		_maxCapacity(ht._maxCapacity)
	{
		for (UInt32 i = 0; i < _maxCapacity; ++i)
		{
			if (ht._entries[i])
				_entries[i] = new HashEntryMap(ht._entries[i]->begin(), ht._entries[i]->end());
			else
				_entries[i] = 0;
		}
	}

	~HashTable()
		/// Destroys the HashTable.
	{
		clear();
	}

	HashTable& operator = (const HashTable& ht)
	{
		if (this != &ht)
		{
			clear();
			_maxCapacity = ht._maxCapacity;
			poco_assert_dbg (_entries == 0);
			_entries = new HashEntryMap*[_maxCapacity];
			_size = ht._size;

			for (UInt32 i = 0; i < _maxCapacity; ++i)
			{
				if (ht._entries[i])
					_entries[i] = new HashEntryMap(ht._entries[i]->begin(), ht._entries[i]->end());
				else
					_entries[i] = 0;
			}
		}
		return *this;
	}

	void clear()
	{
		if (!_entries)
			return;
		for (UInt32 i = 0; i < _maxCapacity; ++i)
		{
			delete _entries[i];
		}
		delete[] _entries;
		_entries     = 0;
		_size        = 0;
		_maxCapacity = 0;
	}

	UInt32 insert(const Key& key, const Value& value)
		/// Returns the hash value of the inserted item.
		/// Throws an exception if the entry was already inserted
	{
		UInt32 hsh = hash(key);
		insertRaw(key, hsh, value);
		return hsh;
	}

	Value& insertRaw(const Key& key, UInt32 hsh, const Value& value)
		/// Returns the hash value of the inserted item.
		/// Throws an exception if the entry was already inserted
	{
		if (!_entries[hsh])
			_entries[hsh] = new HashEntryMap();
		std::pair<typename HashEntryMap::iterator, bool> res(_entries[hsh]->insert(std::make_pair(key, value)));
		if (!res.second)
			throw InvalidArgumentException("HashTable::insert, key already exists.");
		_size++;
		return res.first->second;
	}

	UInt32 update(const Key& key, const Value& value)
		/// Returns the hash value of the inserted item.
		/// Replaces an existing entry if it finds one
	{
		UInt32 hsh = hash(key);
		updateRaw(key, hsh, value);
		return hsh;
	}

	void updateRaw(const Key& key, UInt32 hsh, const Value& value)
		/// Returns the hash value of the inserted item.
		/// Replaces an existing entry if it finds one
	{
		if (!_entries[hsh])
			_entries[hsh] = new HashEntryMap();
		std::pair<Iterator, bool> res = _entries[hsh]->insert(std::make_pair(key, value));
		if (res.second == false)
			res.first->second = value;
		else
			_size++;
	}

	void remove(const Key& key)
	{
		UInt32 hsh = hash(key);
		removeRaw(key, hsh);
	}

	void removeRaw(const Key& key, UInt32 hsh)
		/// Performance version, allows to specify the hash value
	{
		if (_entries[hsh])
		{
			_size -= _entries[hsh]->erase(key);
		}
	}

	UInt32 hash(const Key& key) const
	{
		return _hash(key, _maxCapacity);
	}

	const Value& get(const Key& key) const
		/// Throws an exception if the value does not exist
	{
		UInt32 hsh = hash(key);
		return getRaw(key, hsh);
	}

	const Value& getRaw(const Key& key, UInt32 hsh) const
		/// Throws an exception if the value does not exist
	{
		if (!_entries[hsh])
			throw InvalidArgumentException("key not found");

		ConstIterator it = _entries[hsh]->find(key);
		if (it == _entries[hsh]->end())
			throw InvalidArgumentException("key not found");

		return it->second;
	}

	Value& get(const Key& key)
		/// Throws an exception if the value does not exist
	{
		UInt32 hsh = hash(key);
		return const_cast<Value&>(getRaw(key, hsh));
	}

	const Value& operator [] (const Key& key) const
	{
		return get(key);
	}
	
	Value& operator [] (const Key& key)
	{
		UInt32 hsh = hash(key);

		if (!_entries[hsh])
			return insertRaw(key, hsh, Value());
			
		ConstIterator it = _entries[hsh]->find(key);
		if (it == _entries[hsh]->end())
			return insertRaw(key, hsh, Value());

		return it->second;
	}
	
	const Key& getKeyRaw(const Key& key, UInt32 hsh)
		/// Throws an exception if the key does not exist. returns a reference to the internally
		/// stored key. Useful when someone does an insert and wants for performance reason only to store
		/// a pointer to the key in another collection
	{
		if (!_entries[hsh])
			throw InvalidArgumentException("key not found");
		ConstIterator it = _entries[hsh]->find(key);
		if (it == _entries[hsh]->end())
			throw InvalidArgumentException("key not found");
		return it->first;
	}

	bool get(const Key& key, Value& v) const
		/// Sets v to the found value, returns false if no value was found
	{
		UInt32 hsh = hash(key);
		return getRaw(key, hsh, v);
	}

	bool getRaw(const Key& key, UInt32 hsh, Value& v) const
		/// Sets v to the found value, returns false if no value was found
	{
		if (!_entries[hsh])
			return false;

		ConstIterator it = _entries[hsh]->find(key);
		if (it == _entries[hsh]->end())
			return false;

		v = it->second;
		return true;
	}

	bool exists(const Key& key)
	{
		UInt32 hsh = hash(key);
		return existsRaw(key, hsh);
	}

	bool existsRaw(const Key& key, UInt32 hsh)
	{
		return _entries[hsh] && (_entries[hsh]->end() != _entries[hsh]->find(key));
	}

	std::size_t size() const
		/// Returns the number of elements already inserted into the HashTable
	{
		return _size;
	}
	
	UInt32 maxCapacity() const
	{
		return _maxCapacity;
	}

	void resize(UInt32 newSize)
		/// Resizes the hashtable, rehashes all existing entries. Expensive!
	{
		if (_maxCapacity != newSize)
		{
			HashTableVector cpy = _entries;
			_entries = 0;
			UInt32 oldSize = _maxCapacity;
			_maxCapacity = newSize;
			_entries = new HashEntryMap*[_maxCapacity];
			memset(_entries, '\0', sizeof(HashEntryMap*)*_maxCapacity);

			if (_size == 0)
			{
				// no data was yet inserted
				delete[] cpy;
				return;
			}
			_size = 0;
			for (UInt32 i = 0; i < oldSize; ++i)
			{
				if (cpy[i])
				{
					ConstIterator it = cpy[i]->begin();
					ConstIterator itEnd = cpy[i]->end();
					for (; it != itEnd; ++it)
					{
						insert(it->first, it->second);
					}
					delete cpy[i];
				}
			}
			delete[] cpy;
		}
	}

	HashStatistic currentState(bool details = false) const
		/// Returns the current internal state
	{
		UInt32 numberOfEntries = (UInt32)_size;
		UInt32 numZeroEntries = 0;
		UInt32 maxEntriesPerHash = 0;
		std::vector<UInt32> detailedEntriesPerHash;
	#ifdef _DEBUG
		UInt32 totalSize = 0;
	#endif
		for (UInt32 i = 0; i < _maxCapacity; ++i)
		{
			if (_entries[i])
			{
				UInt32 size = (UInt32)_entries[i]->size();
				poco_assert_dbg(size != 0);
				if (size > maxEntriesPerHash)
					maxEntriesPerHash = size;
				if (details)
					detailedEntriesPerHash.push_back(size);
	#ifdef _DEBUG
				totalSize += size;
	#endif
			}
			else
			{
				numZeroEntries++;
				if (details)
					detailedEntriesPerHash.push_back(0);
			}
		}
	#ifdef _DEBUG
		poco_assert_dbg(totalSize == numberOfEntries);
	#endif
		return HashStatistic(_maxCapacity, numberOfEntries, numZeroEntries, maxEntriesPerHash, detailedEntriesPerHash);
	}

private:
	HashTableVector _entries;
	std::size_t     _size;
	UInt32          _maxCapacity;
	KeyHashFunction _hash;
};


} // namespace Poco


#endif // Foundation_HashTable_INCLUDED
