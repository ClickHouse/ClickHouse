//
// SimpleHashTable.h
//
// $Id: //poco/1.4/Foundation/include/Poco/SimpleHashTable.h#1 $
//
// Library: Foundation
// Package: Hashing
// Module:  SimpleHashTable
//
// Definition of the SimpleHashTable class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_SimpleHashTable_INCLUDED
#define Foundation_SimpleHashTable_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Exception.h"
#include "Poco/HashFunction.h"
#include "Poco/HashStatistic.h"
#include <vector>
#include <map>
#include <cstddef>
#include <algorithm>


namespace Poco {


//@ deprecated
template <class Key, class Value, class KeyHashFunction = HashFunction<Key> >
class SimpleHashTable
	/// A SimpleHashTable stores a key value pair that can be looked up via a hashed key.
	///
	/// In comparision to a HashTable, this class handles collisions by sequentially searching the next
	/// free location. This also means that the maximum size of this table is limited, i.e. if the hash table
	/// is full, it will throw an exception and that this class does not support remove operations.
	/// On the plus side it is faster than the HashTable.
	///
	/// This class is NOT thread safe.
{
public:
	class HashEntry
	{
	public:
		Key   key;
		Value value;
		HashEntry(const Key k, const Value v): key(k), value(v)
		{
		}
	};

	typedef std::vector<HashEntry*> HashTableVector;

	SimpleHashTable(UInt32 capacity = 251): _entries(capacity, 0), _size(0), _capacity(capacity)
		/// Creates the SimpleHashTable.
	{
	}

	SimpleHashTable(const SimpleHashTable& ht):
		_size(ht._size),
		_capacity(ht._capacity)
	{
		_entries.reserve(ht._capacity);
		for (typename HashTableVector::iterator it = ht._entries.begin(); it != ht._entries.end(); ++it)
		{
			if (*it) 
				_entries.push_back(new HashEntry(*it));
			else
				_entries.push_back(0);
		}
	}

	~SimpleHashTable()
		/// Destroys the SimpleHashTable.
	{
		clear();
	}

	SimpleHashTable& operator = (const SimpleHashTable& ht)
	{
		if (this != &ht)
		{
			SimpleHashTable tmp(ht);
			swap(tmp);
		}
		return *this;
	}
	
	void swap(SimpleHashTable& ht)
	{
		using std::swap;
		swap(_entries, ht._entries);
		swap(_size, ht._size);
		swap(_capacity, ht._capacity);
	}

	void clear()
	{
		for (typename HashTableVector::iterator it = _entries.begin(); it != _entries.end(); ++it)
		{
			delete *it;
			*it = 0;
		}
		_size = 0;
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
		UInt32 pos = hsh;
		if (!_entries[pos])
			_entries[pos] = new HashEntry(key, value);
		else
		{
			UInt32 origHash = hsh;
			while (_entries[hsh % _capacity])
			{
				if (_entries[hsh % _capacity]->key == key)
					throw ExistsException();
				if (hsh - origHash > _capacity)
					throw PoolOverflowException("SimpleHashTable full");
				hsh++;
			}
			pos = hsh % _capacity;
			_entries[pos] = new HashEntry(key, value);
		}
		_size++;
		return _entries[pos]->value;
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
			_entries[hsh] = new HashEntry(key, value);
		else
		{
			UInt32 origHash = hsh;
			while (_entries[hsh % _capacity])
			{
				if (_entries[hsh % _capacity]->key == key)
				{
					_entries[hsh % _capacity]->value = value;
					return;
				}
				if (hsh - origHash > _capacity)
					throw PoolOverflowException("SimpleHashTable full");
				hsh++;
			}
			_entries[hsh % _capacity] = new HashEntry(key, value);
		}
		_size++;
	}

	UInt32 hash(const Key& key) const
	{
		return _hash(key, _capacity);
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
		UInt32 origHash = hsh;
		while (true)
		{
			if (_entries[hsh % _capacity])
			{
				if (_entries[hsh % _capacity]->key == key)
				{
					return _entries[hsh % _capacity]->value;
				}
			}
			else
				throw InvalidArgumentException("value not found");
			if (hsh - origHash > _capacity)
				throw InvalidArgumentException("value not found");
			hsh++;
		}
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
		UInt32 origHash = hsh;
		while (true)
		{
			if (_entries[hsh % _capacity])
			{
				if (_entries[hsh % _capacity]->key == key)
				{
					return _entries[hsh % _capacity]->value;
				}
			}
			else return insertRaw(key, hsh, Value());
			if (hsh - origHash > _capacity)
				return insertRaw(key, hsh, Value());
			hsh++;
		}
	}

	const Key& getKeyRaw(const Key& key, UInt32 hsh)
		/// Throws an exception if the key does not exist. returns a reference to the internally
		/// stored key. Useful when someone does an insert and wants for performance reason only to store
		/// a pointer to the key in another collection
	{
		UInt32 origHash = hsh;
		while (true)
		{
			if (_entries[hsh % _capacity])
			{
				if (_entries[hsh % _capacity]->key == key)
				{
					return _entries[hsh % _capacity]->key;
				}
			}
			else
				throw InvalidArgumentException("key not found");

			if (hsh - origHash > _capacity)
				throw InvalidArgumentException("key not found");
			hsh++;
		}
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
		UInt32 origHash = hsh;
		while (true)
		{
			if (_entries[hsh % _capacity])
			{
				if (_entries[hsh % _capacity]->key == key)
				{
					v = _entries[hsh % _capacity]->value;
					return true;
				}
			}
			else
				return false;
			if (hsh - origHash > _capacity)
				return false;
			hsh++;
		}
	}

	bool exists(const Key& key) const
	{
		UInt32 hsh = hash(key);
		return existsRaw(key, hsh);
	}

	bool existsRaw(const Key& key, UInt32 hsh) const
	{
		UInt32 origHash = hsh;
		while (true)
		{
			if (_entries[hsh % _capacity])
			{
				if (_entries[hsh % _capacity]->key == key)
				{
					return true;
				}
			}
			else
				return false;
			if (hsh - origHash > _capacity)
				return false;
			hsh++;
		}
	}

	std::size_t size() const
		/// Returns the number of elements already inserted into the SimpleHashTable
	{
		return _size;
	}
	
	UInt32 capacity() const
	{
		return _capacity;
	}

	void resize(UInt32 newSize)
		/// Resizes the hashtable, rehashes all existing entries. Expensive!
	{
		if (_capacity != newSize)
		{
			SimpleHashTable tmp(newSize);
			swap(tmp);
			for (typename HashTableVector::const_iterator it = tmp._entries.begin(); it != tmp._entries.end(); ++it)
			{
				if (*it)
				{
					insertRaw((*it)->key, hash((*it)->key), (*it)->value);
				}
			}
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
		for (int i=0; i < _capacity; ++i)
		{
			if (_entries[i])
			{
				maxEntriesPerHash = 1;
				UInt32 size = 1;
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
		return HashStatistic(_capacity, numberOfEntries, numZeroEntries, maxEntriesPerHash, detailedEntriesPerHash);
	}

private:
	HashTableVector _entries;
	std::size_t     _size;
	UInt32          _capacity;
	KeyHashFunction _hash;
};


} // namespace Poco


#endif // Foundation_HashTable_INCLUDED
