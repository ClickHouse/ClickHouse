//
// LRUStrategy.h
//
// Library: Foundation
// Package: Cache
// Module:  LRUStrategy
//
// Definition of the LRUStrategy class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_LRUStrategy_INCLUDED
#define Foundation_LRUStrategy_INCLUDED


#include "Poco/KeyValueArgs.h"
#include "Poco/ValidArgs.h"
#include "Poco/AbstractStrategy.h"
#include "Poco/EventArgs.h"
#include "Poco/Exception.h"
#include <list>
#include <map>
#include <cstddef>


namespace Poco {


template <class TKey, class TValue>
class LRUStrategy: public AbstractStrategy<TKey, TValue>
	/// An LRUStrategy implements least recently used cache replacement.
{
public:
	typedef std::list<TKey>                   Keys;
	typedef typename Keys::iterator           Iterator;
	typedef typename Keys::const_iterator     ConstIterator;
	typedef std::map<TKey, Iterator>          KeyIndex;
	typedef typename KeyIndex::iterator       IndexIterator;
	typedef typename KeyIndex::const_iterator ConstIndexIterator;

public:
	LRUStrategy(std::size_t size): 
		_size(size)
	{
		if (_size < 1) throw InvalidArgumentException("size must be > 0");
	}

	~LRUStrategy()
	{
	}

	void onAdd(const void*, const KeyValueArgs <TKey, TValue>& args)
	{
		_keys.push_front(args.key());
		std::pair<IndexIterator, bool> stat = _keyIndex.insert(std::make_pair(args.key(), _keys.begin()));
		if (!stat.second)
		{
			stat.first->second = _keys.begin();
		}
	}

	void onRemove(const void*, const TKey& key)
	{
		IndexIterator it = _keyIndex.find(key);

		if (it != _keyIndex.end())
		{
			_keys.erase(it->second);
			_keyIndex.erase(it);
		}
	}

	void onGet(const void*, const TKey& key)
	{
		// LRU: in case of an hit, move to begin
		IndexIterator it = _keyIndex.find(key);

		if (it != _keyIndex.end())
		{
			_keys.splice(_keys.begin(), _keys, it->second); //_keys.erase(it->second)+_keys.push_front(key);
			it->second = _keys.begin();
		}
	}

	void onClear(const void*, const EventArgs& args)
	{
		_keys.clear();
		_keyIndex.clear();
	}

	void onIsValid(const void*, ValidArgs<TKey>& args)
	{
		if (_keyIndex.find(args.key()) == _keyIndex.end())
		{
			args.invalidate();
		}
	}

	void onReplace(const void*, std::set<TKey>& elemsToRemove)
	{
		// Note: replace only informs the cache which elements
		// it would like to remove!
		// it does not remove them on its own!
		std::size_t curSize = _keyIndex.size();

		if (curSize < _size)
		{
			return;
		}

		std::size_t diff = curSize - _size;
		Iterator it = --_keys.end(); //--keys can never be invoked on an empty list due to the minSize==1 requirement of LRU
		std::size_t i = 0;

		while (i++ < diff) 
		{
			elemsToRemove.insert(*it);
			if (it != _keys.begin())
			{
				--it;
			}
		}
	}

protected:
	std::size_t _size;     /// Number of keys the cache can store.
	Keys        _keys;
	KeyIndex    _keyIndex; /// For faster access to _keys
};


} // namespace Poco


#endif // Foundation_LRUStrategy_INCLUDED
