//
// ExpireStrategy.h
//
// Library: Foundation
// Package: Cache
// Module:  ExpireStrategy
//
// Definition of the ExpireStrategy class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_ExpireStrategy_INCLUDED
#define Foundation_ExpireStrategy_INCLUDED


#include "Poco/KeyValueArgs.h"
#include "Poco/ValidArgs.h"
#include "Poco/AbstractStrategy.h"
#include "Poco/Bugcheck.h"
#include "Poco/Timestamp.h"
#include "Poco/EventArgs.h"
#include <set>
#include <map>


namespace Poco {


template < 
	class TKey,
	class TValue
>
class ExpireStrategy: public AbstractStrategy<TKey, TValue>
	/// An ExpireStrategy implements time based expiration of cache entries
{
public:
	typedef std::multimap<Timestamp, TKey>     TimeIndex;
	typedef typename TimeIndex::iterator       IndexIterator;
	typedef typename TimeIndex::const_iterator ConstIndexIterator;
	typedef std::map<TKey, IndexIterator>      Keys;
	typedef typename Keys::iterator            Iterator;

public:
	ExpireStrategy(Timestamp::TimeDiff expireTimeInMilliSec): _expireTime(expireTimeInMilliSec * 1000)
		/// Create an expire strategy. Note that the smallest allowed caching time is 25ms.
		/// Anything lower than that is not useful with current operating systems.
	{
		if (_expireTime < 25000) throw InvalidArgumentException("expireTime must be at least 25 ms");  
	}

	~ExpireStrategy()
	{
	}

	void onAdd(const void*, const KeyValueArgs <TKey, TValue>& args)
	{
		Timestamp now;
		typename TimeIndex::value_type tiValue(now, args.key());
		IndexIterator it = _keyIndex.insert(tiValue);
		typename Keys::value_type kValue(args.key(), it);
		std::pair<Iterator, bool> stat = _keys.insert(kValue);
		if (!stat.second)
		{
			_keyIndex.erase(stat.first->second);
			stat.first->second = it;
		}
	}

	void onRemove(const void*, const TKey& key)
	{
		Iterator it = _keys.find(key);
		if (it != _keys.end())
		{
			_keyIndex.erase(it->second);
			_keys.erase(it);
		}
	}

	void onGet(const void*, const TKey& key)
	{
		// get triggers no changes in an expire
	}

	void onClear(const void*, const EventArgs& args)
	{
		_keys.clear();
		_keyIndex.clear();
	}

	void onIsValid(const void*, ValidArgs<TKey>& args)
	{
		Iterator it = _keys.find(args.key());
		if (it != _keys.end())
		{
			if (it->second->first.isElapsed(_expireTime))
			{
				args.invalidate();
			}
		}
		else //not found: probably removed by onReplace
			args.invalidate();
	}

	void onReplace(const void*, std::set<TKey>& elemsToRemove)
	{
		// Note: replace only informs the cache which elements
		// it would like to remove!
		// it does not remove them on its own!
		IndexIterator it = _keyIndex.begin();
		while (it != _keyIndex.end() && it->first.isElapsed(_expireTime))
		{
			elemsToRemove.insert(it->second);
			++it;
		}
	}

protected:
	Timestamp::TimeDiff _expireTime;
	Keys      _keys;     /// For faster replacement of keys, the iterator points to the _keyIndex map
	TimeIndex _keyIndex; /// Maps time to key value
};


} // namespace Poco


#endif // Foundation_ExpireStrategy_INCLUDED
