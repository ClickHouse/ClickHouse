//
// UniqueExpireStrategy.h
//
// $Id: //poco/1.4/Foundation/include/Poco/UniqueExpireStrategy.h#1 $
//
// Library: Foundation
// Package: Cache
// Module:  UniqueExpireStrategy
//
// Definition of the UniqueExpireStrategy class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_UniqueExpireStrategy_INCLUDED
#define Foundation_UniqueExpireStrategy_INCLUDED


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
class UniqueExpireStrategy: public AbstractStrategy<TKey, TValue>
	/// An UniqueExpireStrategy implements time based expiration of cache entries. In contrast
	/// to ExpireStrategy which only allows to set a per cache expiration value, it allows to define 
	/// expiration per CacheEntry.
	/// Each TValue object must thus offer the following method:
	///    
	///    const Poco::Timestamp& getExpiration() const;
	///    
	/// which returns the absolute timepoint when the entry will be invalidated.
{
public:
	typedef std::multimap<Timestamp, TKey>     TimeIndex;
	typedef typename TimeIndex::iterator       IndexIterator;
	typedef typename TimeIndex::const_iterator ConstIndexIterator;
	typedef std::map<TKey, IndexIterator>      Keys;
	typedef typename Keys::iterator            Iterator;

public:
	UniqueExpireStrategy()
		/// Create an unique expire strategy.
	{
	}

	~UniqueExpireStrategy()
	{
	}

	void onAdd(const void*, const KeyValueArgs <TKey, TValue>& args)
	{
		// note: we have to insert even if the expire timepoint is in the past (for StrategyCollection classes to avoid inconsistency with LRU)
		// no problem: will be removed with next get
		const Timestamp& expire = args.value().getExpiration();
		IndexIterator it = _keyIndex.insert(std::make_pair(expire, args.key()));
		std::pair<Iterator, bool> stat = _keys.insert(std::make_pair(args.key(), it));
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
			Timestamp now;
			if (it->second->first <= now)
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
		Timestamp now;
		while (it != _keyIndex.end() && it->first < now)
		{
			elemsToRemove.insert(it->second);
			++it;
		}
	}

protected:
	Keys      _keys;     /// For faster replacement of keys, the iterator points to the _keyIndex map
	TimeIndex _keyIndex; /// Maps time to key value
};


} // namespace Poco


#endif // Foundation_UniqueExpireStrategy_INCLUDED
