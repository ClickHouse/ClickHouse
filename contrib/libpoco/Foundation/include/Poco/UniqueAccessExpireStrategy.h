//
// UniqueAccessExpireStrategy.h
//
// $Id: //poco/1.4/Foundation/include/Poco/UniqueAccessExpireStrategy.h#1 $
//
// Library: Foundation
// Package: Cache
// Module:  UniqueAccessExpireStrategy
//
// Definition of the UniqueAccessExpireStrategy class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_UniqueAccessExpireStrategy_INCLUDED
#define Foundation_UniqueAccessExpireStrategy_INCLUDED


#include "Poco/KeyValueArgs.h"
#include "Poco/ValidArgs.h"
#include "Poco/AbstractStrategy.h"
#include "Poco/Bugcheck.h"
#include "Poco/Timestamp.h"
#include "Poco/Timespan.h"
#include "Poco/EventArgs.h"
#include "Poco/UniqueExpireStrategy.h"
#include <set>
#include <map>


namespace Poco {


template < 
	class TKey,
	class TValue
>
class UniqueAccessExpireStrategy: public AbstractStrategy<TKey, TValue>
	/// An UniqueExpireStrategy implements time based expiration of cache entries. In contrast
	/// to ExpireStrategy which only allows to set a per cache expiration value, it allows to define 
	/// expiration per CacheEntry.
	/// Each TValue object must thus offer the following method:
	///    
	///    const Poco::Timestamp& getTimeout() const;
	///    
	/// which returns the timespan for how long an object will be valid without being accessed.
{
public:
	typedef std::pair<TKey, Timespan>           KeyExpire;
	typedef std::multimap<Timestamp, KeyExpire> TimeIndex;
	typedef typename TimeIndex::iterator        IndexIterator;
	typedef typename TimeIndex::const_iterator  ConstIndexIterator;
	typedef std::map<TKey, IndexIterator>       Keys;
	typedef typename Keys::iterator             Iterator;

public:
	UniqueAccessExpireStrategy()
		/// Create an unique expire strategy.
	{
	}

	~UniqueAccessExpireStrategy()
	{
	}

	void onAdd(const void*, const KeyValueArgs <TKey, TValue>& args)
	{
		// the expire value defines how many millisecs in the future the
		// value will expire, even insert negative values!
		Timestamp expire;
		expire += args.value().getTimeout().totalMicroseconds();
		
		IndexIterator it = _keyIndex.insert(std::make_pair(expire, std::make_pair(args.key(), args.value().getTimeout())));
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
		// get updates the expiration time stamp
		Iterator it = _keys.find(key);
		if (it != _keys.end())
		{
			KeyExpire ke = it->second->second;
			// gen new absolute expire value
			Timestamp expire;
			expire += ke.second.totalMicroseconds();
			// delete old index
			_keyIndex.erase(it->second);
			IndexIterator itt = _keyIndex.insert(std::make_pair(expire, ke));
			// update iterator
			it->second = itt;
		}
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
			elemsToRemove.insert(it->second.first);
			++it;
		}
	}

protected:
	Keys      _keys;     /// For faster replacement of keys, the iterator points to the _keyIndex map
	TimeIndex _keyIndex; /// Maps time to key value
};


} // namespace Poco


#endif // Foundation_UniqueAccessExpireStrategy_INCLUDED
