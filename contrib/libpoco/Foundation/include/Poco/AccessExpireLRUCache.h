//
// AccessExpireLRUCache.h
//
// $Id: //poco/1.4/Foundation/include/Poco/AccessExpireLRUCache.h#1 $
//
// Library: Foundation
// Package: Cache
// Module:  AccessExpireLRUCache
//
// Definition of the AccessExpireLRUCache class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_AccessExpireLRUCache_INCLUDED
#define Foundation_AccessExpireLRUCache_INCLUDED


#include "Poco/AbstractCache.h"
#include "Poco/StrategyCollection.h"
#include "Poco/AccessExpireStrategy.h"
#include "Poco/LRUStrategy.h"


namespace Poco {


template < 
	class TKey,
	class TValue,
	class TMutex = FastMutex, 
	class TEventMutex = FastMutex
>
class AccessExpireLRUCache: public AbstractCache<TKey, TValue, StrategyCollection<TKey, TValue>, TMutex, TEventMutex>
	/// An AccessExpireLRUCache combines LRU caching and time based expire caching.
	/// It cache entries for a fixed time period (per default 10 minutes)
	/// but also limits the size of the cache (per default: 1024).
{
public:
	AccessExpireLRUCache(long cacheSize = 1024, Timestamp::TimeDiff expire = 600000): 
		AbstractCache<TKey, TValue, StrategyCollection<TKey, TValue>, TMutex, TEventMutex >(StrategyCollection<TKey, TValue>())
	{
		this->_strategy.pushBack(new LRUStrategy<TKey, TValue>(cacheSize));
		this->_strategy.pushBack(new AccessExpireStrategy<TKey, TValue>(expire));
	}

	~AccessExpireLRUCache()
	{
	}

private:
	AccessExpireLRUCache(const AccessExpireLRUCache& aCache);
	AccessExpireLRUCache& operator = (const AccessExpireLRUCache& aCache);
};


} // namespace Poco


#endif // Foundation_AccessExpireLRUCache_INCLUDED
