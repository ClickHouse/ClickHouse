//
// UniqueAccessExpireLRUCache.h
//
// Library: Foundation
// Package: Cache
// Module:  UniqueAccessExpireLRUCache
//
// Definition of the UniqueAccessExpireLRUCache class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_UniqueAccessExpireLRUCache_INCLUDED
#define Foundation_UniqueAccessExpireLRUCache_INCLUDED


#include "Poco/AbstractCache.h"
#include "Poco/StrategyCollection.h"
#include "Poco/UniqueAccessExpireStrategy.h"
#include "Poco/LRUStrategy.h"


namespace Poco {


template < 
	class TKey,
	class TValue,
	class TMutex = FastMutex, 
	class TEventMutex = FastMutex
>
class UniqueAccessExpireLRUCache: public AbstractCache<TKey, TValue, StrategyCollection<TKey, TValue>, TMutex, TEventMutex>
	/// A UniqueAccessExpireLRUCache combines LRU caching and time based per entry expire caching.
	/// One can define for each cache entry a seperate timepoint
	/// but also limit the size of the cache (per default: 1024).
	/// Each TValue object must thus offer the following method:
	///    
	///    const Poco::Timespan& getTimeout() const;
	///    
	/// which returns the relative timespan for how long the entry should be valid without being accessed!
	/// The absolute expire timepoint is calculated as now() + getTimeout().
	/// Accessing an object will update this absolute expire timepoint.
	/// You can use the Poco::AccessExpirationDecorator to add the getExpiration
	/// method to values that do not have a getExpiration function.
{
public:
	UniqueAccessExpireLRUCache(long cacheSize = 1024): 
		AbstractCache<TKey, TValue, StrategyCollection<TKey, TValue>, TMutex, TEventMutex>(StrategyCollection<TKey, TValue>())
	{
		this->_strategy.pushBack(new LRUStrategy<TKey, TValue>(cacheSize));
		this->_strategy.pushBack(new UniqueAccessExpireStrategy<TKey, TValue>());
	}

	~UniqueAccessExpireLRUCache()
	{
	}

private:
	UniqueAccessExpireLRUCache(const UniqueAccessExpireLRUCache& aCache);
	UniqueAccessExpireLRUCache& operator = (const UniqueAccessExpireLRUCache& aCache);
};


} // namespace Poco


#endif // Foundation_UniqueAccessExpireLRUCache_INCLUDED
