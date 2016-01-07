//
// UniqueExpireLRUCache.h
//
// $Id: //poco/1.4/Foundation/include/Poco/UniqueExpireLRUCache.h#1 $
//
// Library: Foundation
// Package: Cache
// Module:  UniqueExpireLRUCache
//
// Definition of the UniqueExpireLRUCache class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_UniqueExpireLRUCache_INCLUDED
#define Foundation_UniqueExpireLRUCache_INCLUDED


#include "Poco/AbstractCache.h"
#include "Poco/StrategyCollection.h"
#include "Poco/UniqueExpireStrategy.h"
#include "Poco/LRUStrategy.h"


namespace Poco {


template < 
	class TKey,
	class TValue,
	class TMutex = FastMutex, 
	class TEventMutex = FastMutex
>
class UniqueExpireLRUCache: public AbstractCache<TKey, TValue, StrategyCollection<TKey, TValue>, TMutex, TEventMutex>
	/// A UniqueExpireLRUCache combines LRU caching and time based per entry expire caching.
	/// One can define for each cache entry a seperate timepoint
	/// but also limit the size of the cache (per default: 1024).
	/// Each TValue object must thus offer the following method:
	///    
	///    const Poco::Timestamp& getExpiration() const;
	///    
	/// which returns the absolute timepoint when the entry will be invalidated.
	/// Accessing an object will NOT update this absolute expire timepoint.
	/// You can use the Poco::ExpirationDecorator to add the getExpiration
	/// method to values that do not have a getExpiration function.
{
public:
	UniqueExpireLRUCache(long cacheSize = 1024): 
		AbstractCache<TKey, TValue, StrategyCollection<TKey, TValue>, TMutex, TEventMutex>(StrategyCollection<TKey, TValue>())
	{
		this->_strategy.pushBack(new LRUStrategy<TKey, TValue>(cacheSize));
		this->_strategy.pushBack(new UniqueExpireStrategy<TKey, TValue>());
	}

	~UniqueExpireLRUCache()
	{
	}

private:
	UniqueExpireLRUCache(const UniqueExpireLRUCache& aCache);
	UniqueExpireLRUCache& operator = (const UniqueExpireLRUCache& aCache);
};


} // namespace Poco


#endif // Foundation_UniqueExpireLRUCache_INCLUDED
