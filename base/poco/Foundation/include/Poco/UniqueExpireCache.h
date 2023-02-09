//
// UniqueExpireCache.h
//
// Library: Foundation
// Package: Cache
// Module:  UniqueExpireCache
//
// Definition of the UniqueExpireCache class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_UniqueExpireCache_INCLUDED
#define Foundation_UniqueExpireCache_INCLUDED


#include "Poco/AbstractCache.h"
#include "Poco/UniqueExpireStrategy.h"


namespace Poco {


template <
	class TKey, 
	class TValue,
	class TMutex = FastMutex, 
	class TEventMutex = FastMutex
> 
class UniqueExpireCache: public AbstractCache<TKey, TValue, UniqueExpireStrategy<TKey, TValue>, TMutex, TEventMutex>
	/// An UniqueExpireCache caches entries for a given time amount. In contrast
	/// to ExpireCache which only allows to set a per cache expiration value, it allows to define 
	/// expiration per CacheEntry.
	/// Each TValue object must thus offer the following method:
	///    
	///    const Poco::Timestamp& getExpiration() const;
	///    
	/// which returns the absolute timepoint when the entry will be invalidated.
	/// Accessing an object will NOT update this absolute expire timepoint.
	/// You can use the Poco::ExpirationDecorator to add the getExpiration
	/// method to values that do not have a getExpiration function.
	///
	/// Be careful when using an UniqueExpireCache. A cache is often used
	/// like cache.has(x) followed by cache.get x). Note that it could happen
	/// that the "has" call works, then the current execution thread gets descheduled, time passes,
	/// the entry gets invalid, thus leading to an empty SharedPtr being returned 
	/// when "get" is invoked.
{
public:
	UniqueExpireCache():
		AbstractCache<TKey, TValue, UniqueExpireStrategy<TKey, TValue>, TMutex, TEventMutex>(UniqueExpireStrategy<TKey, TValue>())
	{
	}

	~UniqueExpireCache()
	{
	}

private:
	UniqueExpireCache(const UniqueExpireCache& aCache);
	UniqueExpireCache& operator = (const UniqueExpireCache& aCache);
};


} // namespace Poco


#endif // Foundation_UniqueExpireCache_INCLUDED
