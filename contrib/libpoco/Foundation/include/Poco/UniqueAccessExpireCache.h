//
// UniqueAccessExpireCache.h
//
// $Id: //poco/1.4/Foundation/include/Poco/UniqueAccessExpireCache.h#1 $
//
// Library: Foundation
// Package: Cache
// Module:  UniqueAccessExpireCache
//
// Definition of the UniqueAccessExpireCache class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_UniqueAccessExpireCache_INCLUDED
#define Foundation_UniqueAccessExpireCache_INCLUDED


#include "Poco/AbstractCache.h"
#include "Poco/UniqueAccessExpireStrategy.h"


namespace Poco {


template <
	class TKey, 
	class TValue,
	class TMutex = FastMutex, 
	class TEventMutex = FastMutex
> 
class UniqueAccessExpireCache: public AbstractCache<TKey, TValue, UniqueAccessExpireStrategy<TKey, TValue>, TMutex, TEventMutex>
	/// An UniqueAccessExpireCache caches entries for a given time span. In contrast
	/// to ExpireCache which only allows to set a per cache expiration value, it allows to define 
	/// expiration per CacheEntry.
	/// Each TValue object must thus offer the following method:
	///    
	///    const Poco::Timespan& getTimeout() const;
	///    
	/// which returns the relative timespan for how long the entry should be valid without being accessed!
	/// The absolute expire timepoint is calculated as now() + getTimeout().
	/// Accessing an object will update this absolute expire timepoint.
	/// You can use the Poco::AccessExpirationDecorator to add the getExpiration
	/// method to values that do not have a getExpiration function.
	///
	/// Be careful when using an UniqueAccessExpireCache. A cache is often used
	/// like cache.has(x) followed by cache.get x). Note that it could happen
	/// that the "has" call works, then the current execution thread gets descheduled, time passes,
	/// the entry gets invalid, thus leading to an empty SharedPtr being returned 
	/// when "get" is invoked.
{
public:
	UniqueAccessExpireCache():
		AbstractCache<TKey, TValue, UniqueAccessExpireStrategy<TKey, TValue>, TMutex, TEventMutex>(UniqueAccessExpireStrategy<TKey, TValue>())
	{
	}

	~UniqueAccessExpireCache()
	{
	}

private:
	UniqueAccessExpireCache(const UniqueAccessExpireCache& aCache);
	UniqueAccessExpireCache& operator = (const UniqueAccessExpireCache& aCache);
};


} // namespace Poco


#endif // Foundation_UniqueAccessExpireCache_INCLUDED
