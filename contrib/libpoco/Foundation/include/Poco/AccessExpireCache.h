//
// AccessExpireCache.h
//
// $Id: //poco/1.4/Foundation/include/Poco/AccessExpireCache.h#1 $
//
// Library: Foundation
// Package: Cache
// Module:  AccessExpireCache
//
// Definition of the AccessExpireCache class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_AccessExpireCache_INCLUDED
#define Foundation_AccessExpireCache_INCLUDED


#include "Poco/AbstractCache.h"
#include "Poco/AccessExpireStrategy.h"


namespace Poco {


template <
	class TKey, 
	class TValue, 
	class TMutex = FastMutex, 
	class TEventMutex = FastMutex
> 
class AccessExpireCache: public AbstractCache<TKey, TValue, AccessExpireStrategy<TKey, TValue>, TMutex, TEventMutex>
	/// An AccessExpireCache caches entries for a fixed time period (per default 10 minutes).
	/// Entries expire when they are not accessed with get() during this time period. Each access resets
	/// the start time for expiration.
	/// Be careful when using an AccessExpireCache. A cache is often used
	/// like cache.has(x) followed by cache.get x). Note that it could happen
	/// that the "has" call works, then the current execution thread gets descheduled, time passes,
	/// the entry gets invalid, thus leading to an empty SharedPtr being returned 
	/// when "get" is invoked.
{
public:
	AccessExpireCache(Timestamp::TimeDiff expire = 600000): 
		AbstractCache<TKey, TValue, AccessExpireStrategy<TKey, TValue>, TMutex, TEventMutex>(AccessExpireStrategy<TKey, TValue>(expire))
	{
	}

	~AccessExpireCache()
	{
	}

private:
	AccessExpireCache(const AccessExpireCache& aCache);
	AccessExpireCache& operator = (const AccessExpireCache& aCache);
};


} // namespace Poco


#endif // Foundation_AccessExpireCache_INCLUDED
