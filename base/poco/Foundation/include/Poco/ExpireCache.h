//
// ExpireCache.h
//
// Library: Foundation
// Package: Cache
// Module:  ExpireCache
//
// Definition of the ExpireCache class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_ExpireCache_INCLUDED
#define Foundation_ExpireCache_INCLUDED


#include "Poco/AbstractCache.h"
#include "Poco/ExpireStrategy.h"


namespace Poco {


template <
	class TKey, 
	class TValue, 
	class TMutex = FastMutex, 
	class TEventMutex = FastMutex
> 
class ExpireCache: public AbstractCache<TKey, TValue, ExpireStrategy<TKey, TValue>, TMutex, TEventMutex>
	/// An ExpireCache caches entries for a fixed time period (per default 10 minutes).
	/// Entries expire independently of the access pattern, i.e. after a constant time.
	/// If you require your objects to expire after they were not accessed for a given time
	/// period use a Poco::AccessExpireCache.
	///
	/// Be careful when using an ExpireCache. A cache is often used
	/// like cache.has(x) followed by cache.get x). Note that it could happen
	/// that the "has" call works, then the current execution thread gets descheduled, time passes,
	/// the entry gets invalid, thus leading to an empty SharedPtr being returned 
	/// when "get" is invoked.
{
public:
	ExpireCache(Timestamp::TimeDiff expire = 600000): 
		AbstractCache<TKey, TValue, ExpireStrategy<TKey, TValue>, TMutex, TEventMutex>(ExpireStrategy<TKey, TValue>(expire))
	{
	}

	~ExpireCache()
	{
	}

private:
	ExpireCache(const ExpireCache& aCache);
	ExpireCache& operator = (const ExpireCache& aCache);
};


} // namespace Poco


#endif // Foundation_ExpireCache_INCLUDED
